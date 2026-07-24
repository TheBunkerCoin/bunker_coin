use std::time::Duration;

use async_trait::async_trait;
use log::{debug, info, trace, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};
use tokio_serial::{DataBits, FlowControl, Parity, SerialPortBuilderExt, StopBits};

use crate::hostmode::{
    encode_frame, HostmodeDecoder, HostmodeFrame, HostmodePacket, PACTOR_CHANNEL,
};
use crate::{PactorLinkEvent, PactorLinkStatus, PactorTransport, ScsPactorError};

const STATUS_CHANNEL: u8 = 254;
const EXTENDED_POLL_CHANNEL: u8 = 255;
const MAX_HOSTMODE_RETRIES: u8 = 3;

/// Prefix for printable terminal-mode data lines (`#<hex>\r`).
const DATA_LINE_MARKER: &str = "#";

fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

fn decode_hex_line(hex: &str) -> Option<Vec<u8>> {
    let hex = hex.trim();
    if hex.is_empty() || !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16)?;
        let lo = (bytes[i + 1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }
    Some(out)
}

#[derive(Clone, Debug)]
pub struct UsbPactorConfig {
    pub port: String,
    pub baud_rate: u32,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub command_timeout: Duration,
}

impl UsbPactorConfig {
    pub fn new(port: impl Into<String>) -> Self {
        Self {
            port: port.into(),
            baud_rate: 829_440,
            read_timeout: Some(Duration::from_secs(10)),
            write_timeout: Some(Duration::from_secs(10)),
            command_timeout: Duration::from_secs(90),
        }
    }
}

pub struct UsbPactorTransport {
    writer: Mutex<Box<dyn AsyncWrite + Send + Unpin>>,
    command_rx: Mutex<mpsc::Receiver<String>>,
    data_rx: Mutex<mpsc::Receiver<Vec<u8>>>,
    event_rx: Mutex<mpsc::Receiver<PactorLinkEvent>>,
    packet_rx: Mutex<mpsc::Receiver<HostmodePacket>>,
    transaction_lock: Mutex<()>,
    /// Hostmode sequence counter; bit 7 toggles only after ACK.
    packet_counter: Mutex<PacketCounter>,
    /// Latches link drops so `read_data` fails fast even if the drop precedes the wait.
    link_down: tokio::sync::watch::Receiver<bool>,
    /// Clears stale `link_down` state on a fresh connect.
    link_down_tx: watch::Sender<bool>,
    read_task: JoinHandle<()>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    command_timeout: Duration,
}

#[derive(Debug)]
struct PacketCounter {
    /// Next hostmode counter bit; starts false and toggles after ACK.
    toggle: bool,
}

impl PacketCounter {
    fn new() -> Self {
        Self { toggle: false }
    }

    /// Applies the cmd/data bit plus current hostmode counter bit.
    fn apply(&self, code: u8) -> u8 {
        let base = code & 0x01; // keep only cmd/data bit
        if self.toggle {
            base | 0x80
        } else {
            base
        }
    }

    /// Advances after ACK, never after repeat request.
    fn advance(&mut self) {
        self.toggle = !self.toggle;
    }

    /// Resets to parity 0 before connect so prior hostmode polls do not desync the modem.
    fn reset(&mut self) {
        self.toggle = false;
    }
}

impl UsbPactorTransport {
    pub async fn connect(config: UsbPactorConfig) -> Result<Self, ScsPactorError> {
        let serial = tokio_serial::new(&config.port, config.baud_rate)
            .data_bits(DataBits::Eight)
            .parity(Parity::None)
            .stop_bits(StopBits::One)
            .flow_control(FlowControl::None)
            .open_native_async()
            .map_err(|e| ScsPactorError::Io(std::io::Error::other(e.to_string())))?;

        Ok(Self::from_stream(serial, config))
    }

    pub fn from_stream<S>(stream: S, config: UsbPactorConfig) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (reader, writer) = tokio::io::split(stream);
        Self::from_split(reader, writer, config)
    }

    pub fn from_split<R, W>(mut reader: R, writer: W, config: UsbPactorConfig) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let (command_tx, command_rx) = mpsc::channel(1024);
        let (data_tx, data_rx) = mpsc::channel(1024);
        let (event_tx, event_rx) = mpsc::channel(1024);
        let (packet_tx, packet_rx) = mpsc::channel(1024);

        let label = config
            .port
            .rsplit('/')
            .next()
            .unwrap_or(&config.port)
            .to_string();

        let (link_down_tx, link_down) = watch::channel(false);
        let struct_link_down_tx = link_down_tx.clone();

        let read_task = tokio::spawn(async move {
            let mut decoder = HostmodeDecoder::new();
            let mut term_line: Vec<u8> = Vec::new();
            let mut buf = [0u8; 1024];
            debug!("[reader:{label}] task started");

            loop {
                let n = match reader.read(&mut buf).await {
                    Ok(0) => {
                        debug!("[reader:{label}] EOF on serial stream");
                        let _ = event_tx
                            .send(PactorLinkEvent::Status(PactorLinkStatus::Disconnected))
                            .await;
                        let _ = link_down_tx.send(true);
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        warn!("[reader:{label}] serial read error: {e}");
                        let _ = event_tx
                            .send(PactorLinkEvent::Status(PactorLinkStatus::LinkFailure))
                            .await;
                        let _ = link_down_tx.send(true);
                        break;
                    }
                };

                trace!("[reader:{label}] got {} bytes: {:02x?}", n, &buf[..n]);

                // Connected firmware reports status as terminal ASCII amid hostmode bytes.
                for &b in &buf[..n] {
                    if b == b'\r' || b == b'\n' {
                        if !term_line.is_empty() {
                            let line = String::from_utf8_lossy(&term_line).trim().to_string();
                            if !line.is_empty() {
                                // `#<hex>` lines are data; other terminal lines are status.
                                if let Some(hex) = line.strip_prefix(DATA_LINE_MARKER) {
                                    if let Some(bytes) = decode_hex_line(hex) {
                                        debug!(
                                            "[reader:{label}] data line -> {} bytes routed",
                                            bytes.len()
                                        );
                                        let _ = data_tx.send(bytes).await;
                                    } else {
                                        warn!(
                                            "[reader:{label}] bad data line (hex decode failed): {line:?}"
                                        );
                                    }
                                } else {
                                    let link_down =
                                        route_terminal_line(&line, &command_tx, &event_tx).await;
                                    if link_down {
                                        // Wake blocked reads on link drop.
                                        let _ = link_down_tx.send(true);
                                    }
                                }
                            }
                            term_line.clear();
                        }
                    } else if (b.is_ascii_graphic() || b == b' ') && term_line.len() < 4096 {
                        term_line.push(b);
                    } else {
                        // Hostmode bytes invalidate partial terminal text.
                        term_line.clear();
                    }
                }

                decoder.push(&buf[..n]);
                loop {
                    match decoder.next_packet() {
                        Ok(Some(HostmodePacket::Frame(frame))) => {
                            let ascii = String::from_utf8_lossy(&frame.payload);
                            trace!(
                                "[reader:{label}] decoded frame ch={} code=0x{:02x} payload({})={:02x?} ascii={:?}",
                                frame.channel,
                                frame.code,
                                frame.payload.len(),
                                &frame.payload,
                                ascii
                            );
                            let _ = packet_tx.send(HostmodePacket::Frame(frame.clone())).await;
                            if route_frame(frame, &command_tx, &data_tx, &event_tx)
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Ok(Some(HostmodePacket::RepeatRequest)) => {
                            trace!("[reader:{label}] decoded RepeatRequest");
                            let _ = packet_tx.send(HostmodePacket::RepeatRequest).await;
                        }
                        Ok(None) => break,
                        Err(e) => {
                            debug!("[reader:{label}] decode error: {e}");
                            let _ = event_tx
                                .send(PactorLinkEvent::Status(PactorLinkStatus::LinkFailure))
                                .await;
                            break;
                        }
                    }
                }
            }
        });

        Self {
            writer: Mutex::new(Box::new(writer)),
            command_rx: Mutex::new(command_rx),
            data_rx: Mutex::new(data_rx),
            event_rx: Mutex::new(event_rx),
            packet_rx: Mutex::new(packet_rx),
            transaction_lock: Mutex::new(()),
            packet_counter: Mutex::new(PacketCounter::new()),
            link_down,
            link_down_tx: struct_link_down_tx,
            read_task,
            read_timeout: config.read_timeout,
            write_timeout: config.write_timeout,
            command_timeout: config.command_timeout,
        }
    }

    async fn send_hostmode_frame(&self, frame: HostmodeFrame) -> Result<(), ScsPactorError> {
        let encoded = self.encode_outbound_frame(frame).await?;
        self.write_encoded_frame(&encoded).await
    }

    /// Resets hostmode packet-counter toggle to parity 0 before connect.
    pub async fn reset_packet_counter(&self) {
        self.packet_counter.lock().await.reset();
    }

    pub async fn send_hostmode_frame_no_response(
        &self,
        frame: HostmodeFrame,
    ) -> Result<(), ScsPactorError> {
        self.send_hostmode_frame(frame).await
    }

    /// Send a command and advance the hostmode counter only after ACK.
    pub async fn send_command_best_effort_ack(
        &self,
        frame: HostmodeFrame,
        ack_timeout: Duration,
    ) -> Result<Option<HostmodeFrame>, ScsPactorError> {
        let _lock = self.transaction_lock.lock().await;
        let encoded = self.encode_outbound_frame(frame.clone()).await?;
        trace!(
            "[tx] best_effort_ack: ch={} code=0x{:02x} payload={:02x?} encoded={:02x?}",
            frame.channel,
            frame.code,
            &frame.payload,
            &encoded
        );
        self.write_encoded_frame(&encoded).await?;
        match self.recv_hostmode_packet(ack_timeout).await {
            Ok(HostmodePacket::Frame(resp)) => {
                trace!(
                    "[tx] best_effort_ack response: ch={} code=0x{:02x} payload={:02x?}",
                    resp.channel,
                    resp.code,
                    &resp.payload
                );
                self.packet_counter.lock().await.advance();
                Ok(Some(resp))
            }
            _ => {
                trace!(
                    "[tx] best_effort_ack: no ack within {ack_timeout:?}, counter held (frame not consumed)"
                );
                Ok(None)
            }
        }
    }

    /// Writes terminal-mode bytes without hostmode framing.
    pub async fn write_raw(&self, bytes: &[u8]) -> Result<(), ScsPactorError> {
        self.write_encoded_frame(bytes).await
    }

    async fn encode_outbound_frame(&self, frame: HostmodeFrame) -> Result<Vec<u8>, ScsPactorError> {
        let counter = self.packet_counter.lock().await;
        let code_with_counter = counter.apply(frame.code);
        let framed = HostmodeFrame::with_code(frame.channel, code_with_counter, frame.payload);
        encode_frame(&framed)
    }

    async fn write_encoded_frame(&self, encoded: &[u8]) -> Result<(), ScsPactorError> {
        let mut writer = self.writer.lock().await;
        let write = writer.write_all(encoded);
        if let Some(d) = self.write_timeout {
            timeout(d, write)
                .await
                .map_err(|_| ScsPactorError::Timeout)??;
        } else {
            write.await?;
        }
        writer.flush().await?;
        Ok(())
    }

    pub async fn hostmode_transaction(
        &self,
        frame: HostmodeFrame,
    ) -> Result<HostmodeFrame, ScsPactorError> {
        let _transaction = self.transaction_lock.lock().await;
        let encoded = self.encode_outbound_frame(frame.clone()).await?;
        trace!(
            "[tx] hostmode_transaction: ch={} code=0x{:02x} payload={:02x?} encoded={:02x?}",
            frame.channel,
            frame.code,
            &frame.payload,
            &encoded
        );
        let mut retries = 0;

        loop {
            self.write_encoded_frame(&encoded).await?;
            match self.recv_hostmode_packet(self.command_timeout).await? {
                HostmodePacket::Frame(response) => {
                    self.packet_counter.lock().await.advance();
                    trace!(
                        "[tx] hostmode_transaction response: ch={} code=0x{:02x} payload={:02x?}",
                        response.channel,
                        response.code,
                        &response.payload
                    );
                    return Ok(response);
                }
                HostmodePacket::RepeatRequest => {
                    retries += 1;
                    debug!("[tx] repeat request (retry {retries}/{MAX_HOSTMODE_RETRIES})");
                    if retries > MAX_HOSTMODE_RETRIES {
                        return Err(ScsPactorError::Protocol(
                            "hostmode repeat request limit exceeded".to_owned(),
                        ));
                    }
                }
            }
        }
    }

    async fn recv_hostmode_packet(
        &self,
        timeout_after: Duration,
    ) -> Result<HostmodePacket, ScsPactorError> {
        let mut rx = self.packet_rx.lock().await;
        timeout(timeout_after, rx.recv())
            .await
            .map_err(|_| ScsPactorError::Timeout)?
            .ok_or(ScsPactorError::Disconnected)
    }

    /// Polls a channel with hostmode payload `G`.
    pub async fn poll_channel(&self, channel: u8) -> Result<HostmodeFrame, ScsPactorError> {
        self.hostmode_transaction(HostmodeFrame::command(channel, b"G".to_vec()))
            .await
    }

    pub async fn poll_pending_channels(&self) -> Result<Vec<u8>, ScsPactorError> {
        let response = self.poll_channel(EXTENDED_POLL_CHANNEL).await?;
        if response.channel != EXTENDED_POLL_CHANNEL {
            return Err(ScsPactorError::Protocol(format!(
                "expected channel {EXTENDED_POLL_CHANNEL} poll response, got {}",
                response.channel
            )));
        }

        Ok(response
            .payload
            .iter()
            .copied()
            .take_while(|byte| *byte != 0)
            .filter_map(|byte| byte.checked_sub(1))
            .collect())
    }

    pub async fn poll_status(&self) -> Result<Vec<u8>, ScsPactorError> {
        let response = self.poll_channel(STATUS_CHANNEL).await?;
        if response.channel != STATUS_CHANNEL {
            return Err(ScsPactorError::Protocol(format!(
                "expected channel {STATUS_CHANNEL} status response, got {}",
                response.channel
            )));
        }
        Ok(response.payload)
    }

    /// Sends a PACTOR hostmode command; the command letter lives in the payload.
    async fn send_host_command(&self, cmd_letter: u8, args: &[u8]) -> Result<(), ScsPactorError> {
        let mut payload = vec![cmd_letter];
        if !args.is_empty() {
            payload.push(b' ');
            payload.extend_from_slice(args);
        }
        self.send_hostmode_frame(HostmodeFrame::command(PACTOR_CHANNEL, payload))
            .await
    }

    pub async fn send_command(&self, line: &str) -> Result<(), ScsPactorError> {
        let trimmed = line.trim();
        let mut parts = trimmed.splitn(2, char::is_whitespace);
        let command = parts.next().unwrap_or_default();
        let args = parts.next().unwrap_or_default().trim_start();
        let cmd_letter = match command {
            "MYCALL" => b'I',
            "CONNECT" => b'C',
            "DISCONNECT" => b'D',
            command if command.len() == 1 => command.as_bytes()[0],
            _ => {
                return Err(ScsPactorError::Protocol(format!(
                    "unsupported hostmode command {command}"
                )))
            }
        };
        self.send_host_command(cmd_letter, args.as_bytes()).await
    }

    pub async fn read_status_line(&self) -> Result<String, ScsPactorError> {
        let mut rx = self.command_rx.lock().await;
        let read = rx.recv();
        if let Some(d) = self.read_timeout {
            timeout(d, read)
                .await
                .map_err(|_| ScsPactorError::Timeout)?
                .ok_or(ScsPactorError::Disconnected)
        } else {
            read.await.ok_or(ScsPactorError::Disconnected)
        }
    }

    fn parse_status_line(line: &str) -> Result<PactorLinkEvent, ScsPactorError> {
        if let Some(rest) = line.strip_prefix("CONNECTED ") {
            return Ok(PactorLinkEvent::Status(PactorLinkStatus::Connected {
                remote_call: rest.trim().to_owned(),
            }));
        }
        if let Some(rest) = line.strip_prefix("CONNECTING ") {
            return Ok(PactorLinkEvent::Status(PactorLinkStatus::Connecting {
                remote_call: rest.trim().to_owned(),
            }));
        }
        if line.starts_with("DISCONNECTED") {
            return Ok(PactorLinkEvent::Status(PactorLinkStatus::Disconnected));
        }
        if line.starts_with("BUSY") {
            return Ok(PactorLinkEvent::Status(PactorLinkStatus::Busy));
        }
        if line.starts_with("QUEUED") {
            return Ok(PactorLinkEvent::Status(PactorLinkStatus::Queued));
        }
        if line.starts_with("LINK FAILURE") || line.starts_with("FAIL") || line.starts_with("NO ") {
            return Ok(PactorLinkEvent::Status(PactorLinkStatus::LinkFailure));
        }
        if let Some(rest) = line.strip_prefix("LINK QUALITY ") {
            let mut speed_level = 0;
            let mut retries = 0;
            for field in rest.split_whitespace() {
                if let Some(value) = field.strip_prefix("SPEED=") {
                    speed_level = value.parse().unwrap_or_default();
                } else if let Some(value) = field.strip_prefix("RETRIES=") {
                    retries = value.parse().unwrap_or_default();
                }
            }
            return Ok(PactorLinkEvent::LinkQuality {
                speed_level,
                retries,
            });
        }
        Err(ScsPactorError::Protocol(line.to_owned()))
    }
}

impl Drop for UsbPactorTransport {
    fn drop(&mut self) {
        self.read_task.abort();
    }
}

async fn route_frame(
    frame: HostmodeFrame,
    command_tx: &mpsc::Sender<String>,
    data_tx: &mpsc::Sender<Vec<u8>>,
    event_tx: &mpsc::Sender<PactorLinkEvent>,
) -> Result<(), ScsPactorError> {
    match frame.channel {
        PACTOR_CHANNEL => {
            if frame.code & 0x01 != 0 {
                let line = String::from_utf8(frame.payload)
                    .map_err(|e| ScsPactorError::Protocol(e.to_string()))?;
                if let Ok(event) = UsbPactorTransport::parse_status_line(&line) {
                    let _ = event_tx.send(event).await;
                }
                command_tx
                    .send(line)
                    .await
                    .map_err(|_| ScsPactorError::Disconnected)?;
            } else {
                data_tx
                    .send(frame.payload)
                    .await
                    .map_err(|_| ScsPactorError::Disconnected)?;
            }
        }
        STATUS_CHANNEL => {
            if frame.payload.len() >= 3 {
                let _ = event_tx
                    .send(PactorLinkEvent::LinkQuality {
                        speed_level: frame.payload[2],
                        retries: 0,
                    })
                    .await;
            }
        }
        EXTENDED_POLL_CHANNEL => {}
        _ => {
            if frame.code & 0x01 != 0 {
                let line = String::from_utf8(frame.payload)
                    .map_err(|e| ScsPactorError::Protocol(e.to_string()))?;
                if let Ok(event) = UsbPactorTransport::parse_status_line(&line) {
                    let _ = event_tx.send(event).await;
                }
                let _ = command_tx.send(line).await;
            } else {
                let _ = data_tx.send(frame.payload).await;
            }
        }
    }
    Ok(())
}

/// Route terminal-mode status banners into command text and link events.
async fn route_terminal_line(
    line: &str,
    command_tx: &mpsc::Sender<String>,
    event_tx: &mpsc::Sender<PactorLinkEvent>,
) -> bool {
    let body = line.trim_start_matches('*').trim();
    let mut link_down = false;

    if let Some(rest) = body.strip_prefix("CONNECTED TO ") {
        let _ = event_tx
            .send(PactorLinkEvent::Status(PactorLinkStatus::Connected {
                remote_call: rest.trim().to_owned(),
            }))
            .await;
    } else if let Some(rest) = body.strip_prefix("NOW CALLING ") {
        let _ = event_tx
            .send(PactorLinkEvent::Status(PactorLinkStatus::Connecting {
                remote_call: rest.trim().to_owned(),
            }))
            .await;
    } else if body.starts_with("DISCONNECTED") {
        let _ = event_tx
            .send(PactorLinkEvent::Status(PactorLinkStatus::Disconnected))
            .await;
        link_down = true;
    } else if body.starts_with("LINK FAILURE")
        || body.starts_with("CONNECT FAILED")
        || body.starts_with("NO CONNECT")
        || body.starts_with("STBY")
    {
        // STBY is treated as link failure only by terminal-line routing.
        let _ = event_tx
            .send(PactorLinkEvent::Status(PactorLinkStatus::LinkFailure))
            .await;
        link_down = true;
    } else if let Some(event) = parse_quality_banner(body) {
        // Terminal mode must not poll status; parse only volunteered quality banners.
        let _ = event_tx.send(event).await;
    } else if !is_prompt_echo(body) {
        info!("[status-line?] unrecognized modem banner: {body:?}");
    }

    let _ = command_tx.send(body.to_owned()).await;
    link_down
}

/// Filters terminal prompt echoes produced by CR nudges.
fn is_prompt_echo(body: &str) -> bool {
    let b = body.trim();
    b.is_empty() || b == "cmd:" || b.ends_with("cmd:")
}

/// Parses volunteered terminal-mode speed/retry banners without soliciting status.
fn parse_quality_banner(body: &str) -> Option<PactorLinkEvent> {
    let upper = body.to_ascii_uppercase();

    if let Some(rest) = upper.strip_prefix("LINK QUALITY") {
        let mut speed_level = 0u8;
        let mut retries = 0u32;
        for field in rest.split_whitespace() {
            if let Some(value) = field.strip_prefix("SPEED=") {
                speed_level = value.parse().unwrap_or_default();
            } else if let Some(value) = field.strip_prefix("RETRIES=") {
                retries = value.parse().unwrap_or_default();
            }
        }
        return Some(PactorLinkEvent::LinkQuality {
            speed_level,
            retries,
        });
    }

    for prefix in ["SPEED-LEVEL", "SPEEDLEVEL", "SPEED LEVEL"] {
        if let Some(rest) = upper.strip_prefix(prefix) {
            if let Ok(level) = rest.trim().parse::<u8>() {
                return Some(PactorLinkEvent::LinkQuality {
                    speed_level: level,
                    retries: 0,
                });
            }
        }
    }

    None
}

#[async_trait]
impl PactorTransport for UsbPactorTransport {
    async fn set_mycall(&self, callsign: &str) -> Result<(), ScsPactorError> {
        let mut payload = vec![b'I', b' '];
        payload.extend_from_slice(callsign.as_bytes());
        let response = self
            .hostmode_transaction(HostmodeFrame::command(PACTOR_CHANNEL, payload))
            .await?;
        debug!(
            "set_mycall({callsign}): ch={} code=0x{:02x} payload={:?}",
            response.channel,
            response.code,
            String::from_utf8_lossy(&response.payload)
        );
        Ok(())
    }

    async fn connect_peer(&self, remote_call: &str) -> Result<(), ScsPactorError> {
        let _ = self.link_down_tx.send(false);
        // Dialing must use terminal text; framed hostmode bytes corrupt the callsign.
        let _ = self.write_raw(b"JHOST0\r").await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let cmd = format!("C {remote_call}\r");
        self.write_raw(cmd.as_bytes()).await?;
        debug!("[connect] C {remote_call} sent (terminal); waiting for link status ...");

        let deadline = Instant::now() + self.command_timeout;
        let mut saw_link_setup = false;
        let mut rx = self.event_rx.lock().await;

        // CR nudges fresh terminal status while waiting for link resolution.
        let nudge_interval = Duration::from_secs(2);

        loop {
            if Instant::now() >= deadline {
                return Err(ScsPactorError::Timeout);
            }
            let wait = nudge_interval.min(deadline.saturating_duration_since(Instant::now()));

            match timeout(wait, rx.recv()).await {
                Ok(Some(event)) => match event {
                    PactorLinkEvent::Status(PactorLinkStatus::Connected { remote_call }) => {
                        debug!("[connect] link established (CONNECTED TO {remote_call})");
                        // CONVerse mode makes subsequent `write_data` bytes transmit.
                        let _ = self.write_raw(b"CONV\r").await;
                        debug!("[connect] entered converse mode (CONV)");
                        return Ok(());
                    }
                    PactorLinkEvent::Status(PactorLinkStatus::Connecting { remote_call }) => {
                        debug!("[connect] calling {remote_call} ...");
                        saw_link_setup = true;
                    }
                    PactorLinkEvent::Status(PactorLinkStatus::Busy) => {
                        return Err(ScsPactorError::Busy);
                    }
                    PactorLinkEvent::Status(
                        PactorLinkStatus::Disconnected | PactorLinkStatus::LinkFailure,
                    ) => {
                        // Pre-call disconnect can be stale; after NOW CALLING it is failure.
                        if saw_link_setup {
                            return Err(ScsPactorError::Io(std::io::Error::other(
                                "PACTOR link setup failed",
                            )));
                        }
                        debug!("[connect] ignoring pre-call status (stale)");
                    }
                    _ => {}
                },
                Ok(None) => return Err(ScsPactorError::Disconnected),
                Err(_) => {
                    // Nudge the modem to re-emit terminal status.
                    let _ = self.write_raw(b"\r").await;
                }
            }
        }
    }

    async fn accept_incoming(
        &self,
        timeout_after: Option<Duration>,
    ) -> Result<String, ScsPactorError> {
        // Auto-answer requires terminal mode until CONNECTED, then CONVerse.
        let _ = self.link_down_tx.send(false);
        let _ = self.write_raw(b"JHOST0\r").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        debug!("[accept] listening; waiting for incoming CONNECTED ...");

        let deadline = Instant::now() + timeout_after.unwrap_or(self.command_timeout);
        let nudge_interval = Duration::from_secs(2);
        let mut rx = self.event_rx.lock().await;

        loop {
            if Instant::now() >= deadline {
                return Err(ScsPactorError::Timeout);
            }
            let wait = nudge_interval.min(deadline.saturating_duration_since(Instant::now()));
            match timeout(wait, rx.recv()).await {
                Ok(Some(PactorLinkEvent::Status(PactorLinkStatus::Connected { remote_call }))) => {
                    debug!("[accept] incoming link established (CONNECTED TO {remote_call})");
                    let _ = self.write_raw(b"CONV\r").await;
                    debug!("[accept] entered converse mode (CONV)");
                    return Ok(remote_call);
                }
                Ok(Some(PactorLinkEvent::Status(PactorLinkStatus::Connecting { remote_call }))) => {
                    debug!("[accept] incoming call from {remote_call} ...");
                }
                Ok(Some(_)) => {}
                Ok(None) => return Err(ScsPactorError::Disconnected),
                Err(_) => {
                    // Nudge the modem to re-emit terminal status.
                    let _ = self.write_raw(b"\r").await;
                }
            }
        }
    }

    async fn changeover(&self) -> Result<(), ScsPactorError> {
        // Ctrl-Z hands over the transmit turn locally; it is not sent over the air.
        debug!("[changeover] handing transmit turn to peer (Ctrl-Z)");
        self.write_raw(&[0x1a]).await
    }

    async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
        // Payloads travel as printable `#<hex>\r` terminal lines.
        let line = format!("{DATA_LINE_MARKER}{}\r", encode_hex(data));
        trace!("[data] write_data: {} bytes -> {:?}", data.len(), &line);
        let r = self.write_raw(line.as_bytes()).await;
        if let Err(e) = &r {
            debug!("[data] write_data error: {e}");
        }
        r
    }

    async fn read_data(&self, max_len: usize) -> Result<Vec<u8>, ScsPactorError> {
        trace!(
            "[data] read_data: waiting (timeout={:?}) ...",
            self.read_timeout
        );
        let mut rx = self.data_rx.lock().await;

        // Ignore stale link-down latches; only fresh drops fail this read.
        let mut link_down = self.link_down.clone();
        link_down.mark_unchanged();
        let recv_with_down = async {
            tokio::select! {
                biased;
                _ = link_down.changed() => None,
                msg = rx.recv() => Some(msg),
            }
        };

        let recv_result = if let Some(d) = self.read_timeout {
            match timeout(d, recv_with_down).await {
                Ok(inner) => inner,
                Err(_) => {
                    debug!("[data] read_data: timed out after {d:?}");
                    return Err(ScsPactorError::Timeout);
                }
            }
        } else {
            recv_with_down.await
        };

        let mut data = match recv_result {
            Some(Some(data)) => data,
            Some(None) => {
                debug!("[data] read_data: channel closed");
                return Err(ScsPactorError::Disconnected);
            }
            None => {
                debug!("[data] read_data: link dropped during transfer");
                return Err(ScsPactorError::Disconnected);
            }
        };

        trace!("[data] read_data: got {} bytes", data.len());
        data.truncate(max_len);
        Ok(data)
    }

    async fn disconnect(&self) -> Result<(), ScsPactorError> {
        // Leave CONVerse before terminal `D`; hostmode disconnect is wrong here.
        self.write_raw(&[0x1b]).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.write_raw(b"D\r").await?;
        Ok(())
    }

    fn is_link_up(&self) -> bool {
        !*self.link_down.borrow()
    }

    async fn next_event(
        &self,
        timeout_after: Option<Duration>,
    ) -> Result<PactorLinkEvent, ScsPactorError> {
        let mut rx = self.event_rx.lock().await;
        let read = rx.recv();
        if let Some(d) = timeout_after {
            timeout(d, read)
                .await
                .map_err(|_| ScsPactorError::Timeout)?
                .ok_or(ScsPactorError::Disconnected)
        } else {
            read.await.ok_or(ScsPactorError::Disconnected)
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{duplex, AsyncRead, AsyncReadExt};

    use super::*;
    use crate::hostmode::{
        decode_frame, encode_repeat_request, TYPE_COMMAND, TYPE_COMMAND_COUNTER, TYPE_DATA,
    };

    fn base_code(code: u8) -> u8 {
        code & 0x3F
    }

    fn test_config() -> UsbPactorConfig {
        UsbPactorConfig {
            port: "mock".to_owned(),
            baud_rate: 115_200,
            read_timeout: Some(Duration::from_millis(100)),
            write_timeout: Some(Duration::from_millis(100)),
            command_timeout: Duration::from_millis(100),
        }
    }

    async fn read_terminal_connect<R: AsyncRead + Unpin>(reader: &mut R) {
        let mut acc = Vec::new();
        let mut buf = [0u8; 256];
        loop {
            let n = reader.read(&mut buf).await.unwrap();
            acc.extend_from_slice(&buf[..n]);
            if acc.windows(b"C NODE".len()).any(|w| w == b"C NODE") {
                return;
            }
        }
    }

    #[tokio::test]
    async fn usb_transport_writes_commands_as_hostmode_frames() {
        let (transport_side, mut modem_side) = duplex(4096);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        let modem = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = modem_side.read(&mut buf).await.unwrap();
            let frame = decode_frame(&buf[..n]).unwrap();
            assert_eq!(frame.channel, PACTOR_CHANNEL);
            assert_eq!(base_code(frame.code), TYPE_COMMAND);
            assert_eq!(frame.payload, b"I N0CALL");

            let ok = encode_frame(&HostmodeFrame::command(PACTOR_CHANNEL, b"OK".to_vec())).unwrap();
            modem_side.write_all(&ok).await.unwrap();
        });

        transport.set_mycall("N0CALL").await.unwrap();
        modem.await.unwrap();
    }

    #[tokio::test]
    async fn usb_transport_demuxes_command_events_and_data() {
        let (transport_side, mut modem_side) = duplex(2048);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        let status = encode_frame(&HostmodeFrame::command(
            PACTOR_CHANNEL,
            b"CONNECTED NODE".to_vec(),
        ))
        .unwrap();
        let data = encode_frame(&HostmodeFrame::new(PACTOR_CHANNEL, b"payload".to_vec())).unwrap();

        modem_side.write_all(&[0x00, 0x01, 0x02]).await.unwrap();
        modem_side.write_all(&status).await.unwrap();
        modem_side.write_all(&data).await.unwrap();

        assert_eq!(
            transport
                .next_event(Some(Duration::from_millis(100)))
                .await
                .unwrap(),
            PactorLinkEvent::Status(PactorLinkStatus::Connected {
                remote_call: "NODE".to_owned()
            })
        );
        assert_eq!(
            transport.read_status_line().await.unwrap(),
            "CONNECTED NODE"
        );
        assert_eq!(transport.read_data(1024).await.unwrap(), b"payload");
    }

    #[tokio::test]
    async fn usb_transport_connect_peer_waits_for_connected_status() {
        let (transport_side, mut modem_side) = duplex(4096);
        let mut config = test_config();
        config.command_timeout = Duration::from_secs(10);
        let transport = UsbPactorTransport::from_stream(transport_side, config);

        let modem = tokio::spawn(async move {
            read_terminal_connect(&mut modem_side).await;

            modem_side
                .write_all(b"\r\n*** NOW CALLING NODE\r\n")
                .await
                .unwrap();
            modem_side
                .write_all(b"\r\n*** CONNECTED TO NODE\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        transport.connect_peer("NODE").await.unwrap();
        modem.await.unwrap();
    }

    #[tokio::test]
    async fn usb_transport_connect_peer_reports_link_failure() {
        let (transport_side, mut modem_side) = duplex(4096);
        let mut config = test_config();
        config.command_timeout = Duration::from_secs(10);
        let transport = UsbPactorTransport::from_stream(transport_side, config);

        let modem = tokio::spawn(async move {
            read_terminal_connect(&mut modem_side).await;

            modem_side
                .write_all(b"\r\n*** NOW CALLING NODE\r\n")
                .await
                .unwrap();
            modem_side
                .write_all(b"\r\n*** DISCONNECTED AT - 00:00:00\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let err = transport.connect_peer("NODE").await.unwrap_err();
        assert!(matches!(err, ScsPactorError::Io(_)));
        modem.await.unwrap();
    }

    #[tokio::test]
    async fn usb_transport_connect_peer_ignores_stale_disconnect() {
        let (transport_side, mut modem_side) = duplex(4096);
        let mut config = test_config();
        config.command_timeout = Duration::from_secs(10);
        let transport = UsbPactorTransport::from_stream(transport_side, config);

        let modem = tokio::spawn(async move {
            read_terminal_connect(&mut modem_side).await;

            modem_side
                .write_all(b"\r\n*** DISCONNECTED AT - 00:00:00\r\n")
                .await
                .unwrap();
            modem_side
                .write_all(b"\r\n*** NOW CALLING NODE\r\n")
                .await
                .unwrap();
            modem_side
                .write_all(b"\r\n*** CONNECTED TO NODE\r\n")
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        transport.connect_peer("NODE").await.unwrap();
        modem.await.unwrap();
    }

    #[tokio::test]
    async fn usb_transport_reports_disconnect_when_device_unplugs() {
        let (transport_side, modem_side) = duplex(1024);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());
        drop(modem_side);

        assert_eq!(
            transport
                .next_event(Some(Duration::from_millis(100)))
                .await
                .unwrap(),
            PactorLinkEvent::Status(PactorLinkStatus::Disconnected)
        );
    }

    #[tokio::test]
    async fn usb_transport_writes_disconnect_as_terminal_command() {
        let (transport_side, mut modem_side) = duplex(1024);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        transport.disconnect().await.unwrap();

        let mut acc = Vec::new();
        let mut buf = [0u8; 1024];
        while !acc.windows(2).any(|w| w == b"D\r") {
            let n = modem_side.read(&mut buf).await.unwrap();
            acc.extend_from_slice(&buf[..n]);
        }
        assert!(acc.contains(&0x1b), "expected ESC before disconnect");
        assert!(
            acc.windows(2).any(|w| w == b"D\r"),
            "expected terminal D command"
        );
    }

    #[tokio::test]
    async fn usb_hostmode_transaction_retransmits_after_repeat_request() {
        let (transport_side, mut modem_side) = duplex(4096);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        let modem = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = modem_side.read(&mut buf).await.unwrap();
            let first = buf[..n].to_vec();
            let frame = decode_frame(&first).unwrap();
            assert_eq!(frame.channel, PACTOR_CHANNEL);
            assert_eq!(base_code(frame.code), TYPE_COMMAND);
            assert_eq!(frame.payload, b"V");

            modem_side
                .write_all(&encode_repeat_request())
                .await
                .unwrap();

            let n = modem_side.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], first.as_slice());

            let response =
                encode_frame(&HostmodeFrame::command(PACTOR_CHANNEL, b"OK".to_vec())).unwrap();
            modem_side.write_all(&response).await.unwrap();
        });

        let response = transport
            .hostmode_transaction(HostmodeFrame::command(PACTOR_CHANNEL, b"V".to_vec()))
            .await
            .unwrap();
        assert_eq!(response.channel, PACTOR_CHANNEL);
        assert_eq!(response.payload, b"OK");
        modem.await.unwrap();
    }

    #[tokio::test]
    async fn usb_poll_pending_channels_uses_extended_hostmode_channel() {
        let (transport_side, mut modem_side) = duplex(4096);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        let modem = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = modem_side.read(&mut buf).await.unwrap();
            let frame = decode_frame(&buf[..n]).unwrap();
            assert_eq!(frame.channel, EXTENDED_POLL_CHANNEL);
            assert_eq!(base_code(frame.code), TYPE_COMMAND);
            assert_eq!(frame.payload, b"G");

            let response = encode_frame(&HostmodeFrame::with_code(
                EXTENDED_POLL_CHANNEL,
                TYPE_COMMAND,
                vec![3, 4, 0],
            ))
            .unwrap();
            modem_side.write_all(&response).await.unwrap();
        });

        let channels = transport.poll_pending_channels().await.unwrap();
        assert_eq!(channels, vec![2, 3]);
        modem.await.unwrap();
    }

    #[test]
    fn quality_banners_parse_tolerantly() {
        match parse_quality_banner("LINK QUALITY SPEED=3 RETRIES=7") {
            Some(PactorLinkEvent::LinkQuality {
                speed_level,
                retries,
            }) => {
                assert_eq!(speed_level, 3);
                assert_eq!(retries, 7);
            }
            other => panic!("expected LinkQuality, got {other:?}"),
        }
        match parse_quality_banner("Speed-Level 2") {
            Some(PactorLinkEvent::LinkQuality {
                speed_level,
                retries,
            }) => {
                assert_eq!(speed_level, 2);
                assert_eq!(retries, 0);
            }
            other => panic!("expected LinkQuality, got {other:?}"),
        }
        assert!(parse_quality_banner("SOME UNKNOWN BANNER").is_none());
        assert!(parse_quality_banner("SPEED-LEVEL notanumber").is_none());
    }

    #[tokio::test]
    async fn terminal_quality_banner_routes_to_events() {
        let (command_tx, _command_rx) = mpsc::channel(8);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let link_down =
            route_terminal_line("*** LINK QUALITY SPEED=4 RETRIES=2", &command_tx, &event_tx).await;
        assert!(!link_down);
        match event_rx.try_recv().unwrap() {
            PactorLinkEvent::LinkQuality {
                speed_level,
                retries,
            } => {
                assert_eq!(speed_level, 4);
                assert_eq!(retries, 2);
            }
            other => panic!("expected LinkQuality, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn usb_poll_status_returns_status_payload() {
        let (transport_side, mut modem_side) = duplex(4096);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        let modem = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = modem_side.read(&mut buf).await.unwrap();
            let frame = decode_frame(&buf[..n]).unwrap();
            assert_eq!(frame.channel, STATUS_CHANNEL);
            assert_eq!(base_code(frame.code), TYPE_COMMAND);

            let response = encode_frame(&HostmodeFrame::with_code(
                STATUS_CHANNEL,
                TYPE_DATA,
                vec![1, 3, 5],
            ))
            .unwrap();
            modem_side.write_all(&response).await.unwrap();
        });

        assert_eq!(transport.poll_status().await.unwrap(), vec![1, 3, 5]);
        assert_eq!(
            transport
                .next_event(Some(Duration::from_millis(100)))
                .await
                .unwrap(),
            PactorLinkEvent::LinkQuality {
                speed_level: 5,
                retries: 0
            }
        );
        modem.await.unwrap();
    }

    #[tokio::test]
    async fn usb_transport_writes_data_as_hex_line() {
        let (transport_side, mut modem_side) = duplex(1024);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        transport.write_data(b"hello").await.unwrap();

        let mut buf = [0u8; 1024];
        let n = modem_side.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"#68656c6c6f\r");
    }

    #[tokio::test]
    async fn usb_transport_reads_data_from_hex_line() {
        let (transport_side, mut modem_side) = duplex(1024);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        modem_side
            .write_all(b"\r\n*** CONNECTED TO NODE\r\n")
            .await
            .unwrap();
        modem_side.write_all(b"#68656c6c6f\r").await.unwrap();

        let data = transport.read_data(1024).await.unwrap();
        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn usb_transport_toggles_packet_counter() {
        let (transport_side, mut modem_side) = duplex(2048);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        let modem = tokio::spawn(async move {
            let mut decoder = HostmodeDecoder::new();
            let mut buf = [0u8; 1024];
            let mut frames = Vec::new();

            for _ in 0..2 {
                let n = modem_side.read(&mut buf).await.unwrap();
                decoder.push(&buf[..n]);
                while let Some(frame) = decoder.next_frame().unwrap() {
                    frames.push(frame);
                    let ack = encode_frame(&HostmodeFrame::command(PACTOR_CHANNEL, b"OK".to_vec()))
                        .unwrap();
                    modem_side.write_all(&ack).await.unwrap();
                }
            }
            frames
        });

        transport.set_mycall("A").await.unwrap();
        transport.set_mycall("B").await.unwrap();

        let frames = modem.await.unwrap();
        assert_eq!(frames.len(), 2);

        assert_eq!(frames[0].code, TYPE_COMMAND);
        assert_eq!(frames[0].payload, b"I A");

        assert_eq!(frames[1].code, TYPE_COMMAND_COUNTER);
        assert_eq!(frames[1].payload, b"I B");
    }
}
