use std::time::Duration;

use async_trait::async_trait;
use log::{debug, trace, warn};
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

/// Marker prefixing a data frame sent over the connected terminal-mode link.
///
/// After the firmware reverts to terminal mode on connect, payload bytes are
/// carried as a hex-encoded line `#<hex>\r`. Hex keeps the payload printable so
/// it survives the text-oriented PACTOR terminal link, and the `#` marker lets
/// the reader tell data lines apart from `*** ...` status banners.
const DATA_LINE_MARKER: &str = "#";

/// Hex-encode bytes (lowercase, no separators).
fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// Decode a hex line back to bytes, returning None on malformed input.
fn decode_hex_line(hex: &str) -> Option<Vec<u8>> {
    let hex = hex.trim();
    if hex.is_empty() || hex.len() % 2 != 0 {
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
    /// Packet counter state. The SCS hostmode protocol toggles bit 7 (0x80) of
    /// the type byte on each successfully ACKed frame. The first frame after
    /// entering hostmode must set bit 6 (0x40) to reset the modem's counter.
    packet_counter: Mutex<PacketCounter>,
    /// Set to `true` by the reader when the link drops (DISCONNECTED / STBY /
    /// link failure), so a blocked `read_data` can fail fast instead of waiting
    /// the full read timeout. A `watch` (not a Notify) is used so a drop that
    /// happens *between* receives is still observed by the next `read_data`
    /// (important for batch receives, where the reader isn't always parked).
    link_down: tokio::sync::watch::Receiver<bool>,
    /// Sender side of `link_down`, kept so a fresh connect can clear a stale
    /// drop flag from a previous session.
    link_down_tx: watch::Sender<bool>,
    read_task: JoinHandle<()>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    command_timeout: Duration,
}

#[derive(Debug)]
struct PacketCounter {
    /// Whether the next frame should have the counter bit (0x80) set.
    /// Matches ptc-go: starts false, toggles after each successful ACK.
    toggle: bool,
}

impl PacketCounter {
    fn new() -> Self {
        Self { toggle: false }
    }

    /// Apply counter bit to the type byte for the next outbound frame.
    /// ptc-go uses only bits 0 (cmd/data) and 7 (counter toggle):
    ///   0x00 = data, counter=0
    ///   0x01 = command, counter=0
    ///   0x80 = data, counter=1
    ///   0x81 = command, counter=1
    fn apply(&self, code: u8) -> u8 {
        let base = code & 0x01; // keep only cmd/data bit
        if self.toggle {
            base | 0x80
        } else {
            base
        }
    }

    /// Advance the counter after a successful ACK (not a repeat request).
    fn advance(&mut self) {
        self.toggle = !self.toggle;
    }

    /// Reset the toggle to the initial (parity-0) state.
    ///
    /// ptc-go enters its connect with `packetcounter = false` (its init
    /// commands use raw ASCII writes that never toggle), so the modem expects
    /// the connect to be the first parity-0 channel command. Any prior
    /// hostmode poll (e.g. a verify) leaves us toggled, so reset before
    /// connecting to match the modem's expectation.
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

        // Short port label so the two modems' reader logs are distinguishable.
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

                // This SCS Dragon firmware reverts to terminal mode when it
                // processes a connect command, then reports link state as plain
                // ASCII lines (e.g. "*** CONNECTED TO NODE", "*** DISCONNECTED")
                // rather than hostmode frames. Accumulate those lines and route
                // them as status so connect/disconnect can be observed. Hostmode
                // frame bytes (0xAA-framed, with control/CRC bytes) never form a
                // clean "*** "/"cmd:" line, so this won't misfire on framed data.
                for &b in &buf[..n] {
                    if b == b'\r' || b == b'\n' {
                        if !term_line.is_empty() {
                            let line = String::from_utf8_lossy(&term_line).trim().to_string();
                            if !line.is_empty() {
                                // Data frames are sent over the connected terminal
                                // link as a hex line prefixed with DATA_LINE_MARKER
                                // ('#'). Decode and route those to the data stream;
                                // everything else is treated as status text.
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
                                        // Wake any blocked read_data so it can
                                        // fail fast instead of waiting the full
                                        // timeout on a link that just dropped.
                                        let _ = link_down_tx.send(true);
                                    }
                                }
                            }
                            term_line.clear();
                        }
                    } else if (b.is_ascii_graphic() || b == b' ') && term_line.len() < 4096 {
                        term_line.push(b);
                    } else {
                        // Non-printable (likely hostmode framing) — drop the
                        // partial terminal line so framed bytes aren't parsed
                        // as text.
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

    /// Reset the hostmode packet-counter toggle to parity 0.
    ///
    /// Call this before issuing a connect if a prior hostmode poll (e.g. a
    /// post-init verify) has advanced the toggle — the modem expects the
    /// connect to be the first parity-0 channel command (matches ptc-go).
    pub async fn reset_packet_counter(&self) {
        self.packet_counter.lock().await.reset();
    }

    pub async fn send_hostmode_frame_no_response(
        &self,
        frame: HostmodeFrame,
    ) -> Result<(), ScsPactorError> {
        self.send_hostmode_frame(frame).await
    }

    /// Send a command with the proper packet counter and try to read an ACK.
    ///
    /// The hostmode toggle bit (0x80) is a sequence number the modem uses to
    /// detect retransmissions: it only flips once the modem has *acknowledged*
    /// the frame. If the modem does not respond, the frame was not consumed, so
    /// the counter must NOT advance — otherwise every subsequent frame lands on
    /// the wrong toggle and the modem silently discards all of them (observed
    /// on real hardware: a no-ACK `C` connect desynced the whole session).
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

    /// Write raw bytes directly to the serial port (no hostmode framing).
    ///
    /// Used for terminal-mode interaction (connect, converse data, disconnect)
    /// where the modem is not in hostmode and expects plain ASCII rather than
    /// CRC-framed packets. Any reply is surfaced by the background reader.
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
                    // Successful ACK — advance the packet counter
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
                    // Don't advance counter on repeat — resend same frame
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

    /// Poll a channel by sending a command frame with payload "G".
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

    /// Send a command on the PACTOR channel.
    ///
    /// In ptc-go hostmode, the command letter (e.g. `I`, `C`, `D`) is part
    /// of the payload with type byte = 0x01. So `set_mycall("N0CALL")`
    /// sends: channel=31, type=0x01, payload=`"I N0CALL"`.
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
            // On the PACTOR channel, type byte distinguishes command responses
            // (TYPE_COMMAND / TYPE_COMMAND_COUNTER) from data.
            if frame.code & 0x01 != 0 {
                // Command response — payload is ASCII status text
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
                // Data frame
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
            // Other channels: try to route as command if type says so, else data
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

/// Parse a terminal-mode status line emitted by the modem after it reverts from
/// hostmode on connect, and route it to the command stream and link events.
///
/// Handles the SCS terminal phrasing: lines are prefixed with `*** ` and use
/// `CONNECTED TO <call>` / `DISCONNECTED ...` / `NOW CALLING <call>` rather than
/// the hostmode `CONNECTED <call>` form parsed by [`parse_status_line`].
/// Returns `true` if the line indicates the link went down (DISCONNECTED / STBY
/// / link failure), so the reader can wake any blocked `read_data`.
async fn route_terminal_line(
    line: &str,
    command_tx: &mpsc::Sender<String>,
    event_tx: &mpsc::Sender<PactorLinkEvent>,
) -> bool {
    // Strip the leading "*** " banner marker if present.
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
        // STBY (standby) after a call attempt means the modem gave up — the link
        // setup failed. (It also follows a normal disconnect, which connect_peer
        // only treats as a failure once a call was actually in progress.)
        let _ = event_tx
            .send(PactorLinkEvent::Status(PactorLinkStatus::LinkFailure))
            .await;
        link_down = true;
    }

    // Always forward the raw line so callers polling read_status_line() see it.
    let _ = command_tx.send(body.to_owned()).await;
    link_down
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
        // Clear any stale link-down flag from a previous session.
        let _ = self.link_down_tx.send(false);
        // On this SCS Dragon firmware the connect runs in TERMINAL mode, not
        // hostmode: a framed hostmode `C` command is parsed as literal typed
        // characters, and its trailing CRC bytes leak into the callsign (e.g.
        // "C NODE" + CRC 0x73('s') was dialed as "NODES"). So issue the connect
        // as a clean terminal command ("C <CALL>\r"), exactly as the manual
        // (picocom) flow does. The modem then reports link state as plain ASCII
        // lines ("*** NOW CALLING X" / "*** CONNECTED TO X" / "*** DISCONNECTED"),
        // which the background reader parses into PactorLinkEvents (see
        // route_terminal_line); we wait on that event stream.
        // Ensure we are in terminal mode: leave JHOST hostmode first. The modem
        // ignores a stray JHOST0 if already in terminal mode (harmless), and a
        // short settle lets it switch before we type the connect.
        let _ = self.write_raw(b"JHOST0\r").await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let cmd = format!("C {remote_call}\r");
        self.write_raw(cmd.as_bytes()).await?;
        debug!("[connect] C {remote_call} sent (terminal); waiting for link status ...");

        let deadline = Instant::now() + self.command_timeout;
        let mut saw_link_setup = false;
        let mut rx = self.event_rx.lock().await;

        // Period between CR nudges. The firmware only emits its status banner in
        // response to a terminal newline, so we poke it periodically until the
        // link resolves rather than blocking on a single recv.
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
                        // The connected modem sits at the command prompt, where
                        // typed text is parsed as commands, not transmitted. Enter
                        // CONVerse mode so subsequent write_data bytes are actually
                        // sent over the link to the peer.
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
                        // A disconnect before we ever saw the call start is just
                        // stale status from a prior session; ignore it and keep
                        // waiting. After NOW CALLING, it means link setup failed.
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
                    // No event this interval — nudge the modem to re-emit status.
                    let _ = self.write_raw(b"\r").await;
                }
            }
        }
    }

    async fn accept_incoming(
        &self,
        timeout_after: Option<Duration>,
    ) -> Result<String, ScsPactorError> {
        // The answering modem is in LISTEN mode. It must stay in terminal/command
        // mode and let the firmware auto-answer the incoming call at the RF level;
        // it must NOT enter CONVerse until the link is actually up. Entering
        // converse early (the old behaviour) only worked when a single process
        // choreographed both modems — across two independent processes it makes
        // the answerer leave command mode before the caller's `C` lands, so the
        // call finds no listener and drops back to the `cmd:` prompt.
        //
        // So wait for the real "*** CONNECTED TO <call>" status (parsed by the
        // background reader into a Connected event), nudging with CR periodically
        // so the firmware re-emits its banner, then enter CONVerse so this side
        // can also transmit.
        let _ = self.link_down_tx.send(false);
        // Leave JHOST hostmode so the listener is in terminal mode: it auto-
        // answers the incoming call and reports link state as the plain ASCII
        // "*** CONNECTED TO X" banner the reader parses (same as the caller side).
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
                    // No event this interval — nudge so the firmware re-emits its
                    // status banner (it only prints in response to a newline).
                    let _ = self.write_raw(b"\r").await;
                }
            }
        }
    }

    async fn changeover(&self) -> Result<(), ScsPactorError> {
        // Send the CHANGEOVER character (Ctrl-Z, 0x1A, set via "CHO 26" at init)
        // to hand the transmit turn to the peer. It is consumed locally by the
        // modem (not sent over the air), so it never appears in the peer's data.
        debug!("[changeover] handing transmit turn to peer (Ctrl-Z)");
        self.write_raw(&[0x1a]).await
    }

    async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
        // After a terminal-mode connect the modem carries payload as raw serial
        // bytes over the link. Send each message as a hex-encoded line
        // ("#<hex>\r") so it stays printable on the text-oriented PACTOR link and
        // is delimited for the receiver (see DATA_LINE_MARKER / the reader).
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

        // Wait for either a data frame, a fresh link-down signal (fail fast on a
        // mid-transfer disconnect instead of blocking the full timeout), or the
        // read timeout. Mark the current watch value as seen so only a *new* drop
        // during this wait trips us — a stale latched value from earlier in the
        // session must not false-fail this receive (e.g. A's flag latched during
        // a long A->B leg would otherwise wrongly fail the following B->A leg).
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
        // After a data session the modem is in terminal CONVerse mode, not
        // hostmode — so a framed hostmode `D` would be garbage and leave the
        // modem wedged in converse for the next session. Return to command mode
        // with ESC (0x1B) first, then issue the terminal disconnect command.
        self.write_raw(&[0x1b]).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.write_raw(b"D\r").await?;
        Ok(())
    }

    fn is_link_up(&self) -> bool {
        // The reader sets link_down=true on DISCONNECTED / STBY / LINK FAILURE /
        // EOF; connect/accept reset it to false. So "up" == not flagged down.
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

    /// Strip counter bits (7,6) from the type byte to get the base code.
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

    /// Drain the terminal-mode connect bytes connect_peer writes and assert the
    /// "C NODE" command appears (it sends "JHOST0\r" then "C NODE\r").
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

            // Respond with OK (set_mycall now uses hostmode_transaction)
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

        // Command response on PACTOR channel (type=COMMAND)
        let status = encode_frame(&HostmodeFrame::command(
            PACTOR_CHANNEL,
            b"CONNECTED NODE".to_vec(),
        ))
        .unwrap();
        // Data on PACTOR channel (type=DATA)
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
            // The connect is issued in terminal mode: the modem receives the
            // raw "JHOST0\r" + "C NODE\r" text, then reports link state as plain
            // ASCII lines.
            read_terminal_connect(&mut modem_side).await;

            modem_side
                .write_all(b"\r\n*** NOW CALLING NODE\r\n")
                .await
                .unwrap();
            modem_side
                .write_all(b"\r\n*** CONNECTED TO NODE\r\n")
                .await
                .unwrap();
            // Keep the pipe open so connect_peer's CR nudges don't BrokenPipe.
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

            // Call starts, then fails (disconnect after NOW CALLING).
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

            // Stale disconnect from a prior session arrives first — must be
            // ignored — then the real call proceeds to CONNECTED.
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

        // Disconnect leaves converse mode with ESC (0x1B), then issues "D\r" as
        // a terminal command (the modem is in terminal mode after a session).
        let mut acc = Vec::new();
        let mut buf = [0u8; 1024];
        // ESC and "D\r" may arrive in separate reads.
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

        // Data is sent over the connected terminal link as "#<hex>\r".
        let mut buf = [0u8; 1024];
        let n = modem_side.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"#68656c6c6f\r");
    }

    #[tokio::test]
    async fn usb_transport_reads_data_from_hex_line() {
        let (transport_side, mut modem_side) = duplex(1024);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        // Modem delivers received data as a hex line; status banners are ignored.
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

        // Use hostmode_transaction so counter advances after each ACK.
        let modem = tokio::spawn(async move {
            let mut decoder = HostmodeDecoder::new();
            let mut buf = [0u8; 1024];
            let mut frames = Vec::new();

            for _ in 0..2 {
                let n = modem_side.read(&mut buf).await.unwrap();
                decoder.push(&buf[..n]);
                while let Some(frame) = decoder.next_frame().unwrap() {
                    frames.push(frame);
                    // Send ACK response
                    let ack = encode_frame(&HostmodeFrame::command(PACTOR_CHANNEL, b"OK".to_vec()))
                        .unwrap();
                    modem_side.write_all(&ack).await.unwrap();
                }
            }
            frames
        });

        // set_mycall uses hostmode_transaction
        transport.set_mycall("A").await.unwrap();
        transport.set_mycall("B").await.unwrap();

        let frames = modem.await.unwrap();
        assert_eq!(frames.len(), 2);

        // First frame: counter=false → type=0x01
        assert_eq!(frames[0].code, TYPE_COMMAND);
        assert_eq!(frames[0].payload, b"I A");

        // Second frame: counter toggled → type=0x81
        assert_eq!(frames[1].code, TYPE_COMMAND_COUNTER);
        assert_eq!(frames[1].payload, b"I B");
    }
}
