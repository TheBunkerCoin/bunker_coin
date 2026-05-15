use std::time::Duration;

use async_trait::async_trait;
use log::{debug, trace, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};
use tokio_serial::{DataBits, FlowControl, Parity, SerialPortBuilderExt, StopBits};

use crate::hostmode::{
    encode_frame, HostmodeDecoder, HostmodeFrame, HostmodePacket, PACTOR_CHANNEL, TYPE_COMMAND,
};
use crate::{PactorLinkEvent, PactorLinkStatus, PactorTransport, ScsPactorError};

const STATUS_CHANNEL: u8 = 254;
const EXTENDED_POLL_CHANNEL: u8 = 255;
const MAX_HOSTMODE_RETRIES: u8 = 3;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PactorChannelState {
    status_messages_pending: u32,
    frames_received_pending: u32,
    frames_not_transmitted: u32,
    frames_not_acknowledged: u32,
    retries: u32,
    link_state: u32,
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

        let read_task = tokio::spawn(async move {
            let mut decoder = HostmodeDecoder::new();
            let mut buf = [0u8; 1024];
            eprintln!("[reader] task started");

            loop {
                let n = match reader.read(&mut buf).await {
                    Ok(0) => {
                        eprintln!("[reader] EOF on serial stream");
                        let _ = event_tx
                            .send(PactorLinkEvent::Status(PactorLinkStatus::Disconnected))
                            .await;
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("[reader] serial read error: {e}");
                        let _ = event_tx
                            .send(PactorLinkEvent::Status(PactorLinkStatus::LinkFailure))
                            .await;
                        break;
                    }
                };

                eprintln!("[reader] got {} bytes: {:02x?}", n, &buf[..n]);
                decoder.push(&buf[..n]);
                loop {
                    match decoder.next_packet() {
                        Ok(Some(HostmodePacket::Frame(frame))) => {
                            let ascii = String::from_utf8_lossy(&frame.payload);
                            eprintln!(
                                "[reader] decoded frame ch={} code=0x{:02x} payload({})={:02x?} ascii={:?}",
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
                            eprintln!("[reader] decoded RepeatRequest");
                            let _ = packet_tx.send(HostmodePacket::RepeatRequest).await;
                        }
                        Ok(None) => break,
                        Err(e) => {
                            eprintln!("[reader] decode error: {e}");
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

    pub async fn send_hostmode_frame_no_response(
        &self,
        frame: HostmodeFrame,
    ) -> Result<(), ScsPactorError> {
        self.send_hostmode_frame(frame).await
    }

    /// Send a command with the proper packet counter and try to read an ACK.
    ///
    /// If the modem ACKs within `ack_timeout`, the counter advances normally.
    /// Otherwise the counter is advanced manually (the modem accepted the
    /// counter-correct frame but chose not to respond).
    pub async fn send_command_best_effort_ack(
        &self,
        frame: HostmodeFrame,
        ack_timeout: Duration,
    ) -> Result<Option<HostmodeFrame>, ScsPactorError> {
        let _lock = self.transaction_lock.lock().await;
        let encoded = self.encode_outbound_frame(frame).await?;
        self.write_encoded_frame(&encoded).await?;
        match self.recv_hostmode_packet(ack_timeout).await {
            Ok(HostmodePacket::Frame(resp)) => {
                self.packet_counter.lock().await.advance();
                Ok(Some(resp))
            }
            _ => {
                self.packet_counter.lock().await.advance();
                Ok(None)
            }
        }
    }

    async fn encode_outbound_frame(
        &self,
        frame: HostmodeFrame,
    ) -> Result<Vec<u8>, ScsPactorError> {
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
        eprintln!(
            "[tx] hostmode_transaction: ch={} code=0x{:02x} payload={:02x?} encoded={:02x?}",
            frame.channel, frame.code, &frame.payload, &encoded
        );
        let mut retries = 0;

        loop {
            self.write_encoded_frame(&encoded).await?;
            match self.recv_hostmode_packet(self.command_timeout).await? {
                HostmodePacket::Frame(response) => {
                    // Successful ACK — advance the packet counter
                    self.packet_counter.lock().await.advance();
                    eprintln!(
                        "[tx] hostmode_transaction response: ch={} code=0x{:02x} payload={:02x?}",
                        response.channel, response.code, &response.payload
                    );
                    return Ok(response);
                }
                HostmodePacket::RepeatRequest => {
                    retries += 1;
                    eprintln!("[tx] repeat request (retry {retries}/{MAX_HOSTMODE_RETRIES})");
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

    async fn poll_pactor_channel_state(&self) -> Result<PactorChannelState, ScsPactorError> {
        let response = self
            .hostmode_transaction(HostmodeFrame::command(PACTOR_CHANNEL, b"L".to_vec()))
            .await?;
        if response.channel != PACTOR_CHANNEL {
            return Err(ScsPactorError::Protocol(format!(
                "expected channel {PACTOR_CHANNEL} L response, got {}",
                response.channel
            )));
        }
        parse_pactor_channel_state(&response.payload)
    }

    /// Send a data frame on the given channel.
    async fn send_data_frame(&self, channel: u8, payload: &[u8]) -> Result<(), ScsPactorError> {
        self.send_hostmode_frame(HostmodeFrame::new(channel, payload.to_vec()))
            .await
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

fn parse_pactor_channel_state(payload: &[u8]) -> Result<PactorChannelState, ScsPactorError> {
    let line = String::from_utf8_lossy(payload);
    let fields = line
        .trim_matches(char::from(0))
        .split_whitespace()
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ScsPactorError::Protocol(format!("invalid L response {line:?}: {e}")))?;

    if fields.len() < 6 {
        return Err(ScsPactorError::Protocol(format!(
            "short L response {line:?}"
        )));
    }

    Ok(PactorChannelState {
        status_messages_pending: fields[0],
        frames_received_pending: fields[1],
        frames_not_transmitted: fields[2],
        frames_not_acknowledged: fields[3],
        retries: fields[4],
        link_state: fields[5],
    })
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
        let mut payload = b"C ".to_vec();
        payload.extend_from_slice(remote_call.as_bytes());
        let frame = HostmodeFrame::command(PACTOR_CHANNEL, payload);
        let ack = self
            .send_command_best_effort_ack(frame, Duration::from_secs(2))
            .await?;
        match &ack {
            Some(resp) => eprintln!(
                "[connect] C {remote_call} ACKed: payload={:?}",
                String::from_utf8_lossy(&resp.payload)
            ),
            None => eprintln!("[connect] C {remote_call} sent (no ACK, counter advanced)"),
        }

        let deadline = Instant::now() + self.command_timeout;
        let mut saw_link_setup = false;
        let mut poll_count = 0u32;
        // The modem may not respond to polls while it is busy with an RF
        // connect attempt. Use a short per-poll timeout and keep retrying
        // until the overall deadline. The modem will resume responding
        // once its internal connect attempt completes or times out.
        let poll_timeout = Duration::from_secs(5);

        loop {
            if Instant::now() >= deadline {
                return Err(ScsPactorError::Timeout);
            }

            poll_count += 1;
            let l_frame = HostmodeFrame::command(PACTOR_CHANNEL, b"L".to_vec());
            let l_resp = self
                .send_command_best_effort_ack(l_frame, poll_timeout)
                .await?;
            let state = match l_resp {
                Some(resp) => {
                    eprintln!(
                        "[connect] poll {poll_count}: raw={:?}",
                        String::from_utf8_lossy(&resp.payload)
                    );
                    match parse_pactor_channel_state(&resp.payload) {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("[connect] poll {poll_count}: parse error: {e}");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                    }
                }
                None => {
                    eprintln!(
                        "[connect] poll {poll_count}: no response (modem busy with RF)"
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            eprintln!(
                "[connect] poll {poll_count}: link_state={} status_pending={} rx_pending={} retries={}",
                state.link_state,
                state.status_messages_pending,
                state.frames_received_pending,
                state.retries,
            );

            match state.link_state {
                1 => saw_link_setup = true,
                2 | 4 | 5 | 6 => {
                    eprintln!("[connect] link established (state={})", state.link_state);
                    return Ok(());
                }
                0 if saw_link_setup => {
                    return Err(ScsPactorError::Io(std::io::Error::other(
                        "PACTOR link setup failed (returned to idle after connecting)",
                    )))
                }
                _ => {}
            }

            // Also check if there are status messages we should read
            if state.status_messages_pending > 0 {
                let g_frame = HostmodeFrame::command(PACTOR_CHANNEL, b"G".to_vec());
                if let Some(g_resp) = self
                    .send_command_best_effort_ack(g_frame, poll_timeout)
                    .await?
                {
                    let g_text = String::from_utf8_lossy(&g_resp.payload);
                    eprintln!("[connect] G poll: {:?}", g_text);
                    if let Ok(event) = Self::parse_status_line(&g_text) {
                        match event {
                            PactorLinkEvent::Status(PactorLinkStatus::Connected { .. }) => {
                                return Ok(())
                            }
                            PactorLinkEvent::Status(PactorLinkStatus::Busy) => {
                                return Err(ScsPactorError::Busy)
                            }
                            PactorLinkEvent::Status(
                                PactorLinkStatus::Disconnected
                                | PactorLinkStatus::LinkFailure,
                            ) => {
                                return Err(ScsPactorError::Io(std::io::Error::other(
                                    g_text.into_owned(),
                                )))
                            }
                            PactorLinkEvent::Status(PactorLinkStatus::Connecting {
                                ..
                            }) => {
                                saw_link_setup = true;
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Poll every 2 seconds (matching ptc-go's status polling rate)
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
        self.send_data_frame(PACTOR_CHANNEL, data).await
    }

    async fn read_data(&self, max_len: usize) -> Result<Vec<u8>, ScsPactorError> {
        let mut rx = self.data_rx.lock().await;
        let read = rx.recv();
        let mut data = if let Some(d) = self.read_timeout {
            timeout(d, read)
                .await
                .map_err(|_| ScsPactorError::Timeout)?
                .ok_or(ScsPactorError::Disconnected)?
        } else {
            read.await.ok_or(ScsPactorError::Disconnected)?
        };
        data.truncate(max_len);
        Ok(data)
    }

    async fn disconnect(&self) -> Result<(), ScsPactorError> {
        self.send_host_command(b'D', &[]).await
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

    async fn read_next_frame<R: AsyncRead + Unpin>(
        reader: &mut R,
        decoder: &mut HostmodeDecoder,
        buf: &mut [u8],
    ) -> HostmodeFrame {
        loop {
            if let Some(frame) = decoder.next_frame().unwrap() {
                return frame;
            }
            let n = reader.read(buf).await.unwrap();
            decoder.push(&buf[..n]);
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
            let ok =
                encode_frame(&HostmodeFrame::command(PACTOR_CHANNEL, b"OK".to_vec())).unwrap();
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
            let mut decoder = HostmodeDecoder::new();
            let mut buf = [0u8; 1024];

            // Modem receives C command with counter.
            let c_frame = read_next_frame(&mut modem_side, &mut decoder, &mut buf).await;
            assert_eq!(c_frame.channel, PACTOR_CHANNEL);
            assert_eq!(c_frame.code, TYPE_COMMAND);
            assert_eq!(c_frame.payload, b"C NODE");

            // ACK the C command
            let c_ack =
                encode_frame(&HostmodeFrame::command(PACTOR_CHANNEL, b"OK".to_vec())).unwrap();
            modem_side.write_all(&c_ack).await.unwrap();

            // connect_peer sends L poll — counter toggled after C ACK.
            let l_frame = read_next_frame(&mut modem_side, &mut decoder, &mut buf).await;
            assert_eq!(l_frame.code, TYPE_COMMAND_COUNTER);
            assert!(l_frame.payload.starts_with(b"L"));

            // Respond with link_state=4 (connected)
            let l_resp = encode_frame(&HostmodeFrame::command(
                PACTOR_CHANNEL,
                b"0 0 0 0 0 4".to_vec(),
            ))
            .unwrap();
            modem_side.write_all(&l_resp).await.unwrap();
        });

        transport.connect_peer("NODE").await.unwrap();
        modem.await.unwrap();
    }

    #[tokio::test]
    async fn usb_transport_connect_peer_reports_busy_status() {
        let (transport_side, mut modem_side) = duplex(4096);
        let mut config = test_config();
        config.command_timeout = Duration::from_secs(10);
        let transport = UsbPactorTransport::from_stream(transport_side, config);

        let modem = tokio::spawn(async move {
            let mut decoder = HostmodeDecoder::new();
            let mut buf = [0u8; 1024];

            // Modem receives C command with counter.
            let c_frame = read_next_frame(&mut modem_side, &mut decoder, &mut buf).await;
            assert_eq!(c_frame.channel, PACTOR_CHANNEL);
            assert_eq!(c_frame.code, TYPE_COMMAND);
            assert_eq!(c_frame.payload, b"C NODE");

            // ACK the C command
            let c_ack =
                encode_frame(&HostmodeFrame::command(PACTOR_CHANNEL, b"OK".to_vec())).unwrap();
            modem_side.write_all(&c_ack).await.unwrap();

            // connect_peer sends L poll — counter toggled after C ACK.
            let l_frame = read_next_frame(&mut modem_side, &mut decoder, &mut buf).await;
            assert_eq!(l_frame.code, TYPE_COMMAND_COUNTER);
            assert!(l_frame.payload.starts_with(b"L"));

            // Respond with status_pending=1
            let l_resp = encode_frame(&HostmodeFrame::command(
                PACTOR_CHANNEL,
                b"1 0 0 0 0 0".to_vec(),
            ))
            .unwrap();
            modem_side.write_all(&l_resp).await.unwrap();

            // connect_peer sends G poll to read status
            let g_frame = read_next_frame(&mut modem_side, &mut decoder, &mut buf).await;
            assert_eq!(g_frame.code, TYPE_COMMAND);
            assert!(g_frame.payload.starts_with(b"G"));

            // Respond with BUSY
            let busy =
                encode_frame(&HostmodeFrame::command(PACTOR_CHANNEL, b"BUSY".to_vec())).unwrap();
            modem_side.write_all(&busy).await.unwrap();
        });

        let err = transport.connect_peer("NODE").await.unwrap_err();
        assert!(matches!(err, ScsPactorError::Busy));
        modem.await.unwrap();
    }

    #[tokio::test]
    async fn usb_transport_connect_peer_waits_through_queued_status() {
        let (transport_side, mut modem_side) = duplex(4096);
        let mut config = test_config();
        config.command_timeout = Duration::from_secs(10);
        let transport = UsbPactorTransport::from_stream(transport_side, config);

        let modem = tokio::spawn(async move {
            let mut decoder = HostmodeDecoder::new();
            let mut buf = [0u8; 1024];

            // Modem receives C command with counter.
            let c_frame = read_next_frame(&mut modem_side, &mut decoder, &mut buf).await;
            assert_eq!(c_frame.channel, PACTOR_CHANNEL);
            assert_eq!(c_frame.code, TYPE_COMMAND);
            assert_eq!(c_frame.payload, b"C NODE");

            // ACK the C command
            let c_ack =
                encode_frame(&HostmodeFrame::command(PACTOR_CHANNEL, b"OK".to_vec())).unwrap();
            modem_side.write_all(&c_ack).await.unwrap();

            // First L poll — counter toggled after C ACK.
            let l1 = read_next_frame(&mut modem_side, &mut decoder, &mut buf).await;
            assert_eq!(l1.code, TYPE_COMMAND_COUNTER);
            assert!(l1.payload.starts_with(b"L"));
            let l1_resp = encode_frame(&HostmodeFrame::command(
                PACTOR_CHANNEL,
                b"0 0 0 0 0 1".to_vec(),
            ))
            .unwrap();
            modem_side.write_all(&l1_resp).await.unwrap();

            // Second L poll — counter toggled after first L ACK.
            let l2 = read_next_frame(&mut modem_side, &mut decoder, &mut buf).await;
            assert_eq!(l2.code, TYPE_COMMAND);
            assert!(l2.payload.starts_with(b"L"));
            let l2_resp = encode_frame(&HostmodeFrame::command(
                PACTOR_CHANNEL,
                b"0 0 0 0 0 4".to_vec(),
            ))
            .unwrap();
            modem_side.write_all(&l2_resp).await.unwrap();
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
    async fn usb_transport_writes_disconnect_as_hostmode_command() {
        let (transport_side, mut modem_side) = duplex(1024);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        transport.disconnect().await.unwrap();

        let mut buf = [0u8; 1024];
        let n = modem_side.read(&mut buf).await.unwrap();
        let frame = decode_frame(&buf[..n]).unwrap();
        assert_eq!(frame.channel, PACTOR_CHANNEL);
        assert_eq!(base_code(frame.code), TYPE_COMMAND);
        assert_eq!(frame.payload, b"D");
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
    async fn usb_transport_writes_data_on_pactor_channel() {
        let (transport_side, mut modem_side) = duplex(1024);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        transport.write_data(b"hello").await.unwrap();

        let mut buf = [0u8; 1024];
        let n = modem_side.read(&mut buf).await.unwrap();
        let frame = decode_frame(&buf[..n]).unwrap();
        assert_eq!(frame.channel, PACTOR_CHANNEL);
        assert_eq!(base_code(frame.code), TYPE_DATA);
        assert_eq!(frame.payload, b"hello");
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
                    let ack = encode_frame(&HostmodeFrame::command(
                        PACTOR_CHANNEL,
                        b"OK".to_vec(),
                    ))
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
