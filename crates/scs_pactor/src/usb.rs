use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::hostmode::{encode_frame, HostmodeDecoder, HostmodeFrame, HostmodePacket};
use crate::{PactorLinkEvent, PactorLinkStatus, PactorTransport, ScsPactorError};

const COMMAND_CHANNEL: u8 = 0;
const DATA_CHANNEL: u8 = 1;
const STATUS_CHANNEL: u8 = 254;
const EXTENDED_POLL_CHANNEL: u8 = 255;
const MAX_HOSTMODE_RETRIES: u8 = 3;

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
            baud_rate: 115_200,
            read_timeout: Some(Duration::from_secs(10)),
            write_timeout: Some(Duration::from_secs(10)),
            command_timeout: Duration::from_secs(10),
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
    read_task: JoinHandle<()>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    command_timeout: Duration,
}

impl UsbPactorTransport {
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

            loop {
                let n = match reader.read(&mut buf).await {
                    Ok(0) => {
                        let _ = event_tx
                            .send(PactorLinkEvent::Status(PactorLinkStatus::Disconnected))
                            .await;
                        break;
                    }
                    Ok(n) => n,
                    Err(_) => {
                        let _ = event_tx
                            .send(PactorLinkEvent::Status(PactorLinkStatus::LinkFailure))
                            .await;
                        break;
                    }
                };

                decoder.push(&buf[..n]);
                loop {
                    match decoder.next_packet() {
                        Ok(Some(HostmodePacket::Frame(frame))) => {
                            let _ = packet_tx.send(HostmodePacket::Frame(frame.clone())).await;
                            if route_frame(frame, &command_tx, &data_tx, &event_tx)
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Ok(Some(HostmodePacket::RepeatRequest)) => {
                            let _ = packet_tx.send(HostmodePacket::RepeatRequest).await;
                        }
                        Ok(None) => break,
                        Err(_) => {
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
            read_task,
            read_timeout: config.read_timeout,
            write_timeout: config.write_timeout,
            command_timeout: config.command_timeout,
        }
    }

    async fn send_hostmode_frame(&self, frame: HostmodeFrame) -> Result<(), ScsPactorError> {
        let encoded = encode_frame(&frame)?;
        self.write_encoded_frame(&encoded).await
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
        let encoded = encode_frame(&frame)?;
        let mut retries = 0;

        loop {
            self.write_encoded_frame(&encoded).await?;
            match self.recv_hostmode_packet(self.command_timeout).await? {
                HostmodePacket::Frame(response) => return Ok(response),
                HostmodePacket::RepeatRequest => {
                    retries += 1;
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

    pub async fn poll_channel(&self, channel: u8) -> Result<HostmodeFrame, ScsPactorError> {
        self.hostmode_transaction(HostmodeFrame::with_code(channel, b'G', vec![0]))
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

    async fn send_frame(&self, channel: u8, payload: &[u8]) -> Result<(), ScsPactorError> {
        self.send_hostmode_frame(HostmodeFrame::new(channel, payload.to_vec()))
            .await
    }

    async fn send_host_command(&self, code: u8, payload: &[u8]) -> Result<(), ScsPactorError> {
        let payload = if payload.is_empty() {
            vec![0]
        } else {
            payload.to_vec()
        };
        self.send_hostmode_frame(HostmodeFrame::with_code(COMMAND_CHANNEL, code, payload))
            .await
    }

    pub async fn send_command(&self, line: &str) -> Result<(), ScsPactorError> {
        let trimmed = line.trim();
        let mut parts = trimmed.splitn(2, char::is_whitespace);
        let command = parts.next().unwrap_or_default();
        let payload = parts.next().unwrap_or_default().trim_start();
        let code = match command {
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
        self.send_host_command(code, payload.as_bytes()).await
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
        COMMAND_CHANNEL => {
            let line = String::from_utf8(frame.payload)
                .map_err(|e| ScsPactorError::Protocol(e.to_string()))?;
            if let Ok(event) = UsbPactorTransport::parse_status_line(&line) {
                let _ = event_tx.send(event).await;
            }
            command_tx
                .send(line)
                .await
                .map_err(|_| ScsPactorError::Disconnected)?;
        }
        DATA_CHANNEL => {
            data_tx
                .send(frame.payload)
                .await
                .map_err(|_| ScsPactorError::Disconnected)?;
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
        other => {
            return Err(ScsPactorError::Protocol(format!(
                "unknown hostmode channel {other}"
            )));
        }
    }
    Ok(())
}

#[async_trait]
impl PactorTransport for UsbPactorTransport {
    async fn set_mycall(&self, callsign: &str) -> Result<(), ScsPactorError> {
        self.send_host_command(b'I', callsign.as_bytes()).await
    }

    async fn connect_peer(&self, remote_call: &str) -> Result<(), ScsPactorError> {
        self.send_host_command(b'C', remote_call.as_bytes()).await?;
        loop {
            let line = timeout(self.command_timeout, self.read_status_line())
                .await
                .map_err(|_| ScsPactorError::Timeout)??;
            match Self::parse_status_line(&line)? {
                PactorLinkEvent::Status(PactorLinkStatus::Connected { .. }) => return Ok(()),
                PactorLinkEvent::Status(PactorLinkStatus::Busy) => {
                    return Err(ScsPactorError::Busy)
                }
                PactorLinkEvent::Status(
                    PactorLinkStatus::Disconnected | PactorLinkStatus::LinkFailure,
                ) => return Err(ScsPactorError::Io(std::io::Error::other(line))),
                _ => {}
            }
        }
    }

    async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
        self.send_frame(DATA_CHANNEL, data).await
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
    use tokio::io::{duplex, AsyncReadExt};

    use super::*;
    use crate::hostmode::{decode_frame, encode_repeat_request};

    fn test_config() -> UsbPactorConfig {
        UsbPactorConfig {
            port: "mock".to_owned(),
            baud_rate: 115_200,
            read_timeout: Some(Duration::from_millis(100)),
            write_timeout: Some(Duration::from_millis(100)),
            command_timeout: Duration::from_millis(100),
        }
    }

    #[tokio::test]
    async fn usb_transport_writes_commands_as_hostmode_frames() {
        let (transport_side, mut modem_side) = duplex(1024);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        transport.set_mycall("N0CALL").await.unwrap();

        let mut buf = [0u8; 1024];
        let n = modem_side.read(&mut buf).await.unwrap();
        let frame = decode_frame(&buf[..n]).unwrap();
        assert_eq!(frame.channel, COMMAND_CHANNEL);
        assert_eq!(frame.code, b'I');
        assert_eq!(frame.payload, b"N0CALL");
    }

    #[tokio::test]
    async fn usb_transport_demuxes_command_events_and_data() {
        let (transport_side, mut modem_side) = duplex(2048);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        let status = encode_frame(&HostmodeFrame::new(
            COMMAND_CHANNEL,
            b"CONNECTED NODE".to_vec(),
        ))
        .unwrap();
        let data = encode_frame(&HostmodeFrame::new(DATA_CHANNEL, b"payload".to_vec())).unwrap();

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
        let (transport_side, mut modem_side) = duplex(2048);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        let modem = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = modem_side.read(&mut buf).await.unwrap();
            let frame = decode_frame(&buf[..n]).unwrap();
            assert_eq!(frame.channel, COMMAND_CHANNEL);
            assert_eq!(frame.code, b'C');
            assert_eq!(frame.payload, b"NODE");

            let connected = encode_frame(&HostmodeFrame::new(
                COMMAND_CHANNEL,
                b"CONNECTED NODE".to_vec(),
            ))
            .unwrap();
            modem_side.write_all(&connected).await.unwrap();
        });

        transport.connect_peer("NODE").await.unwrap();
        modem.await.unwrap();
    }

    #[tokio::test]
    async fn usb_transport_writes_disconnect_as_hostmode_command() {
        let (transport_side, mut modem_side) = duplex(1024);
        let transport = UsbPactorTransport::from_stream(transport_side, test_config());

        transport.disconnect().await.unwrap();

        let mut buf = [0u8; 1024];
        let n = modem_side.read(&mut buf).await.unwrap();
        let frame = decode_frame(&buf[..n]).unwrap();
        assert_eq!(frame.channel, COMMAND_CHANNEL);
        assert_eq!(frame.code, b'D');
        assert_eq!(frame.payload, vec![0]);
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
            assert_eq!(frame.channel, COMMAND_CHANNEL);
            assert_eq!(frame.code, b'V');

            modem_side
                .write_all(&encode_repeat_request())
                .await
                .unwrap();

            let n = modem_side.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], first.as_slice());

            let response =
                encode_frame(&HostmodeFrame::new(COMMAND_CHANNEL, b"OK".to_vec())).unwrap();
            modem_side.write_all(&response).await.unwrap();
        });

        let response = transport
            .hostmode_transaction(HostmodeFrame::with_code(COMMAND_CHANNEL, b'V', vec![0]))
            .await
            .unwrap();
        assert_eq!(response.channel, COMMAND_CHANNEL);
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
            assert_eq!(frame.code, b'G');
            assert_eq!(frame.payload, vec![0]);

            let response = encode_frame(&HostmodeFrame::with_code(
                EXTENDED_POLL_CHANNEL,
                1,
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
            assert_eq!(frame.code, b'G');

            let response =
                encode_frame(&HostmodeFrame::with_code(STATUS_CHANNEL, 7, vec![1, 3, 5])).unwrap();
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
}
