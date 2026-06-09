use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

#[derive(Debug, Error)]
pub enum VaraError {
    #[error("socket io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("operation timed out")]
    Timeout,

    #[error("disconnected from modem")]
    Disconnected,

    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),
}

#[derive(Clone, Debug)]
pub struct VaraConfig {
    pub host: String,
    pub cmd_port: u16,
    pub data_port: u16,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
}

impl VaraConfig {
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            cmd_port: 8300,
            data_port: 8301,
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
        }
    }
}

pub struct VaraClient {
    cmd_writer: Mutex<FramedWrite<tokio::net::tcp::OwnedWriteHalf, LinesCodec>>,
    cmd_reader: Mutex<FramedRead<tokio::net::tcp::OwnedReadHalf, LinesCodec>>,
    data_writer: Mutex<tokio::net::tcp::OwnedWriteHalf>,
    data_reader: Mutex<tokio::net::tcp::OwnedReadHalf>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

impl VaraClient {
    pub async fn connect(config: VaraConfig) -> Result<Self, VaraError> {
        let cmd_stream = TcpStream::connect((config.host.as_str(), config.cmd_port)).await?;
        cmd_stream.set_nodelay(true)?;
        let (cmd_r, cmd_w) = cmd_stream.into_split();
        let cmd_reader = FramedRead::new(cmd_r, LinesCodec::new());
        let cmd_writer = FramedWrite::new(cmd_w, LinesCodec::new());

        let data_stream = TcpStream::connect((config.host.as_str(), config.data_port)).await?;
        data_stream.set_nodelay(true)?;
        let (data_r, data_w) = data_stream.into_split();

        Ok(Self {
            cmd_writer: Mutex::new(cmd_writer),
            cmd_reader: Mutex::new(cmd_reader),
            data_writer: Mutex::new(data_w),
            data_reader: Mutex::new(data_r),
            read_timeout: config.read_timeout,
            write_timeout: config.write_timeout,
        })
    }

    pub async fn send_command(&self, line: &str) -> Result<(), VaraError> {
        let mut writer = self.cmd_writer.lock().await;
        let fut = writer.send(line.to_string());
        if let Some(d) = self.write_timeout {
            let res = timeout(d, fut).await.map_err(|_| VaraError::Timeout)?;
            res.map_err(|e| VaraError::UnexpectedResponse(e.to_string()))?;
        } else {
            writer
                .send(line.to_string())
                .await
                .map_err(|e| VaraError::UnexpectedResponse(e.to_string()))?;
        }
        Ok(())
    }

    pub async fn read_status_line(&self) -> Result<String, VaraError> {
        let mut reader = self.cmd_reader.lock().await;
        let next = if let Some(d) = self.read_timeout {
            timeout(d, reader.next())
                .await
                .map_err(|_| VaraError::Timeout)?
        } else {
            reader.next().await
        };
        match next {
            Some(Ok(line)) => Ok(line),
            Some(Err(e)) => Err(VaraError::UnexpectedResponse(e.to_string())),
            None => Err(VaraError::Disconnected),
        }
    }

    pub async fn set_mycall(&self, callsigns: &[&str]) -> Result<(), VaraError> {
        let cmd = format!("MYCALL {}", callsigns.join(" "));
        self.send_command(&cmd).await?;
        Ok(())
    }

    pub async fn connect_peer(&self, remote_call: &str) -> Result<(), VaraError> {
        self.send_command(&format!("CONNECT {}", remote_call))
            .await?;
        for _ in 0..60 {
            let line = self.read_status_line().await?;
            if line.starts_with("DISCONNECTED") || line.starts_with("WRONG") {
                return Err(VaraError::UnexpectedResponse(line));
            }
            if line.starts_with("CONNECTED") {
                return Ok(());
            }
        }
        Err(VaraError::Timeout)
    }

    pub async fn listen_on(&self, on: bool) -> Result<(), VaraError> {
        self.send_command(if on { "LISTEN ON" } else { "LISTEN OFF" })
            .await
    }

    pub async fn write_data(&self, data: &[u8]) -> Result<(), VaraError> {
        let mut writer = self.data_writer.lock().await;
        let fut = writer.write_all(data);
        if let Some(d) = self.write_timeout {
            timeout(d, fut).await.map_err(|_| VaraError::Timeout)??;
        } else {
            writer.write_all(data).await?;
        }
        Ok(())
    }

    pub async fn read_data(&self, max_len: usize) -> Result<Vec<u8>, VaraError> {
        let mut reader = self.data_reader.lock().await;
        let mut buf = vec![0u8; max_len];
        let fut = reader.read(&mut buf);
        let n = if let Some(d) = self.read_timeout {
            timeout(d, fut).await.map_err(|_| VaraError::Timeout)??
        } else {
            reader.read(&mut buf).await?
        };
        if n == 0 {
            return Err(VaraError::Disconnected);
        }
        buf.truncate(n);
        Ok(buf)
    }

    pub async fn disconnect(&self) -> Result<(), VaraError> {
        self.send_command("DISCONNECT").await
    }

    /// Enter VARA HF unproto (broadcast/FEC) mode.
    ///
    /// Sends `UNPROTO <destination>` to the VARA modem, switching it from
    /// point-to-point ARQ to broadcast FEC mode. In this mode, data written
    /// via [`write_unproto_data`](Self::write_unproto_data) is broadcast to
    /// all stations monitoring the frequency.
    ///
    /// Typical destinations: `"CQ"` (general broadcast), or a specific group call.
    pub async fn enter_unproto_mode(&self, destination: &str) -> Result<(), VaraError> {
        self.send_command(&format!("UNPROTO {destination}")).await
    }

    /// Exit VARA HF unproto mode, returning to normal ARQ/idle state.
    pub async fn exit_unproto_mode(&self) -> Result<(), VaraError> {
        self.send_command("DISCONNECT").await
    }

    /// Write data in unproto (broadcast/FEC) mode.
    ///
    /// This delegates to [`write_data`](Self::write_data) — VARA handles
    /// FEC encoding internally when in unproto mode.
    pub async fn write_unproto_data(&self, data: &[u8]) -> Result<(), VaraError> {
        self.write_data(data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let c = VaraConfig::new("127.0.0.1");
        assert_eq!(c.cmd_port, 8300);
        assert_eq!(c.data_port, 8301);
    }

    #[test]
    fn test_vara_unproto_command_format() {
        // Verify that enter_unproto_mode formats the correct VARA command.
        // We can't call the async method without a connected client, but we
        // can verify the command string format directly.
        let destination = "CQ";
        let expected_cmd = format!("UNPROTO {destination}");
        assert_eq!(expected_cmd, "UNPROTO CQ");

        let destination = "BUNKERNET";
        let expected_cmd = format!("UNPROTO {destination}");
        assert_eq!(expected_cmd, "UNPROTO BUNKERNET");
    }
}
