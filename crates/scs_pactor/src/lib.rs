use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{lookup_host, TcpStream};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

#[derive(Debug, Error)]
pub enum ScsPactorError {
    #[error("socket io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("framing error: {0}")]
    Framing(#[from] tokio_util::codec::LengthDelimitedCodecError),

    #[error("operation timed out")]
    Timeout,

    #[error("message exceeds MTU ({0} bytes)")]
    ExceedsMtu(usize),

    #[error("disconnected from peer")]
    Disconnected,
}

#[derive(Clone, Debug)]
pub struct ScsPactorConfig {
    pub addr: SocketAddr,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
}

impl ScsPactorConfig {
    pub async fn resolve(host: &str, port: u16) -> Result<Self, ScsPactorError> {
        let mut addrs = lookup_host((host, port)).await?;
        let addr = addrs
            .next()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "no addresses"))?;
        Ok(Self {
            addr,
            read_timeout: Some(Duration::from_secs(10)),
            write_timeout: Some(Duration::from_secs(10)),
        })
    }
}

pub struct ScsPactorClient {
    cmd_writer: Mutex<FramedWrite<tokio::net::tcp::OwnedWriteHalf, LinesCodec>>,
    cmd_reader: Mutex<FramedRead<tokio::net::tcp::OwnedReadHalf, LinesCodec>>,
    data_writer: Mutex<tokio::net::tcp::OwnedWriteHalf>,
    data_reader: Mutex<tokio::net::tcp::OwnedReadHalf>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    peer_addr: SocketAddr,
}

impl ScsPactorClient {
    pub async fn connect(config: ScsPactorConfig) -> Result<Self, ScsPactorError> {
        let stream = TcpStream::connect(config.addr).await?;
        stream.set_nodelay(true)?;
        let peer_addr = stream.peer_addr()?;

        let (r, w) = stream.into_split();
        let cmd_reader = FramedRead::new(r, LinesCodec::new());
        let cmd_writer = FramedWrite::new(w, LinesCodec::new());

        let stream2 = TcpStream::connect(peer_addr).await?;
        stream2.set_nodelay(true)?;
        let (r2, w2) = stream2.into_split();

        Ok(Self {
            cmd_writer: Mutex::new(cmd_writer),
            cmd_reader: Mutex::new(cmd_reader),
            data_writer: Mutex::new(w2),
            data_reader: Mutex::new(r2),
            read_timeout: config.read_timeout,
            write_timeout: config.write_timeout,
            peer_addr,
        })
    }

    pub fn peer_addr(&self) -> SocketAddr { self.peer_addr }
    pub async fn send_command(&self, line: &str) -> Result<(), ScsPactorError> {
        let mut writer = self.cmd_writer.lock().await;
        let fut = writer.send(line.to_string());
        if let Some(d) = self.write_timeout {
            let res = timeout(d, fut).await.map_err(|_| ScsPactorError::Timeout)?;
            res.map_err(|e| ScsPactorError::Io(std::io::Error::other(e.to_string())))?;
        } else {
            writer
                .send(line.to_string())
                .await
                .map_err(|e| ScsPactorError::Io(std::io::Error::other(e.to_string())))?;
        }
        Ok(())
    }

    pub async fn read_status_line(&self) -> Result<String, ScsPactorError> {
        let mut reader = self.cmd_reader.lock().await;
        let next = if let Some(d) = self.read_timeout {
            timeout(d, reader.next()).await.map_err(|_| ScsPactorError::Timeout)?
        } else {
            reader.next().await
        };
        match next {
            Some(Ok(line)) => Ok(line),
            Some(Err(e)) => Err(ScsPactorError::Io(std::io::Error::other(e.to_string()))),
            None => Err(ScsPactorError::Disconnected),
        }
    }

    pub async fn set_mycall(&self, callsign: &str) -> Result<(), ScsPactorError> {
        self.send_command(&format!("MYCALL {}", callsign)).await
    }

    pub async fn connect_peer(&self, remote_call: &str) -> Result<(), ScsPactorError> {
        self.send_command(&format!("CONNECT {}", remote_call)).await?;
        for _ in 0..60 {
            let line = self.read_status_line().await?;
            if line.starts_with("DISCONNECTED") || line.starts_with("FAIL") || line.starts_with("NO ") {
                return Err(ScsPactorError::Io(std::io::Error::other(line)));
            }
            if line.starts_with("CONNECTED") {
                return Ok(());
            }
        }
        Err(ScsPactorError::Timeout)
    }

    pub async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
        let mut w = self.data_writer.lock().await;
        let fut = w.write_all(data);
        if let Some(d) = self.write_timeout {
            timeout(d, fut).await.map_err(|_| ScsPactorError::Timeout)??;
        } else {
            w.write_all(data).await?;
        }
        Ok(())
    }

    pub async fn read_data(&self, max_len: usize) -> Result<Vec<u8>, ScsPactorError> {
        let mut r = self.data_reader.lock().await;
        let mut buf = vec![0u8; max_len];
        let fut = r.read(&mut buf);
        let n = if let Some(d) = self.read_timeout {
            timeout(d, fut).await.map_err(|_| ScsPactorError::Timeout)??
        } else {
            r.read(&mut buf).await?
        };
        if n == 0 { return Err(ScsPactorError::Disconnected); }
        buf.truncate(n);
        Ok(buf)
    }

    pub async fn disconnect(&self) -> Result<(), ScsPactorError> {
        self.send_command("D").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn config_resolve_builds() {
        let _ = ScsPactorConfig::resolve("localhost", 9).await;
    }
}
 
