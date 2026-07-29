//! Shared DR-7400 modem bring-up for hardware PACTOR binaries.
//! Uses terminal/converse mode because this firmware exits JHOST hostmode on connect.

use std::time::{Duration, Instant};

use scs_pactor::hostmode::{HostmodeDecoder, HostmodeFrame, HostmodePacket, PACTOR_CHANNEL};
use scs_pactor::{PactorTransport, UsbPactorConfig, UsbPactorTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_serial::{DataBits, FlowControl, Parity, SerialPort, SerialPortBuilderExt, StopBits};

/// Configuration for [`init_modem`].
#[derive(Clone, Debug)]
pub struct PactorInitConfig {
    /// Serial device path (e.g. `/dev/serial/by-id/...`).
    pub port: String,
    /// Serial baud rate (DR-7400 default is 829440).
    pub baud: u32,
    /// This modem's callsign (`MYcall`).
    pub callsign: String,
    /// Hostmode command timeout.
    pub command_timeout: Duration,
    /// Optional TRX CI-V tune frequency (kHz). `None` skips radio tuning.
    pub frequency: Option<f64>,
    /// TRX baud override (must be paired with `trx_addr`).
    pub trx_baud: Option<u32>,
    /// TRX CI-V address override (hex, without `$`; paired with `trx_baud`).
    pub trx_addr: Option<String>,
    /// Enable `LISTEN 1` so this modem accepts an incoming connect (answerer).
    pub listen: bool,
    /// Force-disconnect any stale link before init (ESC + `DD`), preserving config.
    pub reset: bool,
}

impl PactorInitConfig {
    /// Sensible defaults for a given port + callsign.
    pub fn new(port: impl Into<String>, callsign: impl Into<String>) -> Self {
        Self {
            port: port.into(),
            baud: 829_440,
            callsign: callsign.into(),
            command_timeout: Duration::from_secs(90),
            frequency: None,
            trx_baud: None,
            trx_addr: None,
            listen: false,
            reset: false,
        }
    }
}

/// Open a serial port with the DR-7400's 8N1 / no-flow settings.
fn open_serial(port: &str, baud: u32) -> anyhow::Result<tokio_serial::SerialStream> {
    let mut serial = tokio_serial::new(port, baud)
        .data_bits(DataBits::Eight)
        .parity(Parity::None)
        .stop_bits(StopBits::One)
        .flow_control(FlowControl::None)
        .open_native_async()
        .map_err(|e| anyhow::anyhow!("failed to open {port}: {e}"))?;
    let _ = serial.write_data_terminal_ready(true);
    let _ = serial.write_request_to_send(true);
    Ok(serial)
}

/// Drain and discard whatever the modem currently has buffered.
async fn drain_serial(serial: &mut tokio_serial::SerialStream) {
    let mut buf = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_millis(500), serial.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => {}
            _ => break,
        }
    }
}

/// Read for up to `timeout_ms`, collecting everything the modem sends.
async fn read_all(serial: &mut tokio_serial::SerialStream, timeout_ms: u64) -> Vec<u8> {
    let mut all = Vec::new();
    let mut buf = [0u8; 1024];
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, serial.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => all.extend_from_slice(&buf[..n]),
            _ => break,
        }
    }
    all
}

/// Send an ASCII terminal-mode command and read the response.
async fn send_ascii(serial: &mut tokio_serial::SerialStream, cmd: &str) -> anyhow::Result<Vec<u8>> {
    serial.write_all(cmd.as_bytes()).await?;
    serial.write_all(b"\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    Ok(read_all(serial, 800).await)
}

/// Decode any hostmode frames from raw bytes (best-effort).
fn try_decode_hostmode(bytes: &[u8]) -> Vec<HostmodePacket> {
    let mut decoder = HostmodeDecoder::new();
    decoder.push(bytes);
    let mut packets = Vec::new();
    while let Ok(Some(pkt)) = decoder.next_packet() {
        packets.push(pkt);
    }
    packets
}

/// Verify hostmode is active by polling status (`L`) over the transport.
async fn verify_hostmode(transport: &UsbPactorTransport) -> bool {
    let status = HostmodeFrame::command(PACTOR_CHANNEL, b"L".to_vec());
    matches!(
        tokio::time::timeout(
            Duration::from_secs(3),
            transport.hostmode_transaction(status)
        )
        .await,
        Ok(Ok(_))
    )
}

/// Bring a modem to verified JHOST4 CRC hostmode.
/// Terminal/converse setup is required because this firmware exits JHOST on connect.
pub async fn init_modem(cfg: &PactorInitConfig) -> anyhow::Result<UsbPactorTransport> {
    let port = cfg.port.as_str();
    let mut serial = open_serial(port, cfg.baud)?;

    drain_serial(&mut serial).await;

    // ESC returns a stale converse session to terminal command mode.
    serial.write_all(&[0x1b]).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain_serial(&mut serial).await;
    serial.write_all(b"JHOST0\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    drain_serial(&mut serial).await;

    // DD clears stale link state without wiping LISTEN/MYcall on this firmware.
    if cfg.reset {
        serial.write_all(&[0x1b]).await?;
        serial.flush().await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        drain_serial(&mut serial).await;
        send_ascii(&mut serial, "DD").await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        drain_serial(&mut serial).await;
    }

    // Terminal-mode config mirrors ptc-go's DR-7400 bring-up.
    send_ascii(&mut serial, "").await?;
    send_ascii(&mut serial, "Quit").await?;
    let commands = [
        format!("MYcall {}", cfg.callsign),
        format!("PTCH {PACTOR_CHANNEL}"),
        "MAXE 35".to_owned(),
        "REM 0".to_owned(),
        "CHOB 0".to_owned(),
        // Ctrl-Z lets the ISS hand the transmit turn to the peer in converse mode.
        "CHO 26".to_owned(),
        "TONES 4".to_owned(),
        "MARK 1600".to_owned(),
        "SPACE 1400".to_owned(),
        "CWID 0".to_owned(),
        "CONType 3".to_owned(),
        "MODE 0".to_owned(),
    ];
    for command in &commands {
        send_ascii(&mut serial, command).await?;
    }

    if cfg.listen {
        send_ascii(&mut serial, "LISTEN 1").await?;
    }

    if let Some(frequency) = cfg.frequency {
        send_ascii(&mut serial, "TRX TYpe").await?;
        match (cfg.trx_baud, cfg.trx_addr.as_deref()) {
            (Some(baud), Some(addr)) => {
                send_ascii(&mut serial, &format!("TRX TYpe I {baud} ${addr}")).await?;
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(anyhow::anyhow!(
                    "TRX override on {port}: --trx-baud and --trx-addr must be given together"
                ));
            }
            (None, None) => {}
        }
        let resp = send_ascii(&mut serial, &format!("TRX Frequency {frequency}")).await?;
        let resp = String::from_utf8_lossy(&resp);
        if !resp.contains("FREQUENCY CHANGED") {
            return Err(anyhow::anyhow!(
                "TRX Frequency set failed on {port} — radio did not confirm tune \
                 (CI-V cable connected? radio powered on?). response: {resp}"
            ));
        }
    }

    send_ascii(&mut serial, "JHOST4").await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let _ = try_decode_hostmode(&read_all(&mut serial, 2000).await);

    let mut config = UsbPactorConfig::new(port);
    config.command_timeout = cfg.command_timeout;
    // Marginal HF ARQ can take minutes after changeover.
    config.read_timeout = Some(Duration::from_secs(180));
    let transport = UsbPactorTransport::from_stream(serial, config);

    for _ in 0..5 {
        if verify_hostmode(&transport).await {
            return Ok(transport);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(anyhow::anyhow!(
        "failed to enter hostmode on {port} — check modem power and baud rate"
    ))
}

/// Re-open a modem after STBY without the full drain/config/JHOST4 sequence.
/// Assumes stored MYcall/PTCH/CHO/tones/TRX config from a prior [`init_modem`].
pub async fn light_init_modem(cfg: &PactorInitConfig) -> anyhow::Result<UsbPactorTransport> {
    let port = cfg.port.as_str();
    let mut serial = open_serial(port, cfg.baud)?;

    // Return to terminal command mode without wiping persisted modem config.
    serial.write_all(&[0x1b]).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain_serial(&mut serial).await;
    send_ascii(&mut serial, "DD").await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain_serial(&mut serial).await;

    if cfg.listen {
        send_ascii(&mut serial, "LISTEN 1").await?;
    }

    let mut config = UsbPactorConfig::new(port);
    config.command_timeout = cfg.command_timeout;
    config.read_timeout = Some(Duration::from_secs(180));
    Ok(UsbPactorTransport::from_stream(serial, config))
}

/// Establish the PACTOR link from the caller side, retrying after brief settle delays.
pub async fn connect_with_retries(
    transport: &UsbPactorTransport,
    remote_call: &str,
    attempts: u32,
) -> anyhow::Result<()> {
    let mut last_err = None;
    for attempt in 1..=attempts {
        match transport.connect_peer(remote_call).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                eprintln!("  connect attempt {attempt}/{attempts} failed: {e}");
                last_err = Some(e);
                if attempt < attempts {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
    Err(anyhow::anyhow!(
        "connect to {remote_call} failed after {attempts} attempts: {}",
        last_err
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    ))
}
