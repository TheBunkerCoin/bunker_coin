//! Test TRX CI-V frequency control on an SCS PACTOR modem.
//!
//! ```text
//! cargo run -p bunker_coin_sim --bin trx_test -- \
//!   --port /dev/ttyUSB0 --frequency 14079.0
//! ```
//!
//! This initializes the modem into terminal mode, queries the TRX CI-V
//! configuration, and attempts to tune the radio to the given frequency.

use std::time::Duration;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_serial::{DataBits, FlowControl, Parity, SerialPort, SerialPortBuilderExt, StopBits};

#[derive(Parser)]
#[command(name = "trx_test")]
#[command(about = "Test TRX CI-V frequency control on an SCS PACTOR modem")]
struct Args {
    /// Serial port for the modem
    #[arg(long)]
    port: String,

    /// Frequency in kHz (e.g. 14079.0)
    #[arg(long)]
    frequency: f64,

    /// Baud rate for serial port (SCS Dragon 7400 uses 829440)
    #[arg(long, default_value_t = 829_440)]
    baud: u32,
}

async fn read_response(serial: &mut tokio_serial::SerialStream, timeout_ms: u64) -> Vec<u8> {
    let mut all = Vec::new();
    let mut buf = [0u8; 1024];
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
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

async fn send_cmd(serial: &mut tokio_serial::SerialStream, cmd: &str) -> anyhow::Result<String> {
    let label = if cmd.is_empty() { "CR" } else { cmd };
    println!("  >> {label}");
    serial.write_all(cmd.as_bytes()).await?;
    serial.write_all(b"\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let resp = read_response(serial, 800).await;
    let text = String::from_utf8_lossy(&resp).to_string();
    if !text.is_empty() {
        for line in text.lines() {
            println!("     {line}");
        }
    } else {
        println!("     (no response)");
    }
    Ok(text)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("=== TRX Frequency Test ===");
    println!("Port:      {}", args.port);
    println!("Baud:      {}", args.baud);
    println!("Frequency: {} kHz", args.frequency);
    println!();

    // Open serial port
    let mut serial = tokio_serial::new(&args.port, args.baud)
        .data_bits(DataBits::Eight)
        .parity(Parity::None)
        .stop_bits(StopBits::One)
        .flow_control(FlowControl::None)
        .open_native_async()
        .map_err(|e| anyhow::anyhow!("Failed to open {}: {e}", args.port))?;
    let _ = serial.write_data_terminal_ready(true);
    let _ = serial.write_request_to_send(true);
    println!("Serial port opened.\n");

    // Drain any buffered data
    println!("--- Draining buffer ---");
    read_response(&mut serial, 500).await;

    // Exit any existing hostmode
    println!("--- Exiting any hostmode ---");
    serial.write_all(b"JHOST0\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    read_response(&mut serial, 500).await;

    // Sync terminal
    println!("\n--- Terminal sync ---");
    send_cmd(&mut serial, "").await?;

    // Check modem responds
    println!("\n--- Modem identity ---");
    send_cmd(&mut serial, "MYcall").await?;

    // Query TRX CI-V config
    println!("\n--- TRX CI-V configuration ---");
    let trx_config = send_cmd(&mut serial, "TRX TYpe").await?;
    if trx_config.trim().is_empty() || trx_config.contains("no response") {
        println!("\n  WARNING: No TRX config returned. CI-V may not be set up.");
    }

    // Attempt frequency change
    println!("\n--- Frequency change: {} kHz ---", args.frequency);
    let freq_resp = send_cmd(&mut serial, &format!("TRX Frequency {}", args.frequency)).await?;

    // Give the radio a bit more time to respond
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let extra = read_response(&mut serial, 1000).await;
    let extra_text = String::from_utf8_lossy(&extra);
    if !extra_text.is_empty() {
        for line in extra_text.lines() {
            println!("     {line}");
        }
    }

    let combined = format!("{freq_resp}{extra_text}");

    println!();
    if combined.contains("FREQUENCY CHANGED") {
        println!("=== RESULT: PASS — TRX frequency changed successfully ===");
    } else {
        println!("=== RESULT: FAIL — Radio did not confirm frequency change ===");
        println!();
        println!("Possible causes:");
        println!("  - CI-V cable not connected between modem TRX port and radio");
        println!("  - Radio powered off");
        println!("  - Wrong CI-V baud or address (see TRX TYpe output above)");
        println!("  - Radio uses CAT instead of CI-V (Yaesu/Kenwood)");
    }

    Ok(())
}
