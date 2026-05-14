//! Minimal serial probe for SCS PACTOR modems.
//!
//! Tries different framing configurations and sends simple commands to
//! diagnose communication issues.
//!
//! ```text
//! cargo run --bin modem_probe -- --port /dev/ttyUSB0
//! ```

use std::time::Duration;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_serial::{DataBits, FlowControl, Parity, SerialPort, SerialPortBuilderExt, StopBits};

#[derive(Parser)]
#[command(name = "modem_probe")]
struct Args {
    #[arg(long)]
    port: String,

    #[arg(long, default_value_t = 230_400)]
    baud: u32,
}

async fn read_all(serial: &mut tokio_serial::SerialStream, timeout_ms: u64) -> Vec<u8> {
    let mut all = Vec::new();
    let mut buf = [0u8; 4096];
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

fn print_bytes(label: &str, bytes: &[u8]) {
    if bytes.is_empty() {
        println!("  {label}: (no response)");
        return;
    }
    let ascii: String = bytes
        .iter()
        .map(|&b| if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' })
        .collect();
    println!(
        "  {label}: {} bytes hex={:02x?} ascii=\"{ascii}\"",
        bytes.len(),
        bytes
    );
    // Also show as binary for framing analysis
    if bytes.len() <= 8 {
        for &b in bytes {
            println!("    0x{b:02x} = 0b{b:08b}");
        }
    }
}

async fn probe_config(
    port: &str,
    baud: u32,
    data_bits: DataBits,
    parity: Parity,
    stop_bits: StopBits,
    label: &str,
) {
    println!("\n--- {label} (baud={baud}) ---");
    let Ok(mut serial) = tokio_serial::new(port, baud)
        .data_bits(data_bits)
        .parity(parity)
        .stop_bits(stop_bits)
        .flow_control(FlowControl::None)
        .open_native_async()
    else {
        println!("  failed to open");
        return;
    };
    let _ = serial.write_data_terminal_ready(true);
    let _ = serial.write_request_to_send(true);

    // Drain initial
    let initial = read_all(&mut serial, 500).await;
    if !initial.is_empty() {
        print_bytes("initial drain", &initial);
    }

    // Test 1: Send just CR
    let _ = serial.write_all(b"\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let resp = read_all(&mut serial, 1000).await;
    print_bytes("CR only", &resp);

    // Test 2: Send ESC
    let _ = serial.write_all(b"\x1b").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let resp = read_all(&mut serial, 1000).await;
    print_bytes("ESC", &resp);

    // Test 3: Send "MYcall\r"
    let _ = serial.write_all(b"MYcall\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 1000).await;
    print_bytes("MYcall", &resp);

    // Test 4: Send just "V\r" (version query, short command)
    let _ = serial.write_all(b"V\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 1000).await;
    print_bytes("V (version)", &resp);

    // Test 5: Send "JHOST0\r"
    let _ = serial.write_all(b"JHOST0\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 1000).await;
    print_bytes("JHOST0", &resp);

    // Test 6: Send just a single byte 'A' + CR
    let _ = serial.write_all(b"A\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 1000).await;
    print_bytes("A (single char)", &resp);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("Probing modem on {} ...", args.port);

    // Try the standard 8N1 at the specified baud
    probe_config(
        &args.port,
        args.baud,
        DataBits::Eight,
        Parity::None,
        StopBits::One,
        "8N1",
    )
    .await;

    // Try 8N1 at 115200 (common fallback)
    if args.baud != 115_200 {
        probe_config(
            &args.port,
            115_200,
            DataBits::Eight,
            Parity::None,
            StopBits::One,
            "8N1 @115200",
        )
        .await;
    }

    // Try 7E1 (some older modems)
    probe_config(
        &args.port,
        args.baud,
        DataBits::Seven,
        Parity::Even,
        StopBits::One,
        "7E1",
    )
    .await;

    // Try 8E1
    probe_config(
        &args.port,
        args.baud,
        DataBits::Eight,
        Parity::Even,
        StopBits::One,
        "8E1",
    )
    .await;

    // Try 8N2 (two stop bits)
    probe_config(
        &args.port,
        args.baud,
        DataBits::Eight,
        Parity::None,
        StopBits::Two,
        "8N2",
    )
    .await;

    println!("\nDone.");
    Ok(())
}
