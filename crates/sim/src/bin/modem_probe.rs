//! Minimal serial probe for SCS PACTOR modems.
//!
//! Deep probe with FTDI latency tuning and comprehensive baud rate sweep.
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
        .map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else if b == b'\r' {
                '↵'
            } else if b == b'\n' {
                '⏎'
            } else {
                '.'
            }
        })
        .collect();
    println!(
        "  {label}: {} bytes hex={:02x?}\n         ascii=\"{ascii}\"",
        bytes.len(),
        bytes
    );
}

/// Extract the ttyUSB device number from a port path for FTDI sysfs access.
fn ftdi_device_name(port: &str) -> Option<String> {
    // Handle both /dev/ttyUSB0 and /dev/serial/by-id/... (resolve symlink)
    let resolved = std::fs::canonicalize(port).ok()?;
    let name = resolved.file_name()?.to_str()?;
    if name.starts_with("ttyUSB") {
        Some(name.to_string())
    } else {
        None
    }
}

/// Try to set the FTDI latency timer to 1ms (default is 16ms).
/// This dramatically improves small-packet delivery on FTDI USB serial.
fn set_ftdi_latency(port: &str) {
    let Some(dev) = ftdi_device_name(port) else {
        println!("  (could not resolve device name for FTDI tuning)");
        return;
    };
    let path = format!("/sys/bus/usb-serial/devices/{dev}/latency_timer");
    println!("  setting FTDI latency timer: {path} = 1");
    match std::fs::write(&path, "1") {
        Ok(()) => {
            let current = std::fs::read_to_string(&path).unwrap_or_default();
            println!("  latency_timer is now: {}", current.trim());
        }
        Err(e) => println!("  failed to set latency timer: {e} (try: sudo chmod 666 {path})"),
    }
}

/// Read current FTDI latency timer value.
fn read_ftdi_latency(port: &str) {
    let Some(dev) = ftdi_device_name(port) else {
        return;
    };
    let path = format!("/sys/bus/usb-serial/devices/{dev}/latency_timer");
    match std::fs::read_to_string(&path) {
        Ok(val) => println!("  current FTDI latency_timer: {}ms", val.trim()),
        Err(e) => println!("  could not read latency_timer: {e}"),
    }
}

async fn deep_probe(
    port: &str,
    baud: u32,
    label: &str,
) {
    println!("\n=== DEEP PROBE: {label} (baud={baud}) ===");
    let Ok(mut serial) = tokio_serial::new(port, baud)
        .data_bits(DataBits::Eight)
        .parity(Parity::None)
        .stop_bits(StopBits::One)
        .flow_control(FlowControl::None)
        .open_native_async()
    else {
        println!("  failed to open");
        return;
    };
    let _ = serial.write_data_terminal_ready(true);
    let _ = serial.write_request_to_send(true);

    // Drain initial
    let initial = read_all(&mut serial, 1000).await;
    if !initial.is_empty() {
        print_bytes("initial drain", &initial);
    }

    // Send ESC to break out of any mode
    println!("  >> ESC");
    let _ = serial.write_all(b"\x1b").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let resp = read_all(&mut serial, 2000).await;
    print_bytes("ESC", &resp);

    // Send just CR — wait much longer
    println!("  >> CR");
    let _ = serial.write_all(b"\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("CR", &resp);

    // Send another CR
    println!("  >> CR");
    let _ = serial.write_all(b"\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("CR", &resp);

    // Try MYcall with very long wait
    println!("  >> MYcall");
    let _ = serial.write_all(b"MYcall\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("MYcall", &resp);

    // Try VER
    println!("  >> VER");
    let _ = serial.write_all(b"VER\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("VER", &resp);

    // Try sending characters one at a time with delays between
    // This helps with USB bulk transfer buffering
    println!("  >> M-Y-c-a-l-l (char by char)");
    for ch in b"MYcall\r" {
        let _ = serial.write_all(&[*ch]).await;
        let _ = serial.flush().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("MYcall (slow)", &resp);

    // JHOST4 entry attempt
    println!("  >> JHOST4");
    let _ = serial.write_all(b"JHOST4\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("JHOST4", &resp);

    // Post-JHOST4: CRC hostmode L command on ch31
    println!("  >> hostmode L ch31 [aa aa 1f 01 00 4c 32 5f]");
    let _ = serial.write_all(&[0xaa, 0xaa, 0x1f, 0x01, 0x00, 0x4c, 0x32, 0x5f]).await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("hostmode L", &resp);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("Probing modem on {} ...\n", args.port);

    // Show and tune FTDI latency timer
    read_ftdi_latency(&args.port);
    set_ftdi_latency(&args.port);

    // Main probe at specified baud
    deep_probe(&args.port, args.baud, "8N1").await;

    // Try 115200 (SCS Dragon default)
    deep_probe(&args.port, 115_200, "8N1 @115200").await;

    // Try 9600 (universal fallback)
    deep_probe(&args.port, 9600, "8N1 @9600").await;

    println!("\nDone.");
    Ok(())
}
