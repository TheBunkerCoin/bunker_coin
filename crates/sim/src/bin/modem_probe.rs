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

async fn send_and_read(
    serial: &mut tokio_serial::SerialStream,
    cmd: &str,
    label: &str,
    wait_ms: u64,
    read_ms: u64,
) -> Vec<u8> {
    let _ = serial.write_all(cmd.as_bytes()).await;
    let _ = serial.write_all(b"\r").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    let resp = read_all(serial, read_ms).await;
    print_bytes(label, &resp);
    resp
}

async fn deep_probe(
    port: &str,
    baud: u32,
    data_bits: DataBits,
    parity: Parity,
    stop_bits: StopBits,
    label: &str,
) {
    println!("\n=== DEEP PROBE: {label} (baud={baud}) ===");
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
    let initial = read_all(&mut serial, 1000).await;
    if !initial.is_empty() {
        print_bytes("initial drain", &initial);
    }

    // Send ESC to break out of any mode
    let _ = serial.write_all(b"\x1b").await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 1000).await;
    print_bytes("ESC", &resp);

    // Send CR to get a prompt
    send_and_read(&mut serial, "", "CR (empty)", 500, 1500).await;

    // Send CR again
    send_and_read(&mut serial, "", "CR again", 500, 1500).await;

    // Send Quit to get to main menu
    send_and_read(&mut serial, "Quit", "Quit", 500, 1500).await;

    // Version query
    send_and_read(&mut serial, "VER", "VER (version)", 500, 2000).await;

    // MYcall query (no argument = query current)
    send_and_read(&mut serial, "MYcall", "MYcall (query)", 500, 2000).await;

    // Status query
    send_and_read(&mut serial, "STATUS", "STATUS", 500, 2000).await;

    // PTCH query
    send_and_read(&mut serial, "PTCH", "PTCH (query)", 500, 2000).await;

    // JHOST0 to ensure we're in terminal mode
    send_and_read(&mut serial, "JHOST0", "JHOST0", 500, 2000).await;

    // Try entering JHOST4
    println!("\n  --- Attempting JHOST4 entry ---");
    send_and_read(&mut serial, "JHOST4", "JHOST4", 1000, 3000).await;

    // After JHOST4, try sending a CRC hostmode frame and read response
    // with this same framing config
    println!("\n  --- Post-JHOST4: trying CRC hostmode frame ---");
    // L command on channel 31: AA AA 1F 01 00 4C 32 5F
    let hostmode_frame: &[u8] = &[0xaa, 0xaa, 0x1f, 0x01, 0x00, 0x4c, 0x32, 0x5f];
    println!("  >> hostmode L ch31: {:02x?}", hostmode_frame);
    let _ = serial.write_all(hostmode_frame).await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 2000).await;
    print_bytes("hostmode L response", &resp);

    // Try G command too
    // G command on channel 31: AA AA 1F 01 00 47 E1 E1
    let g_frame: &[u8] = &[0xaa, 0xaa, 0x1f, 0x01, 0x00, 0x47, 0xe1, 0xe1];
    println!("  >> hostmode G ch31: {:02x?}", g_frame);
    let _ = serial.write_all(g_frame).await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 2000).await;
    print_bytes("hostmode G response", &resp);

    // Try with sequence reset bit (0x41)
    // L command ch31 with reset: AA AA 1F 41 00 4C 44 59
    let reset_frame: &[u8] = &[0xaa, 0xaa, 0x1f, 0x41, 0x00, 0x4c, 0x44, 0x59];
    println!("  >> hostmode L ch31 (reset): {:02x?}", reset_frame);
    let _ = serial.write_all(reset_frame).await;
    let _ = serial.flush().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 2000).await;
    print_bytes("hostmode L (reset) response", &resp);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("Probing modem on {} ...", args.port);

    // Deep probe with 8N1 (the standard config)
    deep_probe(
        &args.port,
        args.baud,
        DataBits::Eight,
        Parity::None,
        StopBits::One,
        "8N1",
    )
    .await;

    // Deep probe with 7E1 (showed promising results)
    deep_probe(
        &args.port,
        args.baud,
        DataBits::Seven,
        Parity::Even,
        StopBits::One,
        "7E1",
    )
    .await;

    println!("\nDone.");
    Ok(())
}
