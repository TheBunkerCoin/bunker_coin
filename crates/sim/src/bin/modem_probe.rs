//! Minimal serial probe for SCS PACTOR modems.
//!
//! Captures the initial burst of data from the modem immediately on port
//! open, then tries to decode it as CRC hostmode frames.
//!
//! ```text
//! cargo run --bin modem_probe -- --port /dev/ttyUSB0
//! ```

use std::time::Duration;

use clap::Parser;
use scs_pactor::hostmode::{encode_frame, HostmodeDecoder, HostmodeFrame, HostmodePacket};
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

fn set_ftdi_latency(port: &str) {
    let resolved = match std::fs::canonicalize(port) {
        Ok(p) => p,
        Err(_) => return,
    };
    let Some(name) = resolved.file_name().and_then(|n| n.to_str()) else {
        return;
    };
    if !name.starts_with("ttyUSB") {
        return;
    }
    let path = format!("/sys/bus/usb-serial/devices/{name}/latency_timer");
    let _ = std::fs::write(&path, "1");
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
            } else {
                '.'
            }
        })
        .collect();
    println!("  {label}: {} bytes", bytes.len());
    // Print hex in rows of 16
    for (i, chunk) in bytes.chunks(16).enumerate() {
        let hex: Vec<String> = chunk.iter().map(|b| format!("{:02x}", b)).collect();
        let asc: String = chunk
            .iter()
            .map(|&b| {
                if b.is_ascii_graphic() || b == b' ' {
                    b as char
                } else {
                    '.'
                }
            })
            .collect();
        println!("    {:04x}: {:48} {}", i * 16, hex.join(" "), asc);
    }
    let _ = ascii; // suppress warning
}

fn try_decode_hostmode(bytes: &[u8]) -> Vec<HostmodePacket> {
    let mut decoder = HostmodeDecoder::new();
    decoder.push(bytes);
    let mut packets = Vec::new();
    loop {
        match decoder.next_packet() {
            Ok(Some(pkt)) => packets.push(pkt),
            _ => break,
        }
    }
    packets
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    println!("Probing modem on {} at {} baud ...\n", args.port, args.baud);

    set_ftdi_latency(&args.port);

    let mut serial = tokio_serial::new(&args.port, args.baud)
        .data_bits(DataBits::Eight)
        .parity(Parity::None)
        .stop_bits(StopBits::One)
        .flow_control(FlowControl::None)
        .open_native_async()
        .map_err(|e| anyhow::anyhow!("failed to open {}: {e}", args.port))?;
    let _ = serial.write_data_terminal_ready(true);
    let _ = serial.write_request_to_send(true);

    // === Phase 1: Capture initial burst ===
    println!("=== Phase 1: Capturing initial burst (5 seconds) ===");
    let burst = read_all(&mut serial, 5000).await;
    print_bytes("initial burst", &burst);

    if !burst.is_empty() {
        // Try to decode as hostmode frames
        let packets = try_decode_hostmode(&burst);
        if !packets.is_empty() {
            println!("\n  Decoded {} hostmode packet(s):", packets.len());
            for (i, pkt) in packets.iter().enumerate() {
                match pkt {
                    HostmodePacket::Frame(f) => {
                        let payload_ascii: String = f.payload.iter().map(|&b| {
                            if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' }
                        }).collect();
                        println!(
                            "    [{i}] Frame ch={} code=0x{:02x} len={} payload={:02x?} ascii=\"{payload_ascii}\"",
                            f.channel, f.code, f.payload.len(), &f.payload
                        );
                    }
                    HostmodePacket::RepeatRequest => {
                        println!("    [{i}] RepeatRequest");
                    }
                }
            }
        } else {
            println!("\n  No valid hostmode frames found in burst.");
        }

        // Count 0xAA bytes — they indicate hostmode sync patterns
        let aa_count = burst.iter().filter(|&&b| b == 0xAA).count();
        println!("  0xAA sync bytes in burst: {aa_count}");
    }

    // === Phase 2: Send CRC-framed JHOST0 to exit hostmode ===
    println!("\n=== Phase 2: Sending CRC-framed JHOST0 (exit hostmode) ===");
    let quit_frame = encode_frame(&HostmodeFrame::command(0, b"JHOST0".to_vec()))?;
    println!("  >> {:02x?}", quit_frame);
    serial.write_all(&quit_frame).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("JHOST0 response", &resp);

    let packets = try_decode_hostmode(&resp);
    if !packets.is_empty() {
        println!("  Decoded {} packet(s) from JHOST0 response:", packets.len());
        for (i, pkt) in packets.iter().enumerate() {
            println!("    [{i}] {:?}", pkt);
        }
        println!("  => Modem WAS in hostmode and responded to JHOST0!");
    }

    // Wait for modem to settle back to terminal mode
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let extra = read_all(&mut serial, 2000).await;
    if !extra.is_empty() {
        print_bytes("post-JHOST0 extra", &extra);
    }

    // === Phase 3: Try terminal commands ===
    println!("\n=== Phase 3: Terminal mode commands ===");
    for cmd in ["", "MYcall", "VER", "PTCH", "SERBaud"] {
        let label = if cmd.is_empty() { "CR" } else { cmd };
        println!("  >> {label}");
        serial.write_all(cmd.as_bytes()).await?;
        serial.write_all(b"\r").await?;
        serial.flush().await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let resp = read_all(&mut serial, 3000).await;
        print_bytes(label, &resp);
    }

    // === Phase 4: Enter JHOST4 and test hostmode ===
    println!("\n=== Phase 4: Enter JHOST4 and verify ===");
    println!("  >> JHOST4");
    serial.write_all(b"JHOST4\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("JHOST4 response", &resp);

    // Send hostmode L command with sequence reset bit
    let l_frame = encode_frame(&HostmodeFrame::with_code(31, 0x41, b"L".to_vec()))?;
    println!("  >> hostmode L ch31 (reset): {:02x?}", l_frame);
    serial.write_all(&l_frame).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("hostmode L response", &resp);
    let packets = try_decode_hostmode(&resp);
    if !packets.is_empty() {
        println!("  HOSTMODE IS WORKING! Decoded {} packet(s):", packets.len());
        for (i, pkt) in packets.iter().enumerate() {
            println!("    [{i}] {:?}", pkt);
        }
    }

    // Send G poll too
    let g_frame = encode_frame(&HostmodeFrame::command(31, b"G".to_vec()))?;
    println!("  >> hostmode G ch31: {:02x?}", g_frame);
    serial.write_all(&g_frame).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("hostmode G response", &resp);
    let packets = try_decode_hostmode(&resp);
    if !packets.is_empty() {
        println!("  HOSTMODE IS WORKING! Decoded {} packet(s):", packets.len());
        for (i, pkt) in packets.iter().enumerate() {
            println!("    [{i}] {:?}", pkt);
        }
    }

    println!("\nDone.");
    Ok(())
}
