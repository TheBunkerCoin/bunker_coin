//! Minimal serial probe for SCS PACTOR modems using the DR-7400 custom baud rate.

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

    #[arg(long, default_value_t = 829_440)]
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
        if i == 0 {
            println!(
                "  {label}: {} bytes\n    {:04x}: {:48} {}",
                bytes.len(),
                i * 16,
                hex.join(" "),
                asc
            );
        } else {
            println!("    {:04x}: {:48} {}", i * 16, hex.join(" "), asc);
        }
    }
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
    println!(
        "Probing SCS Dragon modem on {} at {} baud ...\n",
        args.port, args.baud
    );

    set_ftdi_latency(&args.port);

    println!("=== Opening serial port at {} baud ===", args.baud);
    let mut serial = match tokio_serial::new(&args.port, args.baud)
        .data_bits(DataBits::Eight)
        .parity(Parity::None)
        .stop_bits(StopBits::One)
        .flow_control(FlowControl::None)
        .open_native_async()
    {
        Ok(s) => s,
        Err(e) => {
            println!("  Failed to open at {} baud: {e}", args.baud);
            println!("  The FTDI FT232R may not support this custom baud rate.");
            println!("  Trying to set it via Linux ioctl...");

            // Some FTDI drivers require custom baud configuration outside this probe.
            return Err(anyhow::anyhow!(
                "Cannot open at {} baud. Try: stty -F {} {} raw",
                args.baud,
                args.port,
                args.baud
            ));
        }
    };
    let _ = serial.write_data_terminal_ready(true);
    let _ = serial.write_request_to_send(true);
    println!("  Port opened successfully!");

    println!("\n=== Phase 1: Initial burst (3 sec) ===");
    let burst = read_all(&mut serial, 3000).await;
    print_bytes("initial", &burst);

    if !burst.is_empty() {
        let packets = try_decode_hostmode(&burst);
        if !packets.is_empty() {
            println!("  Decoded {} hostmode packet(s)!", packets.len());
            for (i, pkt) in packets.iter().enumerate() {
                println!("    [{i}] {:?}", pkt);
            }
        }
        let aa_count = burst.iter().filter(|&&b| b == 0xAA).count();
        if aa_count > 0 {
            println!("  Found {} potential 0xAA sync bytes", aa_count);
        }
    }

    println!("\n=== Phase 2: CRC-framed JHOST0 (exit hostmode) ===");
    let quit_frame = encode_frame(&HostmodeFrame::command(0, b"JHOST0".to_vec()))?;
    println!("  >> {:02x?}", quit_frame);
    serial.write_all(&quit_frame).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("JHOST0 resp", &resp);
    let packets = try_decode_hostmode(&resp);
    if !packets.is_empty() {
        println!(
            "  Modem responded in hostmode! {} packet(s):",
            packets.len()
        );
        for (i, pkt) in packets.iter().enumerate() {
            match pkt {
                HostmodePacket::Frame(f) => {
                    let asc: String = f
                        .payload
                        .iter()
                        .map(|&b| {
                            if b.is_ascii_graphic() || b == b' ' {
                                b as char
                            } else {
                                '.'
                            }
                        })
                        .collect();
                    println!(
                        "    [{i}] ch={} code=0x{:02x} payload=\"{asc}\"",
                        f.channel, f.code
                    );
                }
                HostmodePacket::RepeatRequest => println!("    [{i}] RepeatRequest"),
            }
        }
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    let extra = read_all(&mut serial, 2000).await;
    if !extra.is_empty() {
        print_bytes("extra", &extra);
    }

    println!("\n=== Phase 3: Terminal mode commands ===");
    for cmd in ["", "MYcall", "VER", "SERBaud"] {
        let label = if cmd.is_empty() { "CR" } else { cmd };
        println!("  >> {label}");
        serial.write_all(cmd.as_bytes()).await?;
        serial.write_all(b"\r").await?;
        serial.flush().await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let resp = read_all(&mut serial, 2000).await;
        print_bytes(label, &resp);
    }

    println!("\n=== Phase 4: Enter JHOST4 ===");
    println!("  >> JHOST4");
    serial.write_all(b"JHOST4\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("JHOST4", &resp);

    println!("\n=== Phase 5: Hostmode verification ===");
    let l_frame = encode_frame(&HostmodeFrame::with_code(31, 0x41, b"L".to_vec()))?;
    println!("  >> hostmode L ch31 (reset): {:02x?}", l_frame);
    serial.write_all(&l_frame).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("L resp", &resp);
    let packets = try_decode_hostmode(&resp);
    if !packets.is_empty() {
        println!("  *** HOSTMODE WORKING! *** {} packet(s):", packets.len());
        for (i, pkt) in packets.iter().enumerate() {
            println!("    [{i}] {:?}", pkt);
        }
    }

    let g_frame = encode_frame(&HostmodeFrame::command(31, b"G".to_vec()))?;
    println!("  >> hostmode G ch31: {:02x?}", g_frame);
    serial.write_all(&g_frame).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let resp = read_all(&mut serial, 3000).await;
    print_bytes("G resp", &resp);
    let packets = try_decode_hostmode(&resp);
    if !packets.is_empty() {
        println!("  *** HOSTMODE WORKING! *** {} packet(s):", packets.len());
        for (i, pkt) in packets.iter().enumerate() {
            println!("    [{i}] {:?}", pkt);
        }
    }

    println!("\nDone.");
    Ok(())
}
