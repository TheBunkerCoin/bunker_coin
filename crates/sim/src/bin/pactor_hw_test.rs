//! Hardware PACTOR radio-proto demo.
//!
//! Runs the Ping/Shred/Pong exchange over two real SCS PACTOR modems
//! connected via USB serial.
//!
//! ```text
//! cargo run --bin pactor_hw_test -- \
//!   --port-a /dev/ttyUSB0 --port-b /dev/ttyUSB1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use bunker_coin_radio::{Network, NetworkMessage, PactorRadioNode};
use clap::Parser;
use scs_pactor::hostmode::{
    encode_frame, HostmodeDecoder, HostmodeFrame, HostmodePacket, PACTOR_CHANNEL, TYPE_COMMAND,
};
use scs_pactor::{PactorTransport, UsbPactorConfig, UsbPactorTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_serial::{DataBits, FlowControl, Parity, SerialPort, SerialPortBuilderExt, StopBits};

#[derive(Parser)]
#[command(name = "pactor_hw_test")]
#[command(about = "Run radio-proto exchange over two real USB PACTOR modems")]
struct Args {
    /// Serial port for modem A (sender)
    #[arg(long)]
    port_a: String,

    /// Serial port for modem B (receiver)
    #[arg(long)]
    port_b: String,

    /// Callsign for modem A
    #[arg(long, default_value = "CLIENT")]
    call_a: String,

    /// Callsign for modem B
    #[arg(long, default_value = "NODE")]
    call_b: String,

    /// Baud rate for serial ports
    #[arg(long, default_value_t = 230_400)]
    baud: u32,
}

/// Read all pending bytes from the serial port, printing hex + ASCII.
/// Returns all bytes collected.
async fn drain_serial(serial: &mut tokio_serial::SerialStream) -> Vec<u8> {
    let mut all = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_millis(500), serial.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => {
                let chunk = &buf[..n];
                let ascii: String = chunk
                    .iter()
                    .map(|&b| {
                        if b.is_ascii_graphic() || b == b' ' {
                            b as char
                        } else {
                            '.'
                        }
                    })
                    .collect();
                println!("  rx {} bytes hex={:02x?} ascii=\"{}\"", n, chunk, ascii);
                all.extend_from_slice(chunk);
            }
            _ => break,
        }
    }
    all
}

/// Read bytes with a generous timeout, collecting everything the modem sends.
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
            Ok(Ok(n)) if n > 0 => {
                all.extend_from_slice(&buf[..n]);
            }
            _ => break,
        }
    }
    if !all.is_empty() {
        let ascii: String = all
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
            "  rx {} bytes hex={:02x?} ascii=\"{}\"",
            all.len(),
            &all,
            ascii
        );
    }
    all
}

/// Open a serial port.
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

/// Send an ASCII command (terminal mode) and read response.
async fn send_ascii(
    serial: &mut tokio_serial::SerialStream,
    cmd: &str,
) -> anyhow::Result<Vec<u8>> {
    println!("  >> {cmd}");
    serial.write_all(cmd.as_bytes()).await?;
    serial.write_all(b"\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    Ok(read_all(serial, 800).await)
}

/// Send a CRC-framed hostmode command and read the raw response bytes.
/// Does NOT use UsbPactorTransport — works directly on the serial port for
/// low-level debugging.
async fn send_hostmode_raw(
    serial: &mut tokio_serial::SerialStream,
    frame: &HostmodeFrame,
    label: &str,
) -> anyhow::Result<Vec<u8>> {
    let encoded = encode_frame(frame)?;
    println!(
        "  >> hostmode {label}: {:02x?} ({} bytes)",
        encoded,
        encoded.len()
    );
    serial.write_all(&encoded).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(read_all(serial, 1000).await)
}

/// Send a CRC-framed hostmode command with bit 6 (sequence reset) set.
/// This should be used for the first frame after entering hostmode to ensure
/// the modem ACKs regardless of its internal packet counter state.
async fn send_hostmode_raw_with_reset(
    serial: &mut tokio_serial::SerialStream,
    channel: u8,
    payload: &[u8],
    label: &str,
) -> anyhow::Result<Vec<u8>> {
    // Bit 6 = sequence reset, Bit 0 = 1 for command → 0x41
    let frame = HostmodeFrame::with_code(channel, TYPE_COMMAND | 0x40, payload.to_vec());
    send_hostmode_raw(serial, &frame, label).await
}

/// Try to verify hostmode is active on an already-wrapped transport.
async fn verify_hostmode(transport: &UsbPactorTransport) -> bool {
    let status = HostmodeFrame::command(PACTOR_CHANNEL, b"L".to_vec());
    match tokio::time::timeout(Duration::from_secs(3), transport.hostmode_transaction(status))
        .await
    {
        Ok(Ok(frame)) => {
            println!(
                "  hostmode OK: ch={} code={} payload={:02x?}",
                frame.channel, frame.code, &frame.payload
            );
            true
        }
        Ok(Err(e)) => {
            println!("  hostmode poll error: {e}");
            false
        }
        Err(_) => {
            println!("  hostmode poll timed out");
            false
        }
    }
}

/// Try to decode any hostmode frames from raw bytes.
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

/// Initialize an SCS modem into JHOST4 CRC hostmode.
///
/// Strategy:
/// 1. Send CRC-framed JHOST0 to exit any existing hostmode (matching ptc-go)
/// 2. Send terminal-mode ASCII commands for configuration
/// 3. Enter JHOST4 CRC hostmode
/// 4. Verify with a CRC-framed status poll with bit 6 (sequence reset) set
async fn init_hostmode(
    port: &str,
    baud: u32,
    callsign: &str,
) -> anyhow::Result<UsbPactorTransport> {
    let mut serial = open_serial(port, baud)?;

    // Drain any leftover data in the serial buffer
    println!("  draining ...");
    drain_serial(&mut serial).await;

    // === Step 1: Exit any existing hostmode ===
    // ptc-go sends CRC-framed JHOST0 on channel 0 first thing.
    println!("  step 1: exit any existing hostmode ...");
    let quit_frame = HostmodeFrame::command(0, b"JHOST0".to_vec());
    let resp = send_hostmode_raw(&mut serial, &quit_frame, "JHOST0 ch0").await?;

    // Check if the response looks like a hostmode frame
    let packets = try_decode_hostmode(&resp);
    if !packets.is_empty() {
        println!(
            "  got {} hostmode packet(s) — modem WAS in hostmode",
            packets.len()
        );
        for pkt in &packets {
            println!("    {:?}", pkt);
        }
    }

    // Also send CRC-framed JHOST0 with bit 6 set (sequence reset) in case
    // the modem ignored the first one due to counter mismatch
    let resp2 =
        send_hostmode_raw_with_reset(&mut serial, 0, b"JHOST0", "JHOST0 ch0 (reset)").await?;
    let packets2 = try_decode_hostmode(&resp2);
    if !packets2.is_empty() {
        println!("  got {} hostmode packet(s) after reset-bit JHOST0", packets2.len());
        for pkt in &packets2 {
            println!("    {:?}", pkt);
        }
    }

    // Give the modem time to fully exit hostmode
    tokio::time::sleep(Duration::from_millis(500)).await;
    drain_serial(&mut serial).await;

    // === Step 2: Terminal-mode ASCII init ===
    // ptc-go sends an empty command first, then "Quit"
    println!("  step 2: terminal-mode init ...");

    // Send CR to sync terminal
    send_ascii(&mut serial, "").await?;

    // Quit to main menu (ptc-go does this; harmless error if already there)
    send_ascii(&mut serial, "Quit").await?;

    // Pre-hostmode config commands (matching ptc-go)
    let commands = [
        format!("MYcall {callsign}"),
        format!("PTCH {PACTOR_CHANNEL}"),
        "MAXE 35".to_owned(),
        "REM 0".to_owned(),
        "CHOB 0".to_owned(),
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

    // === Step 3: Enter JHOST4 CRC hostmode ===
    // ptc-go sends this as a terminal-mode ASCII command
    println!("  step 3: entering JHOST4 ...");
    send_ascii(&mut serial, "JHOST4").await?;

    // ptc-go does an extra read after JHOST4 to consume the startup banner
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let banner = read_all(&mut serial, 2000).await;
    if !banner.is_empty() {
        let banner_packets = try_decode_hostmode(&banner);
        if !banner_packets.is_empty() {
            println!("  JHOST4 banner decoded as hostmode:");
            for pkt in &banner_packets {
                println!("    {:?}", pkt);
            }
        }
    }

    // === Step 4: Verify hostmode with sequence-reset bit ===
    // Send an L (status) command on channel 31 with bit 6 set (0x41 type byte)
    // to force the modem to ACK regardless of counter state.
    println!("  step 4: verifying hostmode ...");
    for attempt in 1..=5 {
        println!("  verify attempt {attempt}/5 ...");

        // Send raw CRC-framed poll with sequence reset bit
        let resp = send_hostmode_raw_with_reset(
            &mut serial,
            PACTOR_CHANNEL,
            b"L",
            &format!("L ch{PACTOR_CHANNEL} (reset)"),
        )
        .await?;

        let packets = try_decode_hostmode(&resp);
        if !packets.is_empty() {
            println!("  hostmode verified! Got {} packet(s):", packets.len());
            for pkt in &packets {
                println!("    {:?}", pkt);
            }

            // Success — wrap in transport
            let config = UsbPactorConfig::new(port);
            let transport = UsbPactorTransport::from_stream(serial, config);
            return Ok(transport);
        }

        // Also try without the reset bit in case the modem already synced
        if attempt >= 3 {
            let resp2 = send_hostmode_raw(
                &mut serial,
                &HostmodeFrame::command(PACTOR_CHANNEL, b"G".to_vec()),
                &format!("G ch{PACTOR_CHANNEL}"),
            )
            .await?;
            let packets2 = try_decode_hostmode(&resp2);
            if !packets2.is_empty() {
                println!("  hostmode verified via G poll!");
                let config = UsbPactorConfig::new(port);
                let transport = UsbPactorTransport::from_stream(serial, config);
                return Ok(transport);
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // === Fallback: try wrapping in transport anyway ===
    // The transport's reader task may be able to sync even if our manual
    // decode couldn't.
    println!("  fallback: trying UsbPactorTransport wrapper ...");
    let config = UsbPactorConfig::new(port);
    let transport = UsbPactorTransport::from_stream(serial, config);

    for attempt in 1..=3 {
        println!("  transport verify attempt {attempt}/3 ...");
        if verify_hostmode(&transport).await {
            return Ok(transport);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(anyhow::anyhow!(
        "failed to enter hostmode on {port} — check modem power and baud rate"
    ))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    println!("Initializing modem A on {} ...", args.port_a);
    let modem_a = init_hostmode(&args.port_a, args.baud, &args.call_a).await?;

    println!("Initializing modem B on {} ...", args.port_b);
    let modem_b = init_hostmode(&args.port_b, args.baud, &args.call_b).await?;

    println!("Setting callsigns: A={}, B={}", args.call_a, args.call_b);
    modem_a.set_mycall(&args.call_a).await?;
    modem_b.set_mycall(&args.call_b).await?;

    println!("Modem A connecting to {} ...", args.call_b);
    let link_start = Instant::now();
    modem_a.connect_peer(&args.call_b).await?;
    let link_elapsed = link_start.elapsed();
    println!("Link established in {link_elapsed:.2?}");

    let transport_a: Arc<dyn PactorTransport> = Arc::new(modem_a);
    let transport_b: Arc<dyn PactorTransport> = Arc::new(modem_b);
    let node_a = PactorRadioNode::from_shared(&args.call_a, Arc::clone(&transport_a));
    let node_b = PactorRadioNode::from_shared(&args.call_b, Arc::clone(&transport_b));

    let messages = vec![
        NetworkMessage::Ping,
        NetworkMessage::Shred(b"radio-proto-over-pactor-hw".to_vec()),
        NetworkMessage::Pong,
    ];

    println!("Sending {} messages from A -> B ...", messages.len());
    let send_start = Instant::now();
    for msg in &messages {
        node_a.send(msg, &args.call_b).await?;
    }
    let send_elapsed = send_start.elapsed();

    println!("Receiving on B ...");
    let recv_start = Instant::now();
    let mut received = Vec::new();
    for _ in 0..messages.len() {
        let msg = node_b.receive().await?;
        received.push(msg);
    }
    let recv_elapsed = recv_start.elapsed();

    println!();
    println!("=== PACTOR hardware demo results ===");
    println!("Link setup:    {link_elapsed:.2?}");
    println!("Send time:     {send_elapsed:.2?}");
    println!("Receive time:  {recv_elapsed:.2?}");
    println!("Total:         {:.2?}", link_start.elapsed());
    println!("Messages received: {}", received.len());
    for (i, msg) in received.iter().enumerate() {
        match msg {
            NetworkMessage::Ping => println!("  [{i}] Ping"),
            NetworkMessage::Pong => println!("  [{i}] Pong"),
            NetworkMessage::Shred(data) => {
                println!("  [{i}] Shred ({} bytes)", data.len())
            }
        }
    }

    let ok = received.len() == messages.len()
        && matches!(received[0], NetworkMessage::Ping)
        && matches!(received[1], NetworkMessage::Shred(_))
        && matches!(received[2], NetworkMessage::Pong);
    if ok {
        println!("All messages received correctly!");
    } else {
        println!("WARNING: message mismatch");
    }

    println!("Disconnecting ...");
    drop(node_a);
    drop(node_b);
    transport_a.disconnect().await?;
    transport_b.disconnect().await?;
    println!("Done.");

    Ok(())
}
