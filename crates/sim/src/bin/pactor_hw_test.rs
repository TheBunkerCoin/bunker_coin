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
use scs_pactor::hostmode::{encode_frame, HostmodeFrame, PACTOR_CHANNEL};
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

/// Drain any pending bytes from the serial port (non-blocking read until empty).
async fn drain_serial(serial: &mut tokio_serial::SerialStream) {
    let mut buf = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_millis(200), serial.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => {
                println!("  drained {} bytes hex: {:02x?}", n, &buf[..n]);
            }
            _ => break,
        }
    }
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

    // Keep control lines asserted. Some USB serial stacks/firmware gate the
    // command interface when DTR/RTS are low; ptc-go's serial backend leaves
    // these in the normal active terminal state.
    let _ = serial.write_data_terminal_ready(true);
    let _ = serial.write_request_to_send(true);
    Ok(serial)
}

/// Send an ASCII command and wait for the modem to process it.
async fn send_ascii(serial: &mut tokio_serial::SerialStream, cmd: &str) -> anyhow::Result<()> {
    println!("  >> {cmd}");
    serial.write_all(cmd.as_bytes()).await?;
    serial.write_all(b"\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    drain_serial(serial).await;
    Ok(())
}

/// Try to verify hostmode is active on an already-wrapped transport.
async fn verify_hostmode(transport: &UsbPactorTransport) -> bool {
    // ptc-go uses the L command on the PACTOR channel to query channel state.
    // This works even when the modem has no pending receive data.
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

/// Try to leave any existing CRC hostmode session.
async fn send_hostmode_quit(serial: &mut tokio_serial::SerialStream) -> anyhow::Result<()> {
    println!("  >> hostmode JHOST0 on channel 0");
    let frame = HostmodeFrame::command(0, b"JHOST0".to_vec());
    let encoded = encode_frame(&frame)?;
    println!("  hostmode quit bytes: {:02x?}", encoded);
    serial.write_all(&encoded).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain_serial(serial).await;
    Ok(())
}

/// Initialize an SCS modem into JHOST4 CRC hostmode.
///
/// Follows the ptc-go initialization sequence:
/// 1. Try direct hostmode poll (modem may already be in hostmode)
/// 2. If not, send JHOST0 to exit any existing hostmode
/// 3. Send Quit to reach main menu
/// 4. Send config commands (MYcall, PTCH 31, MAXE 35)
/// 5. Enter JHOST4 CRC hostmode
async fn init_hostmode(
    port: &str,
    baud: u32,
    callsign: &str,
) -> anyhow::Result<UsbPactorTransport> {
    // === Attempt 1: modem may already be in hostmode ===
    println!("  trying direct hostmode poll ...");
    {
        let serial = open_serial(port, baud)?;
        let config = UsbPactorConfig::new(port);
        let transport = UsbPactorTransport::from_stream(serial, config);

        if verify_hostmode(&transport).await {
            println!("  modem already in hostmode");
            return Ok(transport);
        }
        drop(transport);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // === Attempt 2: full terminal-mode init (matching ptc-go) ===
    let mut serial = open_serial(port, baud)?;

    // Exit any existing CRC hostmode first. Plain ASCII JHOST0 is only useful
    // after we are already back in terminal mode.
    println!("  exiting any existing hostmode ...");
    send_hostmode_quit(&mut serial).await?;

    // A blank terminal command gives the modem a chance to print the prompt.
    send_ascii(&mut serial, "").await?;

    // Try ASCII JHOST0 too, in case the modem is already in terminal mode.
    send_ascii(&mut serial, "JHOST0").await?;

    // Quit to main menu
    send_ascii(&mut serial, "Quit").await?;

    // ESC again to be sure we're at the command prompt
    serial.write_all(b"\x1b").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain_serial(&mut serial).await;

    // Send CR to get a clean prompt
    serial.write_all(b"\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain_serial(&mut serial).await;

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
    for command in commands {
        send_ascii(&mut serial, &command).await?;
    }

    // Enter JHOST4 CRC hostmode
    println!("  entering JHOST4 ...");
    serial.write_all(b"JHOST4\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    drain_serial(&mut serial).await;

    // Wrap in hostmode transport and verify
    let config = UsbPactorConfig::new(port);
    let transport = UsbPactorTransport::from_stream(serial, config);

    println!("  verifying hostmode after JHOST4 ...");
    if verify_hostmode(&transport).await {
        return Ok(transport);
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
