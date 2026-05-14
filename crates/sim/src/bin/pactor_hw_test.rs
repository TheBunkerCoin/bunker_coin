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
                    .map(|&b| if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' })
                    .collect();
                println!("  rx {} bytes hex={:02x?} ascii=\"{}\"", n, chunk, ascii);
                all.extend_from_slice(chunk);
            }
            _ => break,
        }
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

    // Keep control lines asserted. Some USB serial stacks/firmware gate the
    // command interface when DTR/RTS are low; ptc-go's serial backend leaves
    // these in the normal active terminal state.
    let _ = serial.write_data_terminal_ready(true);
    let _ = serial.write_request_to_send(true);
    Ok(serial)
}

/// Send an ASCII command and read the modem's full response.
async fn send_ascii(serial: &mut tokio_serial::SerialStream, cmd: &str) -> anyhow::Result<Vec<u8>> {
    println!("  >> {cmd}");
    serial.write_all(cmd.as_bytes()).await?;
    serial.write_all(b"\r").await?;
    serial.flush().await?;
    // Give the modem time to process and respond
    tokio::time::sleep(Duration::from_millis(800)).await;
    Ok(drain_serial(serial).await)
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
    tokio::time::sleep(Duration::from_millis(500)).await;
    drain_serial(serial).await;
    Ok(())
}

/// Send a raw byte sequence and drain the response.
async fn send_raw(
    serial: &mut tokio_serial::SerialStream,
    label: &str,
    bytes: &[u8],
    wait_ms: u64,
) -> anyhow::Result<Vec<u8>> {
    println!("  >> {label} ({} bytes: {:02x?})", bytes.len(), bytes);
    serial.write_all(bytes).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    Ok(drain_serial(serial).await)
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
    println!("  [1/3] trying direct hostmode poll ...");
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
    println!("  [2/3] full terminal-mode init ...");
    let mut serial = open_serial(port, baud)?;

    // First, drain anything pending from the modem
    println!("  draining initial buffer ...");
    drain_serial(&mut serial).await;

    // Send ESC to break out of any mode the modem might be in
    send_raw(&mut serial, "ESC", b"\x1b", 500).await?;

    // Exit any existing CRC hostmode by sending the binary-framed JHOST0
    println!("  exiting any existing hostmode ...");
    send_hostmode_quit(&mut serial).await?;

    // Also try ASCII JHOST0 in case the modem is in terminal mode
    send_ascii(&mut serial, "JHOST0").await?;

    // Send ESC + CR to get to clean command prompt
    send_raw(&mut serial, "ESC", b"\x1b", 300).await?;
    send_raw(&mut serial, "CR", b"\r", 500).await?;

    // Quit to main menu
    send_ascii(&mut serial, "Quit").await?;

    // ESC + CR again to be sure we're at the command prompt
    send_raw(&mut serial, "ESC+CR", b"\x1b\r", 500).await?;

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

    // Enter JHOST4 CRC hostmode
    println!("  entering JHOST4 ...");
    let jhost_resp = send_ascii(&mut serial, "JHOST4").await?;
    // Give extra time for mode switch
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let extra = drain_serial(&mut serial).await;
    println!(
        "  JHOST4 response: {} + {} bytes",
        jhost_resp.len(),
        extra.len()
    );

    // Wrap in hostmode transport and verify with retries
    let config = UsbPactorConfig::new(port);
    let transport = UsbPactorTransport::from_stream(serial, config);

    for attempt in 1..=3 {
        println!("  verifying hostmode (attempt {attempt}/3) ...");
        if verify_hostmode(&transport).await {
            return Ok(transport);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // === Attempt 3: try different baud rates ===
    drop(transport);
    println!("  [3/3] hostmode failed, trying raw CRC frame at different baud rates ...");

    for test_baud in [230_400u32, 115_200, 57_600, 38_400, 19_200, 9600] {
        if test_baud == baud {
            continue; // already tried this one
        }
        println!("  trying baud {test_baud} ...");
        let Ok(mut test_serial) = open_serial(port, test_baud) else {
            continue;
        };
        // Send ESC + CR
        let _ = send_raw(&mut test_serial, "ESC+CR", b"\x1b\r", 500).await;
        let resp = send_ascii(&mut test_serial, "").await?;
        if !resp.is_empty() {
            println!("  got response at baud {test_baud}! Retrying init at this baud.");
            drop(test_serial);
            // Recurse with the working baud rate
            return Box::pin(init_hostmode(port, test_baud, callsign)).await;
        }
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
