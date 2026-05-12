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
use scs_pactor::{PactorTransport, UsbPactorConfig, UsbPactorTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_serial::{DataBits, FlowControl, Parity, SerialPortBuilderExt, StopBits};

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
    #[arg(long, default_value_t = 115_200)]
    baud: u32,

    /// SCS JHOST mode to enter before speaking hostmode.
    ///
    /// 1 is plain hostmode, 4 is CRC hostmode, and 5 is extended CRC hostmode.
    /// The scs_pactor transport uses CRC-framed hostmode packets, so 5 is the
    /// safest default for DRAGON modems.
    #[arg(long, default_value_t = 5)]
    jhost: u8,
}

/// Switch an SCS modem from terminal mode into WA8DED hostmode.
///
/// Sends ESC to break out of any current state, waits for the modem to
/// settle, then sends `JHOST{jhost}\r` to enter hostmode. The serial port is
/// consumed and returned as a `UsbPactorTransport` ready for hostmode
/// framing.
/// Drain any pending bytes from the serial port (non-blocking read until empty).
async fn drain_serial(serial: &mut tokio_serial::SerialStream) {
    let mut buf = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_millis(100), serial.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => {
                println!("  drained {} bytes: {:?}", n, String::from_utf8_lossy(&buf[..n]));
            }
            _ => break,
        }
    }
}

/// Switch an SCS modem from terminal mode into WA8DED hostmode.
///
/// The SCS DRAGON boots in terminal/command mode. We must:
/// 1. Send ESC to abort any in-progress command
/// 2. Drain stale data
/// 3. Send `JHOST{jhost}\r` to enter WA8DED hostmode
/// 4. Drain the hostmode-entry response
/// 5. Verify hostmode is active via a `G` (poll) transaction
async fn init_hostmode(port: &str, baud: u32, jhost: u8) -> anyhow::Result<UsbPactorTransport> {
    anyhow::ensure!(
        matches!(jhost, 1 | 4 | 5),
        "unsupported JHOST mode {jhost}; expected 1, 4, or 5"
    );

    let mut serial = tokio_serial::new(port, baud)
        .data_bits(DataBits::Eight)
        .parity(Parity::None)
        .stop_bits(StopBits::One)
        .flow_control(FlowControl::None)
        .open_native_async()
        .map_err(|e| anyhow::anyhow!("failed to open {port}: {e}"))?;

    // ESC to break out of any current state
    serial.write_all(b"\x1b").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    drain_serial(&mut serial).await;

    // Send a bare CR to get a clean prompt
    serial.write_all(b"\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain_serial(&mut serial).await;

    // Enter WA8DED hostmode.
    println!("  sending JHOST{jhost} ...");
    serial
        .write_all(format!("JHOST{jhost}\r").as_bytes())
        .await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    drain_serial(&mut serial).await;

    // Wrap in hostmode transport
    let config = UsbPactorConfig::new(port);
    let transport = UsbPactorTransport::from_stream(serial, config);

    // Verify hostmode is active by polling the command channel.
    // In hostmode, a poll on channel 0 should return a valid frame.
    println!("  verifying hostmode ...");
    match tokio::time::timeout(Duration::from_secs(3), transport.poll_channel(0)).await {
        Ok(Ok(frame)) => {
            println!(
                "  hostmode OK: ch={} code={} payload={:?}",
                frame.channel,
                frame.code,
                String::from_utf8_lossy(&frame.payload)
            );
        }
        Ok(Err(e)) => {
            return Err(anyhow::anyhow!(
                "hostmode verification failed for {port}: {e}"
            ));
        }
        Err(_) => {
            return Err(anyhow::anyhow!(
                "hostmode verification timed out for {port} — modem may not be in hostmode"
            ));
        }
    }

    Ok(transport)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    println!("Initializing modem A on {} ...", args.port_a);
    let modem_a = init_hostmode(&args.port_a, args.baud, args.jhost).await?;

    println!("Initializing modem B on {} ...", args.port_b);
    let modem_b = init_hostmode(&args.port_b, args.baud, args.jhost).await?;

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

    // Verify correctness
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
