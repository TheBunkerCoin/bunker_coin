//! Hardware PACTOR radio-proto demo.
//!
//! Runs the Ping/Shred/Pong exchange over two real SCS PACTOR modems
//! connected via USB serial.
//!
//! ```text
//! cargo run --bin pactor_hw_test -- \
//!   --port-a /dev/cu.usbserial-A --port-b /dev/cu.usbserial-B
//! ```

use std::sync::Arc;
use std::time::Instant;

use bunker_coin_radio::{Network, NetworkMessage, PactorRadioNode};
use clap::Parser;
use scs_pactor::{PactorTransport, UsbPactorConfig, UsbPactorTransport};

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    println!("Connecting to modem A on {} ...", args.port_a);
    let mut config_a = UsbPactorConfig::new(&args.port_a);
    config_a.baud_rate = args.baud;
    let modem_a = UsbPactorTransport::connect(config_a).await?;

    println!("Connecting to modem B on {} ...", args.port_b);
    let mut config_b = UsbPactorConfig::new(&args.port_b);
    config_b.baud_rate = args.baud;
    let modem_b = UsbPactorTransport::connect(config_b).await?;

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
