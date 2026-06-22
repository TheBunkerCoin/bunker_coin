//! Hardware PACTOR radio-proto demo.
//!
//! Runs the Ping/Shred/Pong exchange over two real SCS PACTOR modems
//! connected via USB serial.
//!
//! ```text
//! cargo run --bin pactor_hw_test -- \
//!   --port-a /dev/serial/by-id/usb-SCS_SCS_DRAGON_7400_DR83NDYP-if00-port0 \
//!   --port-b /dev/serial/by-id/usb-SCS_SCS_DRAGON_7400_DR752ZE5-if00-port0 \
//!   --frequency 14079.0
//! ```
//!
//! For debug logging of hostmode frames:
//! ```text
//! RUST_LOG=scs_pactor=debug cargo run --bin pactor_hw_test -- ...
//! ```
//!
//! For full trace (includes raw serial bytes):
//! ```text
//! RUST_LOG=scs_pactor=trace cargo run --bin pactor_hw_test -- ...
//! ```
//!
//! SCS DRAGON 7400/P4dragon USB serial uses 829440 baud by default.
//!
//! **Note:** The `--frequency` flag tunes both radios via the modem's TRX
//! CI-V interface. PACTOR still requires an RF path between the modems
//! (antennas, band conditions). Without a physical connection,
//! `connect_peer` will time out.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bunker_coin_radio::{Network, NetworkMessage, PactorRadioNode};
use clap::Parser;
use scs_pactor::hostmode::{HostmodeDecoder, HostmodeFrame, HostmodePacket, PACTOR_CHANNEL};
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

    /// Baud rate for serial ports (SCS Dragon DR-7400 uses 829440)
    #[arg(long, default_value_t = 829_440)]
    baud: u32,

    /// Maximum time to wait for PACTOR link establishment
    #[arg(long, default_value_t = 90)]
    connect_timeout_secs: u64,

    /// Stop after sending C <CALL> and print raw L status polls.
    #[arg(long)]
    diagnose_connect: bool,

    /// In diagnose-connect mode, send C <CALL> with the hostmode reset bit.
    #[arg(long)]
    diagnose_connect_reset: bool,

    /// Radio frequency in kHz (e.g. 14079.0). Sent to both modems via TRX CI-V control.
    /// If omitted, TRX tuning is skipped (useful with --diagnose-connect).
    #[arg(long)]
    frequency: Option<f64>,

    /// Override frequency for modem A only in kHz (e.g. 97000.0 for 97 MHz).
    /// Takes precedence over --frequency for modem A.
    #[arg(long)]
    frequency_a: Option<f64>,

    /// Override frequency for modem B only in kHz (e.g. 97000.0 for 97 MHz).
    /// Takes precedence over --frequency for modem B.
    #[arg(long)]
    frequency_b: Option<f64>,

    /// Override TRX CI-V baud rate for both modems (must pair with --trx-addr)
    #[arg(long)]
    trx_baud: Option<u32>,

    /// Override TRX CI-V address in hex for both modems (must pair with --trx-baud)
    #[arg(long)]
    trx_addr: Option<String>,

    /// Override TRX CI-V baud rate for modem A only (must pair with --trx-addr-a)
    #[arg(long)]
    trx_baud_a: Option<u32>,

    /// Override TRX CI-V address in hex for modem A only (must pair with --trx-baud-a)
    #[arg(long)]
    trx_addr_a: Option<String>,

    /// Override TRX CI-V baud rate for modem B only (must pair with --trx-addr-b)
    #[arg(long)]
    trx_baud_b: Option<u32>,

    /// Override TRX CI-V address in hex for modem B only (must pair with --trx-baud-b)
    #[arg(long)]
    trx_addr_b: Option<String>,
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
async fn send_ascii(serial: &mut tokio_serial::SerialStream, cmd: &str) -> anyhow::Result<Vec<u8>> {
    println!("  >> {cmd}");
    serial.write_all(cmd.as_bytes()).await?;
    serial.write_all(b"\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    Ok(read_all(serial, 800).await)
}

/// Try to verify hostmode is active on an already-wrapped transport.
async fn verify_hostmode(transport: &UsbPactorTransport) -> bool {
    let status = HostmodeFrame::command(PACTOR_CHANNEL, b"L".to_vec());
    match tokio::time::timeout(
        Duration::from_secs(3),
        transport.hostmode_transaction(status),
    )
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

async fn diagnose_connect(
    modem: &UsbPactorTransport,
    remote_call: &str,
    duration: Duration,
    reset_connect: bool,
) -> anyhow::Result<()> {
    println!("Diagnosing connect to {remote_call} ...");
    if reset_connect {
        println!(
            "  note: --diagnose-connect-reset is ignored; transport-managed counters are required"
        );
    }
    let connect_frame =
        HostmodeFrame::command(PACTOR_CHANNEL, format!("C {remote_call}").into_bytes());
    println!(
        "  >> hostmode C ch31: payload={:?}",
        String::from_utf8_lossy(&connect_frame.payload)
    );
    match modem
        .send_command_best_effort_ack(connect_frame, Duration::from_secs(3))
        .await?
    {
        Some(resp) => println!(
            "  C ACKed: ch={} code=0x{:02x} payload={:?}",
            resp.channel,
            resp.code,
            String::from_utf8_lossy(&resp.payload)
        ),
        None => println!("  C sent (no ACK within 3s, counter advanced)"),
    }
    let deadline = Instant::now() + duration;
    let mut attempt = 0;

    while Instant::now() < deadline {
        attempt += 1;
        let frame = HostmodeFrame::command(PACTOR_CHANNEL, b"L".to_vec());
        match modem.hostmode_transaction(frame).await {
            Ok(response) => {
                println!(
                    "  L poll {attempt}: ch={} code=0x{:02x} payload_hex={:02x?} payload_ascii={:?}",
                    response.channel,
                    response.code,
                    response.payload,
                    String::from_utf8_lossy(&response.payload)
                );
            }
            Err(err) => println!("  L poll {attempt}: error {err}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
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
    command_timeout: Duration,
    frequency: Option<f64>,
    trx_baud: Option<u32>,
    trx_addr: Option<&str>,
    listen: bool,
) -> anyhow::Result<UsbPactorTransport> {
    let mut serial = open_serial(port, baud)?;

    // Drain any leftover data in the serial buffer
    println!("  draining ...");
    drain_serial(&mut serial).await;

    // === Step 1: Exit any existing hostmode ===
    // Send an ASCII "JHOST0\r" — if the modem is in terminal mode this is
    // just an unrecognized command (harmless); if it somehow ended up in
    // hostmode the ASCII text won't be a valid CRC frame but the modem
    // typically exits hostmode on any non-framed input after a timeout.
    // We avoid sending CRC-framed JHOST0 because that would consume a
    // packet-counter slot and desynchronize the transport later.
    println!("  step 1: exit any existing hostmode ...");
    serial.write_all(b"JHOST0\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
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

    // The answering modem must be in listen mode to accept an incoming PACTOR
    // connect request; without it the originator's `C <CALL>` never links.
    if listen {
        println!("  enabling listen mode (LISTEN 1) ...");
        send_ascii(&mut serial, "LISTEN 1").await?;
    }

    // === Step 2b: TRX CI-V frequency control ===
    if let Some(frequency) = frequency {
        println!("  step 2b: TRX frequency control ...");

        // Query the modem's current TRX config (type, baud, CI-V address)
        send_ascii(&mut serial, "TRX TYpe").await?;

        // Only override TRX settings if the user explicitly requested it.
        // Require both baud and addr together to avoid hardcoding either value.
        match (trx_baud, trx_addr) {
            (Some(baud_override), Some(addr_override)) => {
                println!("  overriding TRX config: baud={baud_override} addr=${addr_override}");
                send_ascii(
                    &mut serial,
                    &format!("TRX TYpe I {baud_override} ${addr_override}"),
                )
                .await?;
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(anyhow::anyhow!(
                    "TRX override on {port}: --trx-baud and --trx-addr must be specified together \
                     (the SCS TRX TYpe command requires both baud and address)"
                ));
            }
            (None, None) => { /* use modem's stored config */ }
        }

        // Tune the radio to the requested frequency.
        // A successful tune returns "*** TRX FREQUENCY CHANGED".
        // If CI-V is dead (cable disconnected, radio off, wrong address), the
        // modem returns just "cmd: " with no confirmation.
        let set_resp = send_ascii(&mut serial, &format!("TRX Frequency {frequency}")).await?;
        let set_str = String::from_utf8_lossy(&set_resp);
        if !set_str.contains("FREQUENCY CHANGED") {
            return Err(anyhow::anyhow!(
                "TRX Frequency set failed on {port} — radio did not confirm tune.\n  \
                 Is the CI-V cable connected? Is the radio powered on?\n  \
                 modem response: {set_str}"
            ));
        }
        println!("  TRX frequency confirmed on {port}");
    } else {
        println!("  step 2b: TRX skipped (no --frequency given)");
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

    // === Step 4: Wrap in transport and verify via hostmode_transaction ===
    // The transport handles packet counter (reset bit on first frame, toggling
    // on subsequent frames). We skip raw verification to avoid desynchronizing
    // the counter — let the transport's first transaction be the verification.
    println!("  step 4: verifying hostmode via transport ...");
    let mut config = UsbPactorConfig::new(port);
    config.command_timeout = command_timeout;
    let transport = UsbPactorTransport::from_stream(serial, config);

    for attempt in 1..=5 {
        println!("  verify attempt {attempt}/5 ...");
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
    let command_timeout = Duration::from_secs(args.connect_timeout_secs);
    let freq_a = args.frequency_a.or(args.frequency);
    let trx_baud_a = args.trx_baud_a.or(args.trx_baud);
    let trx_addr_a = args.trx_addr_a.as_deref().or(args.trx_addr.as_deref());
    let modem_a = init_hostmode(
        &args.port_a,
        args.baud,
        &args.call_a,
        command_timeout,
        freq_a,
        trx_baud_a,
        trx_addr_a,
        false,
    )
    .await?;

    println!("Initializing modem B on {} ...", args.port_b);
    let freq_b = args.frequency_b.or(args.frequency);
    let trx_baud_b = args.trx_baud_b.or(args.trx_baud);
    let trx_addr_b = args.trx_addr_b.as_deref().or(args.trx_addr.as_deref());
    let modem_b = init_hostmode(
        &args.port_b,
        args.baud,
        &args.call_b,
        command_timeout,
        freq_b,
        trx_baud_b,
        trx_addr_b,
        true,
    )
    .await?;

    println!(
        "Callsigns configured during terminal init: A={}, B={}",
        args.call_a, args.call_b
    );

    if args.diagnose_connect {
        diagnose_connect(
            &modem_a,
            &args.call_b,
            Duration::from_secs(args.connect_timeout_secs),
            args.diagnose_connect_reset,
        )
        .await?;
        return Ok(());
    }

    println!("Modem A connecting to {} ...", args.call_b);
    if let Some(freq) = args.frequency {
        println!(
            "  (radios tuned to {} kHz via TRX — timeout={}s)",
            freq, args.connect_timeout_secs
        );
    } else {
        println!(
            "  (TRX tuning skipped — timeout={}s)",
            args.connect_timeout_secs
        );
    }
    println!("  Tip: use --diagnose-connect to watch L poll status during connect");
    let link_start = Instant::now();
    match modem_a.connect_peer(&args.call_b).await {
        Ok(()) => {}
        Err(e) => {
            let elapsed = link_start.elapsed();
            eprintln!("connect_peer failed after {elapsed:.1?}: {e}");
            eprintln!();
            eprintln!("This usually means the two modems cannot hear each other.");
            eprintln!("Check that:");
            eprintln!("  - Both radios responded to TRX Frequency (look for cmd echo above)");
            eprintln!("  - Antennas are connected and band conditions allow the link");
            eprintln!();
            eprintln!("To diagnose, re-run with: --diagnose-connect");
            return Err(e.into());
        }
    }
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
