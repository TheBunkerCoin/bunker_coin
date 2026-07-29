//! Hardware PACTOR radio-proto smoke test over two USB SCS modems.
//!
//! `--frequency` tunes via TRX CI-V, but PACTOR still requires a real RF path.

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
    /// Serial port for modem A.
    #[arg(long)]
    port_a: String,

    /// Serial port for modem B.
    #[arg(long)]
    port_b: String,

    /// Callsign for modem A.
    #[arg(long, default_value = "CLIENT")]
    call_a: String,

    /// Callsign for modem B.
    #[arg(long, default_value = "NODE")]
    call_b: String,

    /// Serial baud rate; SCS Dragon DR-7400 defaults to 829440.
    #[arg(long, default_value_t = 829_440)]
    baud: u32,

    /// Link-establishment timeout in seconds.
    #[arg(long, default_value_t = 90)]
    connect_timeout_secs: u64,

    /// Connect attempts before giving up.
    #[arg(long, default_value_t = 3)]
    connect_attempts: u32,

    /// Clear stale link/call state during init.
    #[arg(long)]
    reset: bool,

    /// Exchange Alpenglow consensus messages over the PACTOR network wrapper.
    #[arg(long)]
    consensus_smoke: bool,

    /// Also exercise reverse B -> A traffic via ARQ changeover.
    #[arg(long)]
    bidirectional: bool,

    /// Consensus message rounds to exchange.
    #[arg(long, default_value_t = 1)]
    rounds: u32,

    /// Messages per transmit turn, amortizing half-duplex turnaround.
    #[arg(long, default_value_t = 1)]
    batch: u32,

    /// Stop after sending C <CALL> and print raw L status polls.
    #[arg(long)]
    diagnose_connect: bool,

    /// In diagnose-connect mode, send C <CALL> with the hostmode reset bit.
    #[arg(long)]
    diagnose_connect_reset: bool,

    /// Radio frequency in kHz; omitted skips TRX tuning.
    #[arg(long)]
    frequency: Option<f64>,

    /// Modem A frequency override in kHz.
    #[arg(long)]
    frequency_a: Option<f64>,

    /// Modem B frequency override in kHz.
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

/// Drains pending serial bytes while printing hex and ASCII.
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

/// Reads all modem bytes until timeout.
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

/// Sends a terminal-mode ASCII command and reads its response.
async fn send_ascii(serial: &mut tokio_serial::SerialStream, cmd: &str) -> anyhow::Result<Vec<u8>> {
    println!("  >> {cmd}");
    serial.write_all(cmd.as_bytes()).await?;
    serial.write_all(b"\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    Ok(read_all(serial, 800).await)
}

/// Verifies hostmode through the wrapped transport.
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
    // Reset the post-init packet counter so connect starts at parity 0.
    modem.reset_packet_counter().await;

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

    // Bare CR distinguishes terminal fallback (`cmd:` prompt) from silent hostmode busy/TX.
    println!("  >> raw-probe: sending bare CR to detect terminal-mode fallback ...");
    if let Err(e) = modem.write_raw(b"\r").await {
        println!("  raw-probe write error: {e}");
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("  (if a 'cmd:' prompt appeared above, the modem left hostmode)");

    let deadline = Instant::now() + duration;
    let mut attempt = 0;

    let poll_timeout = Duration::from_secs(5);
    while Instant::now() < deadline {
        attempt += 1;

        // Channel 255 `G` can answer while channel 31 is busy with ARQ connect.
        let g_probe = HostmodeFrame::command(255, b"G".to_vec());
        match modem
            .send_command_best_effort_ack(g_probe, poll_timeout)
            .await
        {
            Ok(Some(resp)) => println!(
                "  poll {attempt}: ch255 G -> ch={} payload_hex={:02x?} ascii={:?}",
                resp.channel,
                resp.payload,
                String::from_utf8_lossy(&resp.payload)
            ),
            Ok(None) => println!("  poll {attempt}: ch255 G -> no response"),
            Err(err) => println!("  poll {attempt}: ch255 G -> error {err}"),
        }

        let frame = HostmodeFrame::command(PACTOR_CHANNEL, b"L".to_vec());
        match modem
            .send_command_best_effort_ack(frame, poll_timeout)
            .await
        {
            Ok(Some(response)) => println!(
                "  poll {attempt}: ch31 L -> ch={} code=0x{:02x} payload_hex={:02x?} ascii={:?}",
                response.channel,
                response.code,
                response.payload,
                String::from_utf8_lossy(&response.payload)
            ),
            Ok(None) => println!("  poll {attempt}: ch31 L -> no response (modem busy with RF)"),
            Err(err) => println!("  poll {attempt}: ch31 L -> error {err}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
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

/// Initializes an SCS modem into JHOST4 CRC hostmode.
async fn init_hostmode(
    port: &str,
    baud: u32,
    callsign: &str,
    command_timeout: Duration,
    frequency: Option<f64>,
    trx_baud: Option<u32>,
    trx_addr: Option<&str>,
    listen: bool,
    reset: bool,
) -> anyhow::Result<UsbPactorTransport> {
    let mut serial = open_serial(port, baud)?;

    println!("  draining ...");
    drain_serial(&mut serial).await;

    // Exit hostmode with terminal text so no packet-counter slot is consumed.
    println!("  step 1: exit any existing hostmode / converse ...");
    // ESC recovers modems left in CONVerse before terminal commands.
    serial.write_all(&[0x1b]).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain_serial(&mut serial).await;
    serial.write_all(b"JHOST0\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    drain_serial(&mut serial).await;

    // Clear stale link state with disconnect, not RESTART, so LISTEN/MYcall survive.
    if reset {
        println!("  step 1b: clearing stale link state (ESC + disconnect) ...");
        // ESC leaves CONVerse before DD force-disconnects lingering state.
        serial.write_all(&[0x1b]).await?;
        serial.flush().await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        drain_serial(&mut serial).await;
        send_ascii(&mut serial, "DD").await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        drain_serial(&mut serial).await;
    }

    // Terminal-mode init follows the ptc-go sequence.
    println!("  step 2: terminal-mode init ...");

    send_ascii(&mut serial, "").await?;

    send_ascii(&mut serial, "Quit").await?;

    let commands = [
        format!("MYcall {callsign}"),
        format!("PTCH {PACTOR_CHANNEL}"),
        "MAXE 35".to_owned(),
        "REM 0".to_owned(),
        "CHOB 0".to_owned(),
        // Ctrl-Z lets the ISS hand the transmit turn to the peer in converse mode.
        "CHO 26".to_owned(),
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

    // The answerer must listen or the originator's `C <CALL>` never links.
    if listen {
        println!("  enabling listen mode (LISTEN 1) ...");
        send_ascii(&mut serial, "LISTEN 1").await?;
    }

    if let Some(frequency) = frequency {
        println!("  step 2b: TRX frequency control ...");

        send_ascii(&mut serial, "TRX TYpe").await?;

        // TRX overrides require baud and CI-V address together.
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

        // Require explicit tune confirmation; bare `cmd:` means CI-V did not respond.
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

    // Enter JHOST4 as a terminal-mode command.
    println!("  step 3: entering JHOST4 ...");
    send_ascii(&mut serial, "JHOST4").await?;

    // Consume the JHOST4 startup banner before wrapping the transport.
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

    // Let the transport perform first verification so packet counters stay aligned.
    println!("  step 4: verifying hostmode via transport ...");
    let mut config = UsbPactorConfig::new(port);
    config.command_timeout = command_timeout;
    // Slow HF ARQ, especially reverse after changeover, needs a long data read window.
    config.read_timeout = Some(Duration::from_secs(180));
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

/// Exchanges Alpenglow consensus messages over both modems.
async fn consensus_smoke_test(
    transport_a: Arc<dyn PactorTransport>,
    transport_b: Arc<dyn PactorTransport>,
    bidirectional: bool,
    rounds: u32,
    batch: u32,
) -> anyhow::Result<()> {
    use bunker_coin_radio::PactorNetwork;
    use bunkerglow::consensus::{ConsensusMessage, Vote};
    use bunkerglow::crypto::aggsig::SecretKey;
    use bunkerglow::network::Network as BgNetwork;
    use bunkerglow::Slot;

    let rounds = rounds.max(1);
    let batch = batch.max(1);
    println!(
        "=== Consensus exchange over PACTOR ({} round(s), batch {}, {}) ===",
        rounds,
        batch,
        if bidirectional {
            "bidirectional A<->B"
        } else {
            "one-way A->B"
        }
    );

    let net_a: PactorNetwork<ConsensusMessage, ConsensusMessage> =
        PactorNetwork::new(Arc::clone(&transport_a));
    let net_b: PactorNetwork<ConsensusMessage, ConsensusMessage> =
        PactorNetwork::new(Arc::clone(&transport_b));

    let sk_a = SecretKey::new(&mut rand::rng());
    let sk_b = SecretKey::new(&mut rand::rng());

    // B enters converse only when reverse traffic is needed.
    let mut b_in_converse = false;

    let exchange_start = Instant::now();
    let mut messages_ok: u32 = 0;
    let mut slot: u64 = 0;

    async fn recv_batch(
        net: &PactorNetwork<ConsensusMessage, ConsensusMessage>,
        sent: &[ConsensusMessage],
        label: &str,
    ) -> anyhow::Result<()> {
        for i in 0..sent.len() {
            let got: ConsensusMessage = net
                .receive()
                .await
                .map_err(|e| anyhow::anyhow!("{label} receive {i} failed: {e}"))?;
            match (&sent[i], &got) {
                (ConsensusMessage::Vote(s), ConsensusMessage::Vote(g)) if s == g => {}
                _ => return Err(anyhow::anyhow!("{label} message {i} mismatch")),
            }
        }
        Ok(())
    }

    for round in 1..=rounds {
        // A is ISS for A->B; B must remain a passive receiver for this leg.
        let a_batch: Vec<ConsensusMessage> = (0..batch)
            .map(|_| {
                slot += 1;
                ConsensusMessage::Vote(Vote::new_skip(Slot::new(slot), &sk_a, 0))
            })
            .collect();
        let t = Instant::now();
        println!("[round {round}] A -> B batch of {batch} vote(s) ...");
        net_a
            .send_batch(&a_batch)
            .await
            .map_err(|e| anyhow::anyhow!("round {round}: A send_batch failed: {e}"))?;
        recv_batch(&net_b, &a_batch, &format!("round {round}: A->B")).await?;
        messages_ok += batch;
        println!(
            "[round {round}]   A -> B {batch} verified ({:.1?}, {:.1?}/msg)",
            t.elapsed(),
            t.elapsed() / batch
        );

        if !bidirectional {
            continue;
        }

        // B->A requires ARQ changeover and one transmit turn.
        if !b_in_converse {
            transport_b
                .accept_incoming(None)
                .await
                .map_err(|e| anyhow::anyhow!("round {round}: B converse failed: {e}"))?;
            b_in_converse = true;
        }
        let t = Instant::now();
        println!("[round {round}] changeover; B -> A batch of {batch} vote(s) ...");
        transport_a
            .changeover()
            .await
            .map_err(|e| anyhow::anyhow!("round {round}: A changeover failed: {e}"))?;
        let b_batch: Vec<ConsensusMessage> = (0..batch)
            .map(|_| {
                slot += 1;
                ConsensusMessage::Vote(Vote::new_skip(Slot::new(slot), &sk_b, 1))
            })
            .collect();
        net_b
            .send_batch(&b_batch)
            .await
            .map_err(|e| anyhow::anyhow!("round {round}: B send_batch failed: {e}"))?;
        recv_batch(&net_a, &b_batch, &format!("round {round}: B->A")).await?;
        messages_ok += batch;
        println!(
            "[round {round}]   B -> A {batch} verified ({:.1?}, {:.1?}/msg)",
            t.elapsed(),
            t.elapsed() / batch
        );

        // Hand the transmit turn back to A before the next round.
        if round < rounds {
            println!("[round {round}] B handing transmit turn back to A (changeover) ...");
            let _ = transport_b.changeover().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    let elapsed = exchange_start.elapsed();
    println!();
    println!("=== Consensus exchange results ===");
    println!("Rounds:           {rounds}");
    println!("Messages OK:      {messages_ok}");
    println!("Total time:       {elapsed:.1?}");
    if messages_ok > 0 {
        println!("Avg per message:  {:.1?}", elapsed / messages_ok);
    }
    println!("Consensus exchange succeeded over PACTOR!");

    println!("Disconnecting ...");
    let _ = transport_a.disconnect().await;
    let _ = transport_b.disconnect().await;
    println!("Done.");
    Ok(())
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
        args.reset,
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
        args.reset,
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
    let mut last_err = None;
    for attempt in 1..=args.connect_attempts {
        println!(
            "Connect attempt {attempt}/{} to {} ...",
            args.connect_attempts, args.call_b
        );
        match modem_a.connect_peer(&args.call_b).await {
            Ok(()) => {
                last_err = None;
                break;
            }
            Err(e) => {
                eprintln!("  attempt {attempt} failed: {e}");
                last_err = Some(e);
                if attempt < args.connect_attempts {
                    // Let the modem settle into standby before retrying.
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
    if let Some(e) = last_err {
        let elapsed = link_start.elapsed();
        eprintln!(
            "connect failed after {elapsed:.1?} ({} attempts): {e}",
            args.connect_attempts
        );
        eprintln!();
        eprintln!("This usually means the two modems cannot hear each other.");
        eprintln!("Check that:");
        eprintln!("  - Both radios responded to TRX Frequency (look for cmd echo above)");
        eprintln!("  - Antennas are connected and band conditions allow the link");
        eprintln!();
        eprintln!("To diagnose, re-run with: --diagnose-connect");
        return Err(e.into());
    }
    let link_elapsed = link_start.elapsed();
    println!("Link established in {link_elapsed:.2?}");

    // Let the answerer settle after CONNECTED before data transfer.
    println!("Settling link before data exchange ...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let transport_a: Arc<dyn PactorTransport> = Arc::new(modem_a);
    let transport_b: Arc<dyn PactorTransport> = Arc::new(modem_b);

    if args.consensus_smoke {
        return consensus_smoke_test(
            transport_a,
            transport_b,
            args.bidirectional,
            args.rounds,
            args.batch,
        )
        .await;
    }

    let node_a = PactorRadioNode::from_shared(&args.call_a, Arc::clone(&transport_a));
    let node_b = PactorRadioNode::from_shared(&args.call_b, Arc::clone(&transport_b));

    let messages = vec![
        NetworkMessage::Ping,
        NetworkMessage::Shred(b"radio-proto-over-pactor-hw".to_vec()),
        NetworkMessage::Pong,
    ];

    // Interleave small sends and receives so marginal ARQ links avoid retry exhaustion.
    println!(
        "Exchanging {} messages A -> B (interleaved) ...",
        messages.len()
    );
    let send_start = Instant::now();
    let recv_start = send_start;
    let mut received = Vec::new();
    for (i, msg) in messages.iter().enumerate() {
        println!("  -> sending message {}/{} ...", i + 1, messages.len());
        node_a.send(msg, &args.call_b).await?;
        println!(
            "  <- waiting for message {}/{} on B ...",
            i + 1,
            messages.len()
        );
        let got = node_b.receive().await?;
        println!("  ok: received message {}/{}", i + 1, messages.len());
        received.push(got);
    }
    let send_elapsed = send_start.elapsed();
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
