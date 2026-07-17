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

    /// Number of connect attempts before giving up (HF calls intermittently
    /// abort to standby; a retry usually succeeds).
    #[arg(long, default_value_t = 3)]
    connect_attempts: u32,

    /// Send RESTART to both modems during init to clear stale link/call state.
    #[arg(long)]
    reset: bool,

    /// Consensus smoke test: after connect, send one Alpenglow ConsensusMessage
    /// A -> B over PactorNetwork (the bunkerglow Network impl) and verify it.
    /// First increment toward running the consensus simulation over the modems.
    #[arg(long)]
    consensus_smoke: bool,

    /// In the consensus smoke test, also exercise the reverse B -> A direction
    /// (requires the ARQ changeover). When false, only the proven one-way A -> B
    /// exchange is performed (the morning-working path).
    #[arg(long)]
    bidirectional: bool,

    /// Number of consensus message rounds to exchange in the smoke test. Each
    /// round is one A->B vote (plus a B->A counter-vote when --bidirectional).
    /// Used to characterize sustained throughput/stability over the link.
    #[arg(long, default_value_t = 1)]
    rounds: u32,

    /// Messages per transmit turn (batch size). Each round sends this many votes
    /// in a single transmit direction before any changeover, amortizing the
    /// expensive half-duplex turnaround across the batch.
    #[arg(long, default_value_t = 1)]
    batch: u32,

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
    // The post-init verify advanced the packet-counter toggle; reset it so the
    // connect goes out at parity 0 (the modem expects the connect to be the
    // first parity-0 channel command — matches ptc-go and connect_peer).
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

    // DECISIVE PROBE: is the modem still in hostmode after C, or did it drop to
    // terminal mode? Send a bare CR (raw terminal byte). If the modem replies
    // with a "cmd:" prompt (visible in the reader's "[reader] got" log), it has
    // LEFT hostmode. If it stays silent, it is still in hostmode (busy/TX) and
    // simply not answering polls.
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

        // Probe the extended-poll channel (255). SCS hostmode answers the G
        // poll on 255 even while a data channel is busy with an ARQ connect,
        // so this reveals whether the modem is alive and which channel has
        // activity, vs. the L poll on 31 going silent during connect.
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
    reset: bool,
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
    println!("  step 1: exit any existing hostmode / converse ...");
    // If a prior data session left the modem in CONVerse mode, terminal commands
    // (including JHOST0) are ignored until ESC (0x1B) returns it to command mode.
    // Send ESC first so init reliably recovers a converse-stuck modem.
    serial.write_all(&[0x1b]).await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drain_serial(&mut serial).await;
    serial.write_all(b"JHOST0\r").await?;
    serial.flush().await?;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    drain_serial(&mut serial).await;

    // === Step 1b: Optional clear of stale link/call state ===
    // A modem left in a degraded state after a prior session (e.g. a lingering
    // connect/standby state that makes new calls abort to STBY) is cleared by a
    // force-disconnect. We deliberately do NOT use RESTART here: on this firmware
    // RESTART reboots toward defaults and wipes settings like LISTEN/MYcall, so
    // the answering modem would stop accepting calls. A disconnect drops any
    // stuck link while preserving config (which step 2 re-applies anyway).
    if reset {
        println!("  step 1b: clearing stale link state (ESC + disconnect) ...");
        // If the modem was left in CONVerse mode by a prior data session, plain
        // terminal commands are ignored — ESC (0x1B) returns it to command mode
        // first. Then DD force-disconnects any lingering link. Settings are
        // preserved (re-applied in step 2 regardless).
        serial.write_all(&[0x1b]).await?;
        serial.flush().await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        drain_serial(&mut serial).await;
        send_ascii(&mut serial, "DD").await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        drain_serial(&mut serial).await;
    }

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
        // Set the CHANGEOVER character to Ctrl-Z (26). In converse mode, the ISS
        // (master) hands the transmit turn to the peer when this char is sent —
        // required for the answering side (slave) to transmit back (B -> A).
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
    // ARQ data transfer over a marginal HF link at 200 Bd can take far longer
    // than the 10s default; give received data a generous window to arrive. The
    // reverse (slave -> master) direction after a changeover is notably slower,
    // so allow several minutes.
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

/// First increment of running the consensus simulation over the modems: send a
/// single Alpenglow `ConsensusMessage` from A to B via `PactorNetwork` (the
/// `bunkerglow::network::Network` impl the simulation is generic over) and verify
/// it deserializes correctly on the other side.
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

    // PactorNetwork is point-to-point, so the SocketAddr is ignored; pass a dummy
    // one to satisfy the trait. Keep Arc clones so we can disconnect afterwards.
    let net_a: PactorNetwork<ConsensusMessage, ConsensusMessage> =
        PactorNetwork::new(Arc::clone(&transport_a));
    let net_b: PactorNetwork<ConsensusMessage, ConsensusMessage> =
        PactorNetwork::new(Arc::clone(&transport_b));

    let sk_a = SecretKey::new(&mut rand::rng());
    let sk_b = SecretKey::new(&mut rand::rng());

    // B enters converse mode once, up front, only when we will need the reverse
    // direction. (For one-way A->B, B stays a passive receiver throughout.)
    let mut b_in_converse = false;

    let exchange_start = Instant::now();
    let mut messages_ok: u32 = 0;
    let mut slot: u64 = 0;

    // Receive and verify `count` votes from `net`, all expected to be in `sent`.
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
            // Messages within a turn arrive in order, so compare positionally.
            match (&sent[i], &got) {
                (ConsensusMessage::Vote(s), ConsensusMessage::Vote(g)) if s == g => {}
                _ => return Err(anyhow::anyhow!("{label} message {i} mismatch")),
            }
        }
        Ok(())
    }

    for round in 1..=rounds {
        // --- A -> B (one transmit turn, `batch` votes) ---
        // A is the ISS; B receives as a passive slave. Don't put B in converse
        // before this leg — it disrupts B's reception.
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

        // --- B -> A (ARQ changeover, one transmit turn, `batch` votes) ---
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

        // B held the transmit turn for its batch; A has now received it, so B's
        // send is complete — hand the turn back to A for the next round's A->B.
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
                    // Brief pause before retrying so the modem returns to a
                    // clean standby state before the next call.
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

    // Let both ends settle into the connected/converse state before pushing data.
    // The answering modem needs a moment after CONNECTED before it will reliably
    // carry data over the ARQ link.
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

    // Interleave send -> receive per message. Over a marginal HF ARQ link,
    // bursting all messages then reading risks the link exhausting its retry
    // budget (MAXErr -> STBY) before everything transfers; sending one small
    // payload and confirming it arrived before the next keeps the working set
    // small and makes partial progress visible.
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
