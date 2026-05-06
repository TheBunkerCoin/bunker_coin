//! PACTOR-backed radio protocol demo.

use bunker_coin_sim::scenarios;
use scs_pactor::SimulatedPactorConfig;

#[tokio::main]
async fn main() -> Result<(), scs_pactor::ScsPactorError> {
    env_logger::init();

    let mut config = SimulatedPactorConfig::good_link();
    config.latency = std::time::Duration::from_millis(100);
    config.latency_jitter = std::time::Duration::from_millis(25);
    config.setup_delay = std::time::Duration::from_millis(250);

    let result = scenarios::pactor_radio_proto_demo(config).await?;

    println!("PACTOR radio-proto demo complete");
    println!("received messages: {:?}", result.received_messages);
    println!("frames attempted: {}", result.frames_attempted);
    println!("frames lost: {}", result.frames_lost);
    println!("retransmissions: {}", result.retransmissions);
    println!("bytes delivered: {}", result.bytes_delivered);

    let comparison = scenarios::pactor_radio_proto_degradation_demo().await?;
    println!();
    println!("clean vs degraded PACTOR radio-proto:");
    println!(
        "clean: attempts={}, lost={}, retransmissions={}, bytes={}",
        comparison.clean.frames_attempted,
        comparison.clean.frames_lost,
        comparison.clean.retransmissions,
        comparison.clean.bytes_delivered
    );
    println!(
        "degraded: attempts={}, lost={}, retransmissions={}, bytes={}",
        comparison.degraded.frames_attempted,
        comparison.degraded.frames_lost,
        comparison.degraded.retransmissions,
        comparison.degraded.bytes_delivered
    );

    Ok(())
}
