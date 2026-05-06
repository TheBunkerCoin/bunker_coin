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
    println!("received: {:?}", result.received_message);
    println!("frames attempted: {}", result.frames_attempted);
    println!("frames lost: {}", result.frames_lost);
    println!("retransmissions: {}", result.retransmissions);
    println!("bytes delivered: {}", result.bytes_delivered);

    Ok(())
}
