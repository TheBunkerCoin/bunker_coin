//! radio simulation for BunkerCoin

use anyhow::Result;
use bunker_coin_sim::{SimulationPresets, scenarios};
use colored::Colorize;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    println!("{}", "BunkerCoin Radio Simulation".bright_blue().bold());
    println!("{}", "=========================".bright_blue());
    println!();
    
    let test_scenarios = vec![
        ("Good Conditions", SimulationPresets::good_conditions()),
        ("Average Conditions", SimulationPresets::average_conditions()),
        ("Poor Conditions", SimulationPresets::poor_conditions()),
    ];
    
    for (name, config) in test_scenarios {
        println!("{}", format!("\n>>> Testing: {}", name).bright_green().bold());
        println!("Bandwidth: {} bps", config.bandwidth_bps);
        println!("Packet Loss: {}%", (config.packet_loss * 100.0) as u32);
        println!("Latency: {:?}", config.latency);
        println!();
        
        scenarios::basic_consensus_test(config.clone(), 4).await;
        
        scenarios::bandwidth_test(config).await;
        
        println!("{}", "Test complete!".bright_yellow());
        println!("{}", "-".repeat(50));
    }
    
    println!("{}", "\n>>> Testing: Extreme Conditions (Solar Storm)".bright_red().bold());
    let extreme = SimulationPresets::extreme_conditions();
    println!("Bandwidth: {} bps", extreme.bandwidth_bps);
    println!("Packet Loss: {}%", (extreme.packet_loss * 100.0) as u32);
    println!("Latency: {:?}", extreme.latency);
    println!("\nNote: This simulates severe ionospheric disturbance");
    
    scenarios::basic_consensus_test(extreme, 4).await;
    
    println!("\n{}", "All simulations complete!".bright_green().bold());
    println!("\n{}", "Key Findings:".bright_yellow());
    println!("- Shortwave radio constraints significantly impact throughput");
    println!("- 32:96 erasure coding provides resilience against packet loss");
    println!("- 300-byte MTU requires extensive fragmentation of blockchain data");
    println!("- Full consensus would require careful optimization for radio constraints");
    
    Ok(())
} 