//! Radio transmission scheduler for 5-minute windows

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, sleep, sleep_until};
use log::{debug, info};

use crate::{RadioFrame, RadioConfig};

pub struct RadioScheduler {
    config: RadioConfig,
    frame_queue: Arc<Mutex<VecDeque<RadioFrame>>>,
    stats: Arc<Mutex<SchedulerStats>>,
}

#[derive(Debug, Default)]
struct SchedulerStats {
    windows_completed: u64,
    frames_transmitted: u64,
    frames_queued: u64,
}

impl RadioScheduler {
    pub fn new(config: RadioConfig) -> Self {
        Self {
            config,
            frame_queue: Arc::new(Mutex::new(VecDeque::new())),
            stats: Arc::new(Mutex::new(SchedulerStats::default())),
        }
    }
    
    pub async fn queue_frames(&self, frames: Vec<RadioFrame>) {
        let mut queue = self.frame_queue.lock().await;
        let mut stats = self.stats.lock().await;
        
        for frame in frames {
            queue.push_back(frame);
            stats.frames_queued += 1;
        }
        
        debug!("Queued {} frames for transmission", stats.frames_queued);
    }
    
    pub async fn run<F>(&self, mut transmit_fn: F) 
    where
        F: FnMut(&RadioFrame) -> bool + Send,
    {
        let window_duration = self.config.transmission_window;
        let mut next_window = Instant::now() + window_duration;
        
        loop {
            info!("Starting transmission window");
            let window_start = Instant::now();
            
            let frames_to_send = {
                let mut queue = self.frame_queue.lock().await;
                let frames: Vec<_> = queue.drain(..).collect();
                frames
            };
            
            let mut transmitted = 0;
            for frame in frames_to_send {
                if Instant::now() > next_window - Duration::from_secs(10) {
                    self.queue_frames(vec![frame]).await;
                    break;
                }
                
                if transmit_fn(&frame) {
                    transmitted += 1;
                    
                    let frame_size = bincode::serde::encode_to_vec(&frame, bincode::config::standard())
                        .unwrap()
                        .len();
                    let transmit_time = Duration::from_secs_f64(
                        (frame_size * 8) as f64 / self.config.bandwidth_bps as f64
                    );
                    sleep(transmit_time).await;
                } else {
                    self.queue_frames(vec![frame]).await;
                }
            }
            
            {
                let mut stats = self.stats.lock().await;
                stats.frames_transmitted += transmitted;
                stats.windows_completed += 1;
            }
            
            info!(
                "Transmission window complete: {} frames sent in {:?}",
                transmitted,
                window_start.elapsed()
            );
            
            sleep_until(next_window).await;
            next_window += window_duration;
        }
    }
    
    pub async fn get_stats(&self) -> (u64, u64, u64) {
        let stats = self.stats.lock().await;
        (stats.windows_completed, stats.frames_transmitted, stats.frames_queued)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_scheduler_queuing() {
        let config = RadioConfig::default();
        let scheduler = RadioScheduler::new(config);
        
        let frames = vec![
            RadioFrame {
                slot: 1,
                shred_index: 0,
                frame_index: 0,
                total_frames: 1,
                data: vec![0u8; 100],
            },
            RadioFrame {
                slot: 1,
                shred_index: 0,
                frame_index: 1,
                total_frames: 1,
                data: vec![1u8; 100],
            },
        ];
        
        scheduler.queue_frames(frames).await;
        
        let (_, _, queued) = scheduler.get_stats().await;
        assert_eq!(queued, 2);
    }
} 