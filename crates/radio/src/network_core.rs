//! Radio network core for routing messages between nodes with realistic HF radio constraints

use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, mpsc};
use alpenglow::network::{Network, NetworkError, NetworkMessage};
use alpenglow::ValidatorId;
use rand::Rng;
use log::{debug, trace, warn};
use serde::{Serialize, Deserialize};
use reed_solomon_erasure::galois_8::{ReedSolomon, ShardByShard};
use tokio::sync::mpsc::{Sender, Receiver};


use crate::{RadioConfig, RadioError};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FragmentHeader {
    message_id: u64,
    fragment_index: u16,
    total_fragments: u16,
}

struct RadioPacket {
    from: ValidatorId,
    to: ValidatorId,
    payload: Vec<u8>,
    deliver_at: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShardHeader {
    message_id: u64,
    shard_index: u8,
    total_shards: u8,
    data_len: u32,
}

impl Ord for RadioPacket {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.deliver_at.cmp(&self.deliver_at)
    }
}
impl PartialOrd for RadioPacket {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for RadioPacket {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at
    }
}
impl Eq for RadioPacket {}

pub struct RadioNetworkCore {
    nodes: Arc<RwLock<HashMap<ValidatorId, mpsc::Sender<RadioPacket>>>>,
    config: RadioConfig,
    stats: Arc<Mutex<NetworkStats>>,
    packet_queue: Sender<RadioPacket>,
    queue_depth: Arc<AtomicU64>,
}

#[derive(Debug, Default)]
struct NetworkStats {
    packets_sent: u64,
    packets_dropped: u64,
    packets_transmitted: u64,
    bytes_transmitted: u64,
    packets_queued: u64,
}

impl RadioNetworkCore {
    pub fn new(config: RadioConfig) -> Arc<Self> {
        let nodes = Arc::new(RwLock::new(HashMap::<ValidatorId, mpsc::Sender<RadioPacket>>::new()));
        let stats = Arc::new(Mutex::new(NetworkStats::default()));
        let queue_depth = Arc::new(AtomicU64::new(0));
        let (packet_queue, mut packet_rx): (Sender<RadioPacket>, Receiver<RadioPacket>) = mpsc::channel(100000);
        let nodes_clone = nodes.clone();
        let stats_clone = stats.clone();
        let config_clone = config.clone();
        let queue_depth_clone = queue_depth.clone();
        
        log::info!("RadioNetworkCore initialized with config: {:?}", config);
        log::info!("Queue buffer size: 100,000 packets");
        
        tokio::spawn(async move {
            log::info!("Radio packet processing task started");
            let mut packets_processed = 0u64;
            while let Some(packet) = packet_rx.recv().await {
                queue_depth_clone.fetch_sub(1, Ordering::Relaxed);
                let mtu = config_clone.mtu;
                let _header_size = 32;
                let jitter_ms = config_clone.latency_jitter.as_millis() as f64;
                let random_jitter = if jitter_ms > 0.0 {
                    use rand::Rng;
                    let mut rng = rand::rng();
                    let jitter_factor = rng.random_range(-1.0..1.0);
                    Duration::from_millis((jitter_factor * jitter_ms) as u64)
                } else {
                    Duration::ZERO
                };
                let transmission_time = Duration::from_secs_f64(
                    (packet.payload.len() * 8) as f64 / config_clone.bandwidth_bps as f64
                );
                let processing_delay = Duration::from_millis(10) + random_jitter;
                let total_delay = transmission_time + processing_delay;
                packets_processed += 1;
                
                if packets_processed % 100 == 0 {
                    log::info!("Radio queue processed {} packets", packets_processed);
                }
                
                log::trace!("Dequeued packet from {} to {} (payload: {} bytes), sleeping {:?}", packet.from, packet.to, packet.payload.len(), total_delay);
                tokio::time::sleep(total_delay).await;
                
                let base_loss = config_clone.packet_loss as f64;
                let r: f64 = rand::rng().random();
                
                // "bursty" packet loss model
                let dynamic_factor = if r < 0.7 {
                    0.8 + (r / 0.7) * 0.4 
                } else if r < 0.95 {
                    1.2 + ((r - 0.7) / 0.25) * 0.6 
                } else {
                    2.0 + ((r - 0.95) / 0.05) * 1.0
                };
                
                let loss_prob = base_loss * dynamic_factor;
                if rand::rng().random::<f64>() < loss_prob {
                    let mut stats = stats_clone.lock().await;
                    stats.packets_dropped += 1;
                    log::debug!("Radio packet dropped during transmission (p={:.3})", loss_prob);
                    continue;
                }
                
                let n_guard = nodes_clone.read().await;
                let to_node = packet.to;
                let packet_size = packet.payload.len();
                if let Some(channel) = n_guard.get(&to_node) {
                    match channel.try_send(packet) {
                        Ok(_) => {
                            log::trace!("Packet delivered to node {}", to_node);
                            let mut stats = stats_clone.lock().await;
                            stats.packets_transmitted += 1;
                            stats.bytes_transmitted += packet_size as u64;
                        }
                        Err(e) => {
                            log::warn!("Failed to deliver packet to node {}: channel full or closed: {}", to_node, e);
                        }
                    }
                } else {
                    log::warn!("Node {} not found in network, dropping packet", to_node);
                }
            }
        });
        Arc::new(Self {
            nodes,
            config,
            stats,
            packet_queue,
            queue_depth,
        })
    }

    pub async fn join(self: &Arc<Self>, id: ValidatorId) -> RadioNode {
        let (tx, rx) = mpsc::channel(1000);
        self.nodes.write().await.insert(id, tx);
        RadioNode::new(id, Arc::clone(self), rx)
    }

    async fn send_packet(&self, from: ValidatorId, to: ValidatorId, payload: Vec<u8>) -> Result<(), RadioError> {
        if payload.len() > self.config.mtu {
            return Err(RadioError::PacketTooLarge);
        }
        let packet = RadioPacket {
            from,
            to,
            payload,
            deliver_at: Instant::now(),
        };
        log::trace!("Attempting to enqueue packet from {} to {}", from, to);
        
        match self.packet_queue.try_send(packet) {
            Ok(_) => {
                log::trace!("Successfully enqueued packet from {} to {}", from, to);
                self.queue_depth.fetch_add(1, Ordering::Relaxed);
                let mut stats = self.stats.lock().await;
                stats.packets_sent += 1;
                Ok(())
            }
            Err(e) => {
                match e {
                    mpsc::error::TrySendError::Full(_) => {
                        log::error!("Radio packet queue is FULL! Cannot send packet from {} to {}. Consider increasing queue size.", from, to);
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        log::error!("Radio packet queue is closed! Cannot send packet from {} to {}", from, to);
                    }
                }
                Err(RadioError::TransmissionFailed)
            }
        }
    }

    pub async fn get_stats(&self) -> (u64, u64, u64, u64, u64) {
        let stats = self.stats.lock().await;
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        (stats.packets_sent, stats.packets_dropped, stats.packets_transmitted, stats.bytes_transmitted, queue_depth)
    }
}

pub struct RadioNode {
    id: ValidatorId,
    network_core: Arc<RadioNetworkCore>,
    receiver: Mutex<mpsc::Receiver<RadioPacket>>,
    message_counter: Mutex<u64>,
    reassembly: Mutex<HashMap<(ValidatorId, u64), ReassemblyState>>,
}

struct ReassemblyState {
    fragments: HashMap<u16, Vec<u8>>,
    total_fragments: u16,
    last_seen: Instant,
}

impl RadioNode {
    fn new(id: ValidatorId, network_core: Arc<RadioNetworkCore>, receiver: mpsc::Receiver<RadioPacket>) -> Self {
        Self {
            id,
            network_core,
            receiver: Mutex::new(receiver),
            message_counter: Mutex::new(0),
            reassembly: Mutex::new(HashMap::new()),
        }
    }

    async fn send_fragmented(&self, data: &[u8], to: ValidatorId) -> Result<(), NetworkError> {
        let mtu = self.network_core.config.mtu;
        
        let header_size = bincode::serde::encode_to_vec(
            &FragmentHeader { message_id: 0, fragment_index: 0, total_fragments: 1 },
            bincode::config::standard()
        ).unwrap().len();
        
        let effective_mtu = mtu.saturating_sub(header_size);
        
        if data.len() <= effective_mtu {
            let message_id = {
                let mut counter = self.message_counter.lock().await;
                let id = *counter;
                *counter += 1;
                id
            };
            
            let header = FragmentHeader {
                message_id,
                fragment_index: 0,
                total_fragments: 1,
            };
            
            let mut packet = bincode::serde::encode_to_vec(&header, bincode::config::standard()).unwrap();
            packet.extend_from_slice(data);
            
            self.network_core.send_packet(self.id, to, packet).await
                .map_err(|_| NetworkError::Unknown)?;
        } else {
            let chunks: Vec<_> = data.chunks(effective_mtu).collect();
            let total_fragments = chunks.len() as u16;
            
            let message_id = {
                let mut counter = self.message_counter.lock().await;
                let id = *counter;
                *counter += 1;
                id
            };
            
            for (index, chunk) in chunks.iter().enumerate() {
                let header = FragmentHeader {
                    message_id,
                    fragment_index: index as u16,
                    total_fragments,
                };
                
                let mut packet = bincode::serde::encode_to_vec(&header, bincode::config::standard()).unwrap();
                packet.extend_from_slice(chunk);
                
                self.network_core.send_packet(self.id, to, packet).await
                    .map_err(|_| NetworkError::Unknown)?;
            }
        }
        
        Ok(())
    }
    
    async fn try_reassemble(&self, from: ValidatorId, header: FragmentHeader, data: Vec<u8>) -> Option<Vec<u8>> {
        let mut reassembly = self.reassembly.lock().await;
        
        let now = Instant::now();
        reassembly.retain(|_, state| now.duration_since(state.last_seen) < Duration::from_secs(30));
        
        if header.total_fragments == 1 {
            return Some(data);
        }
        
        let key = (from, header.message_id);
        let state = reassembly.entry(key).or_insert_with(|| ReassemblyState {
            fragments: HashMap::new(),
            total_fragments: header.total_fragments,
            last_seen: now,
        });
        
        state.last_seen = now;
        state.fragments.insert(header.fragment_index, data);
        
        if state.fragments.len() == state.total_fragments as usize {
            let mut complete_message = Vec::new();
            for i in 0..state.total_fragments {
                if let Some(fragment) = state.fragments.get(&i) {
                    complete_message.extend_from_slice(fragment);
                } else {
                    warn!("Missing fragment {} for message from {}", i, from);
                    return None;
                }
            }
            reassembly.remove(&key);
            Some(complete_message)
        } else {
            None
        }
    }

    async fn send_with_erasure(&self, data: &[u8], to: ValidatorId) -> Result<(), NetworkError> {
        
        // for now 2:6 due to empty blocks
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 4;
        const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
        const MIN_SHARD_SIZE: usize = 32;
        
        let mtu = self.network_core.config.mtu;
        let header_size = bincode::serde::encode_to_vec(
            &ShardHeader { message_id: 0, shard_index: 0, total_shards: TOTAL_SHARDS as u8, data_len: data.len() as u32 },
            bincode::config::standard()
        ).unwrap().len();
        
        let max_shard_payload_size = mtu.saturating_sub(header_size);
        let min_total_size = DATA_SHARDS * MIN_SHARD_SIZE;
        let required_total_size = data.len().max(min_total_size);
        let shard_payload_size = ((required_total_size + DATA_SHARDS - 1) / DATA_SHARDS).max(MIN_SHARD_SIZE);
        
        if shard_payload_size > max_shard_payload_size {
            log::warn!("Message too large for erasure coding with MTU {}, falling back to fragmentation", mtu);
            return self.send_fragmented(data, to).await;
        }

        let total_data_len = DATA_SHARDS * shard_payload_size;
        let mut padded = vec![0u8; total_data_len];
        padded[..data.len()].copy_from_slice(data);

        let mut shards: Vec<Vec<u8>> = padded
            .chunks(shard_payload_size)
            .map(|c| c.to_vec())
            .collect();
        assert_eq!(shards.len(), DATA_SHARDS, "Data shards length mismatch");
        shards.resize(TOTAL_SHARDS, vec![0u8; shard_payload_size]);

        let mut shard_refs: Vec<&mut [u8]> = shards.iter_mut().map(|v| &mut v[..]).collect();
        let r = ReedSolomon::new(DATA_SHARDS, PARITY_SHARDS).unwrap();
        r.encode(&mut shard_refs).unwrap();
        let total_transmitted = TOTAL_SHARDS * (shard_payload_size + header_size);
        let overhead_factor = total_transmitted as f64 / data.len() as f64;
        log::info!("Encoded message into {} shards ({} data, {} parity), original: {} bytes, per shard: {} bytes, total transmitted: {} bytes ({:.1}x overhead)", 
                  TOTAL_SHARDS, DATA_SHARDS, PARITY_SHARDS, data.len(), shard_payload_size, total_transmitted, overhead_factor);

        let message_id = {
            let mut counter = self.message_counter.lock().await;
            let id = *counter;
            *counter += 1;
            id
        };

        for (i, shard) in shard_refs.iter().enumerate() {
            let header = ShardHeader {
                message_id,
                shard_index: i as u8,
                total_shards: TOTAL_SHARDS as u8,
                data_len: data.len() as u32,
            };
            let mut packet = bincode::serde::encode_to_vec(&header, bincode::config::standard()).unwrap();
            packet.extend_from_slice(shard);
            log::trace!("Sending shard {}/{} for message {} to {}, packet size: {} bytes", 
                       i+1, TOTAL_SHARDS, message_id, to, packet.len());
            self.network_core.send_packet(self.id, to, packet).await
                .map_err(|_| NetworkError::Unknown)?;
        }
        Ok(())
    }

    async fn try_reassemble_erasure(&self, from: ValidatorId, header: ShardHeader, data: Vec<u8>) -> Option<Vec<u8>> {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 4;
        const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
        let key = (from, header.message_id);
        let mut reassembly = self.reassembly.lock().await;
        let now = Instant::now();
        reassembly.retain(|k, state| {
            if now.duration_since(state.last_seen) >= Duration::from_secs(120) {
                log::warn!("Dropping incomplete message from {} (id: {}), timed out after 120s, received {}/{} shards", k.0, k.1, state.fragments.len(), state.total_fragments);
                false
            } else {
                true
            }
        });
        let state = reassembly.entry(key).or_insert_with(|| ReassemblyState {
            fragments: HashMap::new(),
            total_fragments: header.total_shards as u16,
            last_seen: now,
        });
        state.last_seen = now;
        state.fragments.insert(header.shard_index as u16, data);
        log::trace!("Received shard {}/{} for message {} from {}, total received: {}", 
                   header.shard_index + 1, state.total_fragments, header.message_id, from, state.fragments.len());
        
        if state.fragments.len() >= DATA_SHARDS {
            log::info!("Attempting to reconstruct message {} from {} with {}/{} shards", 
                      header.message_id, from, state.fragments.len(), TOTAL_SHARDS);
            
            let mut shards: Vec<Option<Vec<u8>>> = vec![None; TOTAL_SHARDS];
            let mut shard_size = 0;
            for (&idx, frag) in &state.fragments {
                if (idx as usize) < TOTAL_SHARDS {
                    shard_size = frag.len();
                    shards[idx as usize] = Some(frag.clone());
                }
            }
            let r = ReedSolomon::new(DATA_SHARDS, PARITY_SHARDS).unwrap();
            match r.reconstruct(&mut shards) {
                Ok(()) => {
                    let mut data = Vec::with_capacity(DATA_SHARDS * shard_size);
                    for i in 0..DATA_SHARDS {
                        if let Some(ref shard) = shards[i] {
                            data.extend_from_slice(shard);
                        } else {
                            log::error!("Missing data shard {} after successful reconstruction for message {} from {}", i, header.message_id, from);
                            return None;
                        }
                    }
                    data.truncate(header.data_len as usize);
                    log::info!("Successfully reconstructed message {} from {} with {}/{} shards", header.message_id, from, state.fragments.len(), TOTAL_SHARDS);
                    reassembly.remove(&key);
                    return Some(data);
                }
                Err(e) => {
                    log::debug!("Failed to reconstruct message {} from {} with {}/{} shards: {:?}", header.message_id, from, state.fragments.len(), TOTAL_SHARDS, e);
                }
            }
        } else {
            log::trace!("Need {} more shards to attempt reconstruction for message {} from {}", 
                       DATA_SHARDS.saturating_sub(state.fragments.len()), header.message_id, from);
        }
        None
    }

    async fn send_radio(&self, data: &[u8], to: ValidatorId) -> Result<(), NetworkError> {
        log::debug!("RadioNode {} sending {} bytes to {} using erasure coding", self.id, data.len(), to);
        self.send_with_erasure(data, to).await
    }
}

impl Network for RadioNode {
    type Address = String;

    async fn send(&self, msg: &NetworkMessage, to: impl AsRef<str> + Send) -> Result<(), NetworkError> {
        let to_id: ValidatorId = to.as_ref().parse()
            .map_err(|_| NetworkError::Unknown)?;
        let data = msg.to_bytes();
        trace!("RadioNode {} sending {} bytes to {}", self.id, data.len(), to_id);
        self.send_radio(&data, to_id).await
    }

    async fn send_serialized(&self, bytes: &[u8], to: impl AsRef<str> + Send) -> Result<(), NetworkError> {
        let to_id: ValidatorId = to.as_ref().parse()
            .map_err(|_| NetworkError::Unknown)?;
        trace!("RadioNode {} sending {} serialized bytes to {}", self.id, bytes.len(), to_id);
        self.send_radio(bytes, to_id).await
    }

    async fn receive(&self) -> Result<NetworkMessage, NetworkError> {
        loop {
            let packet = self.receiver.lock().await.recv().await
                .ok_or(NetworkError::Unknown)?;
            if let Ok((header, header_len)) = bincode::serde::decode_from_slice::<ShardHeader, _>(
                &packet.payload,
                bincode::config::standard()
            ) {
                let data = packet.payload[header_len..].to_vec();
                if let Some(complete) = self.try_reassemble_erasure(packet.from, header, data).await {
                    match NetworkMessage::from_bytes(&complete) {
                        Ok(msg) => return Ok(msg),
                        Err(_) => {
                            warn!("Failed to decode complete erasure-coded message");
                            continue;
                        }
                    }
                }
                continue;
            }
            let (header, header_len) = match bincode::serde::decode_from_slice::<FragmentHeader, _>(
                &packet.payload,
                bincode::config::standard()
            ) {
                Ok((h, header_len)) => (h, header_len),
                Err(_) => {
                    warn!("Failed to decode fragment header");
                    continue;
                }
            };
            let data = packet.payload[header_len..].to_vec();
            if let Some(complete_message) = self.try_reassemble(packet.from, header, data).await {
                match NetworkMessage::from_bytes(&complete_message) {
                    Ok(msg) => return Ok(msg),
                    Err(_) => {
                        warn!("Failed to decode complete message");
                        continue;
                    }
                }
            }
        }
    }
} 