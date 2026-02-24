//! Radio network core for routing messages between nodes with realistic HF radio constraints

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, mpsc};
use alpenglow::network::{Network, NetworkError, NetworkMessage};
use alpenglow::ValidatorId;
use rand::Rng;
use log::{debug, trace, warn};
use serde::{Serialize, Deserialize};
use reed_solomon_erasure::galois_8::ReedSolomon;
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
    payload: Arc<Vec<u8>>,
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
    stats: Arc<NetworkStats>,
    packet_queue: Sender<RadioPacket>,
    queue_depth: Arc<AtomicU64>,
}

#[derive(Debug)]
struct NetworkStats {
    packets_sent: AtomicU64,
    packets_dropped: AtomicU64,
    packets_transmitted: AtomicU64,
    bytes_transmitted: AtomicU64,
}

impl Default for NetworkStats {
    fn default() -> Self {
        Self {
            packets_sent: AtomicU64::new(0),
            packets_dropped: AtomicU64::new(0),
            packets_transmitted: AtomicU64::new(0),
            bytes_transmitted: AtomicU64::new(0),
        }
    }
}

fn bursty_packet_loss(base_loss: f64) -> Option<f64> {
    let r: f64 = rand::rng().random();
    let dynamic_factor = if r < 0.7 {
        0.8 + (r / 0.7) * 0.4
    } else if r < 0.95 {
        1.2 + ((r - 0.7) / 0.25) * 0.6
    } else {
        2.0 + ((r - 0.95) / 0.05) * 1.0
    };
    let loss_prob = base_loss * dynamic_factor;
    if rand::rng().random::<f64>() < loss_prob {
        Some(loss_prob)
    } else {
        None
    }
}

impl RadioNetworkCore {
    pub fn new(config: RadioConfig) -> Arc<Self> {
        let nodes = Arc::new(RwLock::new(HashMap::<ValidatorId, mpsc::Sender<RadioPacket>>::new()));
        let stats = Arc::new(NetworkStats::default());
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
                let jitter_ms = config_clone.latency_jitter.as_millis() as f64;
                let random_jitter = if jitter_ms > 0.0 {
                    let mut rng = rand::rng();
                    let jitter_factor = rng.random_range(-1.0..1.0);
                    Duration::from_millis((jitter_factor * jitter_ms) as u64)
                } else {
                    Duration::ZERO
                };
                let transmission_time = Duration::from_secs_f64(
                    (packet.payload.len() * 8) as f64 / config_clone.bandwidth_bps as f64
                );
                let total_delay = Duration::from_millis(10) + random_jitter + transmission_time;
                packets_processed += 1;

                if packets_processed % 100 == 0 {
                    log::info!("Radio queue processed {} packets", packets_processed);
                }

                let base_loss = config_clone.packet_loss as f64;

                if packet.to == u64::MAX {
                    log::trace!("Processing broadcast packet from {} (payload: {} bytes), sleeping {:?}", packet.from, packet.payload.len(), total_delay);
                    tokio::time::sleep(total_delay).await;

                    if let Some(loss_prob) = bursty_packet_loss(base_loss) {
                        stats_clone.packets_dropped.fetch_add(1, Ordering::Relaxed);
                        log::debug!("Radio broadcast dropped during transmission (p={:.3})", loss_prob);
                        continue;
                    }

                    let nodes = nodes_clone.read().await;
                    let recipients: Vec<(ValidatorId, mpsc::Sender<RadioPacket>)> = nodes
                        .iter()
                        .filter(|(id, _)| **id != packet.from)
                        .map(|(id, tx)| (*id, tx.clone()))
                        .collect();
                    drop(nodes);

                    stats_clone.packets_transmitted.fetch_add(1, Ordering::Relaxed);
                    stats_clone.bytes_transmitted.fetch_add(packet.payload.len() as u64, Ordering::Relaxed);

                    let deliver_at = Instant::now() + config_clone.latency;
                    for (to_id, channel) in recipients {
                        let delivery_packet = RadioPacket {
                            from: packet.from,
                            to: to_id,
                            payload: Arc::clone(&packet.payload),
                            deliver_at,
                        };

                        match channel.try_send(delivery_packet) {
                            Ok(_) => {
                                log::trace!("broadcast from {} delivered to node {}", packet.from, to_id);
                            }
                            Err(e) => {
                                log::warn!("failed to deliver broadcast to node {}: {}", to_id, e);
                            }
                        }
                    }
                } else {
                    log::trace!("dequeued packet from {} to {} (payload: {} bytes), sleeping {:?}", packet.from, packet.to, packet.payload.len(), total_delay);
                    tokio::time::sleep(total_delay).await;

                    if let Some(loss_prob) = bursty_packet_loss(base_loss) {
                        stats_clone.packets_dropped.fetch_add(1, Ordering::Relaxed);
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
                                stats_clone.packets_transmitted.fetch_add(1, Ordering::Relaxed);
                                stats_clone.bytes_transmitted.fetch_add(packet_size as u64, Ordering::Relaxed);
                            }
                            Err(e) => {
                                log::warn!("Failed to deliver packet to node {}: channel full or closed: {}", to_node, e);
                            }
                        }
                    } else {
                        log::warn!("Node {} not found in network, dropping packet", to_node);
                    }
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

    async fn broadcast_packet(&self, from: ValidatorId, payload: Vec<u8>) -> Result<(), RadioError> {
        if payload.len() > self.config.mtu {
            return Err(RadioError::PacketTooLarge);
        }

        let packet = RadioPacket {
            from,
            to: u64::MAX,
            payload: Arc::new(payload),
            deliver_at: Instant::now(),
        };

        log::trace!("Attempting to enqueue broadcast packet from {}", from);

        match self.packet_queue.try_send(packet) {
            Ok(_) => {
                log::trace!("Successfully enqueued broadcast packet from {}", from);
                self.queue_depth.fetch_add(1, Ordering::Relaxed);
                self.stats.packets_sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                match e {
                    mpsc::error::TrySendError::Full(_) => {
                        log::error!("Radio packet queue is FULL! Cannot send broadcast from {}. Consider increasing queue size.", from);
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        log::error!("Radio packet queue is closed! Cannot send broadcast from {}", from);
                    }
                }
                Err(RadioError::TransmissionFailed)
            }
        }
    }

    async fn send_packet(&self, from: ValidatorId, to: ValidatorId, payload: Vec<u8>) -> Result<(), RadioError> {
        if payload.len() > self.config.mtu {
            return Err(RadioError::PacketTooLarge);
        }
        let packet = RadioPacket {
            from,
            to,
            payload: Arc::new(payload),
            deliver_at: Instant::now(),
        };
        log::trace!("Attempting to enqueue packet from {} to {}", from, to);

        match self.packet_queue.try_send(packet) {
            Ok(_) => {
                log::trace!("Successfully enqueued packet from {} to {}", from, to);
                self.queue_depth.fetch_add(1, Ordering::Relaxed);
                self.stats.packets_sent.fetch_add(1, Ordering::Relaxed);
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
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        (
            self.stats.packets_sent.load(Ordering::Relaxed),
            self.stats.packets_dropped.load(Ordering::Relaxed),
            self.stats.packets_transmitted.load(Ordering::Relaxed),
            self.stats.bytes_transmitted.load(Ordering::Relaxed),
            queue_depth,
        )
    }
}

pub struct RadioNode {
    id: ValidatorId,
    network_core: Arc<RadioNetworkCore>,
    receiver: Mutex<mpsc::Receiver<RadioPacket>>,
    message_counter: AtomicU64,
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
            message_counter: AtomicU64::new(0),
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
            let message_id = self.message_counter.fetch_add(1, Ordering::Relaxed);
            
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
            
            let message_id = self.message_counter.fetch_add(1, Ordering::Relaxed);
            
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
            let estimated_size: usize = state.fragments.values().map(|f| f.len()).sum();
            let mut complete_message = Vec::with_capacity(estimated_size);
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

        let message_id = self.message_counter.fetch_add(1, Ordering::Relaxed);

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
    
    async fn broadcast_radio(&self, data: &[u8]) -> Result<(), NetworkError> {
        log::debug!("RadioNode {} broadcasting {} bytes to all nodes", self.id, data.len());
        
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
            log::warn!("Message too large for erasure coding with MTU {}, falling back to fragmented broadcast", mtu);
            return Err(NetworkError::Unknown);
        }

        let total_data_len = DATA_SHARDS * shard_payload_size;
        let mut padded = vec![0u8; total_data_len];
        padded[..data.len()].copy_from_slice(data);

        let mut shards: Vec<Vec<u8>> = padded
            .chunks(shard_payload_size)
            .map(|c| c.to_vec())
            .collect();
        shards.resize(TOTAL_SHARDS, vec![0u8; shard_payload_size]);

        let mut shard_refs: Vec<&mut [u8]> = shards.iter_mut().map(|v| &mut v[..]).collect();
        let r = ReedSolomon::new(DATA_SHARDS, PARITY_SHARDS).unwrap();
        r.encode(&mut shard_refs).unwrap();

        let message_id = self.message_counter.fetch_add(1, Ordering::Relaxed);

        for (i, shard) in shard_refs.iter().enumerate() {
            let header = ShardHeader {
                message_id,
                shard_index: i as u8,
                total_shards: TOTAL_SHARDS as u8,
                data_len: data.len() as u32,
            };
            let mut packet = bincode::serde::encode_to_vec(&header, bincode::config::standard()).unwrap();
            packet.extend_from_slice(shard);
            
            log::trace!("Broadcasting shard {}/{} for message {}, packet size: {} bytes", 
                       i+1, TOTAL_SHARDS, message_id, packet.len());
            
            self.network_core.broadcast_packet(self.id, packet).await
                .map_err(|_| NetworkError::Unknown)?;
        }
        
        Ok(())
    }
}

impl Network for RadioNode {
    type Address = String;

    async fn send(&self, msg: &NetworkMessage, to: impl AsRef<str> + Send) -> Result<(), NetworkError> {
        let to_str = to.as_ref();
        let data = msg.to_bytes();
        
        log::debug!("RadioNode.send() called with to='{}', checking if broadcast", to_str);
        
        if to_str.eq_ignore_ascii_case("BROADCAST") {
            log::debug!("RadioNode {} BROADCAST DETECTED! Broadcasting message ({} bytes)", self.id, data.len());
            trace!("RadioNode {} broadcasting message ({} bytes)", self.id, data.len());
            self.broadcast_radio(&data).await
        } else {
            let to_id: ValidatorId = to_str.parse()
                .map_err(|_| NetworkError::Unknown)?;
            log::debug!("RadioNode {} unicast to {} ({} bytes)", self.id, to_id, data.len());
            trace!("RadioNode {} sending {} bytes to {}", self.id, data.len(), to_id);
            self.send_radio(&data, to_id).await
        }
    }

    async fn send_serialized(&self, bytes: &[u8], to: impl AsRef<str> + Send) -> Result<(), NetworkError> {
        let to_str = to.as_ref();
        
        if to_str.eq_ignore_ascii_case("BROADCAST") {
            trace!("RadioNode {} broadcasting {} serialized bytes", self.id, bytes.len());
            self.broadcast_radio(bytes).await
        } else {
            let to_id: ValidatorId = to_str.parse()
                .map_err(|_| NetworkError::Unknown)?;
            trace!("RadioNode {} sending {} serialized bytes to {}", self.id, bytes.len(), to_id);
            self.send_radio(bytes, to_id).await
        }
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
                    if packet.from != self.id {
                        // println!("RadioNode {} received broadcast from {} (reassembled message)", self.id, packet.from);
                    }
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