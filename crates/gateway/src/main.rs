use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use rand::Rng;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};

use bunker_coin_radio::RadioConfig;

#[derive(Clone)]
struct Gateway {
    nodes: Arc<RwLock<HashMap<u64, mpsc::Sender<Vec<u8>>>>>,
    packet_queue: mpsc::Sender<RadioPacket>,
    config: RadioConfig,
    queue_depth: Arc<AtomicU64>,
}

#[derive(Debug)]
struct RadioPacket {
    from: u64,
    to: u64,
    payload: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let config = RadioConfig::default();

    let (packet_queue, packet_rx) = mpsc::channel(100000);
    let queue_depth = Arc::new(AtomicU64::new(0));
    let nodes = Arc::new(RwLock::new(HashMap::new()));

    let gateway = Arc::new(Gateway {
        nodes,
        packet_queue,
        config,
        queue_depth,
    });

    tokio::spawn(process_packets(packet_rx, gateway.clone()));

    let app = Router::new()
        .route("/", get(root_handler))
        .route("/ws", get(ws_handler))
        .with_state(gateway.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = TcpListener::bind(addr).await?;
    info!("Gateway listening on {}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn process_packets(
    mut packet_rx: mpsc::Receiver<RadioPacket>,
    gateway: Arc<Gateway>,
) {
    while let Some(packet) = packet_rx.recv().await {
        gateway.queue_depth.fetch_sub(1, Ordering::Relaxed);
        let size = packet.payload.len();
        if size > gateway.config.mtu {
            warn!("Packet too large: {} bytes > MTU {}", size, gateway.config.mtu);
            continue;
        }

        let trans_time = Duration::from_secs_f64((size * 8) as f64 / gateway.config.bandwidth_bps as f64);
        let jitter_ms = gateway.config.latency_jitter.as_millis() as f64;
        let jitter = if jitter_ms > 0.0 {
            let mut rng = rand::rng();
            let jitter_factor = rng.random_range(-1.0..1.0);
            Duration::from_millis((jitter_factor * jitter_ms).abs() as u64)
        } else {
            Duration::ZERO
        };
        let total_delay = trans_time + jitter + Duration::from_millis(10);

        tokio::time::sleep(total_delay).await;

        let base_loss = gateway.config.packet_loss as f64;
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
            info!("Packet dropped with prob {:.3}", loss_prob);
            continue;
        }

        let guard = gateway.nodes.read().await;
        if packet.to == u64::MAX {
            for (&id, tx) in guard.iter() {
                if id != packet.from {
                    let _ = tx.send(packet.payload.clone()).await;
                }
            }
        } else {
            if let Some(tx) = guard.get(&packet.to) {
                let _ = tx.send(packet.payload.clone()).await;
            } else {
                warn!("Target node {} not found", packet.to);
            }
        }
    }
}

async fn root_handler(State(gateway): State<Arc<Gateway>>) -> Html<String> {
    let depth = gateway.queue_depth.load(Ordering::Relaxed);
    let connected = gateway.nodes.read().await.len();
    let html = format!(r#"
        <html>
        <head>
            <title>BunkerCoin Radio Gateway</title>
            <style>
                body {{ font-family: monospace; background: #0a0a0a; color: #00ff00; padding: 20px; }}
                pre {{ font-size: 14px; line-height: 1.6; }}
                .header {{ color: #ff9900; font-weight: bold; }}
                .metric {{ color: #00ff00; }}
                .dim {{ color: #666; }}
            </style>
        </head>
        <body>
        <pre class="header">BunkerCoin Radio Gateway</pre>
        <pre>
<span class="dim">Connected Nodes:</span> <span class="metric">{connected}</span>
<span class="dim">Enqueued Packets:</span> <span class="metric">{depth}</span>

<span class="dim">WebSocket Endpoint:</span> <span class="metric">wss://gateway.bunkercoin.com/ws</span>
        </pre>
        </body>
        </html>
        "#);
    Html(html)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(gateway): State<Arc<Gateway>>,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, gateway))
}

async fn handle_socket(socket: WebSocket, gateway: Arc<Gateway>) {
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(tokio::sync::Mutex::new(sender));
    
    let id = {
        let sender_clone = sender.clone();
        let registration_result = handle_registration(&mut receiver, sender_clone, &gateway).await;
        match registration_result {
            Some(id) => id,
            None => return,
        }
    };

    info!("Node {} registered", id);

    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1000);
    gateway.nodes.write().await.insert(id, tx);
    
    let sender_clone = sender.clone();
    tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            let mut sender_guard = sender_clone.lock().await;
            if sender_guard.send(WsMessage::Binary(payload)).await.is_err() {
                break;
            }
        }
    });

    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(WsMessage::Binary(data)) => {
                let config = bincode::config::standard();
                match bincode::serde::decode_from_slice::<(String, Vec<u8>), _>(&data, config) {
                    Ok(((to_str, payload), _)) => {
                        let to = if to_str.eq_ignore_ascii_case("BROADCAST") {
                            u64::MAX
                        } else {
                            match to_str.parse::<u64>() {
                                Ok(t) => t,
                                Err(_) => {
                                    warn!("Invalid target: {}", to_str);
                                    continue;
                                }
                            }
                        };
                        let packet = RadioPacket { from: id, to, payload };
                        gateway.queue_depth.fetch_add(1, Ordering::Relaxed);
                        if let Err(e) = gateway.packet_queue.send(packet).await {
                            gateway.queue_depth.fetch_sub(1, Ordering::Relaxed);
                            error!("Queue send error: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Decode error: {}", e);
                    }
                }
            }
            Ok(WsMessage::Close(_)) => break,
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    gateway.nodes.write().await.remove(&id);
    info!("Node {} disconnected", id);
}

async fn handle_registration(
    receiver: &mut futures_util::stream::SplitStream<WebSocket>,
    sender: Arc<tokio::sync::Mutex<futures_util::stream::SplitSink<WebSocket, WsMessage>>>,
    gateway: &Arc<Gateway>,
) -> Option<u64> {
    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(WsMessage::Text(s)) if s.starts_with("REGISTER ") => {
                if let Ok(parsed_id) = s[9..].parse::<u64>() {
                    let guard = gateway.nodes.read().await;
                    if guard.contains_key(&parsed_id) {
                        let mut sender_guard = sender.lock().await;
                        let _ = sender_guard.send(WsMessage::Text("ID_ALREADY_REGISTERED".to_string())).await;
                        return None;
                    }
                    drop(guard);
                    
                    let mut sender_guard = sender.lock().await;
                    let _ = sender_guard.send(WsMessage::Text("REGISTERED".to_string())).await;
                    return Some(parsed_id);
                } else {
                    let mut sender_guard = sender.lock().await;
                    let _ = sender_guard.send(WsMessage::Text("INVALID_ID".to_string())).await;
                }
            }
            Ok(_) => {
                let mut sender_guard = sender.lock().await;
                let _ = sender_guard.send(WsMessage::Text("EXPECT_REGISTER".to_string())).await;
            }
            Err(e) => {
                error!("WebSocket error during registration: {}", e);
                return None;
            }
        }
    }
    None
}
