use futures_util::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task;
use tokio::time;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use std::sync::Arc;
use std::env;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Waypoint {
    latitude: f64,
    longitude: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NavigationParams {
    destination: Waypoint,
    speed: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IncomingMessage {
    msg_type: i32,
    timestamp_usec: i64,
    assigned_id: Vec<String>,
    lat_deg: Vec<f64>,
    lon_deg: Vec<f64>,
    alt_hae_m: Vec<f64>,
    formation_azimuth_deg: f64,
    intervehicle_spacing_m: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct StatusUpdate {
    msg_type: i32,
    timestamp_usec: i64,
    src_id: String,
    group_id: Vec<String>,
    lat_deg: f64,
    lon_deg: f64,
    alt_hae_m: f64,
    heading_deg: f64,
    groundspeed_mps: f64,
    cam_azimuth_deg: f64,
    state_of_control: i32,
    mission_phase: i32,
    phase_loc: PhaseLocation,
    cam_yaw_deg: f64,
    cam_roll_deg: f64,
    cam_pitch_deg: f64,
    cam_hfov: f64,
    cam_vfov: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PhaseLocation {
    r#type: String,
    coordinates: Vec<Vec<[f64; 2]>>,
}

fn haversine_distance(start: &Waypoint, end: &Waypoint) -> f64 {
    let r = 6371e3;
    let lat1 = start.latitude.to_radians();
    let lat2 = end.latitude.to_radians();
    let delta_lat = (end.latitude - start.latitude).to_radians();
    let delta_lon = (end.longitude - start.longitude).to_radians();
    let a =
        (delta_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    r * c
}

async fn handle_websocket(api_url: &str, start: Waypoint) {
    let (ws_stream, _) = connect_async(api_url).await.expect("Failed to connect to WebSocket server");
    let (mut write, mut read) = ws_stream.split();

    while let Some(Ok(Message::Text(msg))) = read.next().await {
        let incoming: IncomingMessage = serde_json::from_str(&msg).expect("Failed to parse incoming message");
        let params = NavigationParams {
            destination: Waypoint {
                latitude: incoming.lat_deg[0],
                longitude: incoming.lon_deg[0],
            },
            speed: incoming.intervehicle_spacing_m,
        };

        let total_distance = haversine_distance(&start, &params.destination);
        let total_time = total_distance / params.speed;
        let start_time = Instant::now();

        let mut current_position = start.clone();
        let mut time_elapsed = 0.0;

        while time_elapsed < total_time {
            time::sleep(Duration::from_secs(1)).await;
            time_elapsed = start_time.elapsed().as_secs_f64();

            let progress = time_elapsed / total_time;
            current_position.latitude = start.latitude + progress * (params.destination.latitude - start.latitude);
            current_position.longitude = start.longitude + progress * (params.destination.longitude - start.longitude);

            let status = StatusUpdate {
                msg_type: 3,
                timestamp_usec: 1724437355000000,
                src_id: "agent_1".to_string(),
                group_id: vec!["group_5".to_string()],
                lat_deg: current_position.latitude,
                lon_deg: current_position.longitude,
                alt_hae_m: 0.0,
                heading_deg: 0.0,
                groundspeed_mps: params.speed,
                cam_azimuth_deg: 0.0,
                state_of_control: 0,
                mission_phase: 2,
                phase_loc: PhaseLocation {
                    r#type: "Polygon".to_string(),
                    coordinates: vec![],
                },
                cam_yaw_deg: 0.0,
                cam_roll_deg: 0.0,
                cam_pitch_deg: 0.0,
                cam_hfov: 0.0,
                cam_vfov: 0.0,
            };

            let msg = serde_json::to_string(&status).expect("Failed to serialize status update");
            if let Err(e) = write.send(Message::Text(msg)).await {
                eprintln!("Failed to send status update: {:?}, stopping updates.", e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <start_latitude> <start_longitude> <websocket_url>", args[0]);
        return;
    }

    let start_latitude: f64 = args[1].parse().expect("Invalid latitude");
    let start_longitude: f64 = args[2].parse().expect("Invalid longitude");
    let websocket_url = &args[3];
    let start = Waypoint { latitude: start_latitude, longitude: start_longitude };

    handle_websocket(websocket_url, start).await;
}

