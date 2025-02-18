use futures_util::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use std::env;
use std::time::{Duration, Instant};
use tokio::time;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

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

async fn receive_navigation_params(ws_url: &str) -> NavigationParams {
    let (ws_stream, _) = connect_async(ws_url)
        .await
        .expect("Failed to connect to WebSocket server");
    let (_, mut read) = ws_stream.split();

    if let Some(Ok(Message::Text(msg))) = read.next().await {
        let incoming: IncomingMessage =
            serde_json::from_str(&msg).expect("Failed to parse incoming message");
        return NavigationParams {
            destination: Waypoint {
                latitude: incoming.lat_deg[0],
                longitude: incoming.lon_deg[0],
            },
            speed: incoming.intervehicle_spacing_m,
        };
    }
    panic!("Failed to receive navigation parameters");
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

async fn start_navigation(start: Waypoint, params: NavigationParams, status_ws_url: &str) {
    let (ws_stream, _) = connect_async(status_ws_url)
        .await
        .expect("Failed to connect to WebSocket server");
    let (mut write, _) = ws_stream.split();

    let total_distance = haversine_distance(&start, &params.destination);
    let total_time = total_distance / params.speed;
    let start_time = Instant::now();

    let mut current_position = start.clone();
    let mut time_elapsed = 0.0;

    while time_elapsed < total_time {
        time::sleep(Duration::from_secs(1)).await;
        time_elapsed = start_time.elapsed().as_secs_f64();

        let progress = time_elapsed / total_time;
        current_position.latitude =
            start.latitude + progress * (params.destination.latitude - start.latitude);
        current_position.longitude =
            start.longitude + progress * (params.destination.longitude - start.longitude);

        // let status = StatusUpdate {
        //     current_position: current_position.clone(),
        //     speed: params.speed,
        //     time_remaining: total_time - time_elapsed,
        // };
        let status = StatusUpdate {
            msg_type: 3,
            timestamp_usec: 1724437355000000,
            src_id: "cod_wap".to_string(),
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

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <start_latitude> <start_longitude>", args[0]);
        return;
    }

    let start_latitude: f64 = args[1].parse().expect("Invalid latitude");
    let start_longitude: f64 = args[2].parse().expect("Invalid longitude");
    let start = Waypoint {
        latitude: start_latitude,
        longitude: start_longitude,
    };

    let ws_receive_url = "ws://localhost:9000/receive";
    let ws_send_url = "ws://localhost:9001/send";

    let params = receive_navigation_params(ws_receive_url).await;
    start_navigation(start, params, ws_send_url).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn test_websocket_connection() {
        let listener = TcpListener::bind("127.0.0.1:9000").await.unwrap();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws_stream = accept_async(stream).await.unwrap();
            let test_message = r#"{
                "msg_type": 6,
                "timestamp_usec": 1724437355000000,
                "assigned_id": ["cod_wap"],
                "lat_deg": [34.124453],
                "lon_deg": [-119.277961],
                "alt_hae_m": [60.0],
                "formation_azimuth_deg": 45.0,
                "intervehicle_spacing_m": 10.0
            }"#;
            ws_stream
                .send(Message::Text(test_message.to_string()))
                .await
                .unwrap();
        });

        let params = receive_navigation_params("ws://127.0.0.1:9000").await;
        assert_eq!(params.destination.latitude, 34.124453);
        assert_eq!(params.destination.longitude, -119.277961);
        assert_eq!(params.speed, 10.0);

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_navigation_updates() {
        let listener = TcpListener::bind("127.0.0.1:9001").await.unwrap();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws_stream = accept_async(stream).await.unwrap();

            let mut received_messages = 0;
            while let Some(Ok(Message::Text(msg))) =
                time::timeout(Duration::from_secs(10), ws_stream.next())
                    .await
                    .ok()
                    .flatten()
            {
                let status: StatusUpdate = serde_json::from_str(&msg).unwrap();
                assert!(status.lat_deg > 37.0);
                assert!(status.lon_deg < -118.0);

                received_messages += 1;
                if received_messages >= 5 {
                    // Stop after receiving a few updates
                    break;
                }
            }
        });

        let params = NavigationParams {
            destination: Waypoint {
                latitude: 34.0522,
                longitude: -118.2437,
            },
            speed: 50.0,
        };
        let start = Waypoint {
            latitude: 37.7749,
            longitude: -122.4194,
        };
        start_navigation(start, params, "ws://127.0.0.1:9001").await;
        server_task.await.unwrap();
    }
}
