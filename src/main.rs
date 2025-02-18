use futures_util::{stream::StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task;
use tokio::time;
use tokio_tungstenite::{accept_async, connect_async, tungstenite::protocol::Message};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Waypoint {
    latitude: f64,
    longitude: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct NavigationParams {
    start: Waypoint,
    destination: Waypoint,
    speed: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct StatusUpdate {
    current_position: Waypoint,
    speed: f64,
    time_remaining: f64,
}

async fn receive_navigation_params(ws_url: &str) -> NavigationParams {
    let (ws_stream, _) = connect_async(ws_url)
        .await
        .expect("Failed to connect to WebSocket server");
    let (_, mut read) = ws_stream.split();

    if let Some(Ok(Message::Text(msg))) = read.next().await {
        return serde_json::from_str(&msg).expect("Failed to parse navigation parameters");
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

async fn start_navigation(params: NavigationParams, status_ws_url: &str) {
    let (ws_stream, _) = match connect_async(status_ws_url).await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Failed to connect to WebSocket server: {:?}", e);
            return;
        }
    };
    let (mut write, _) = ws_stream.split();

    let total_distance = haversine_distance(&params.start, &params.destination);
    let total_time = total_distance / params.speed;
    let start_time = Instant::now();

    let mut current_position = params.start.clone();
    let mut time_elapsed = 0.0;

    while time_elapsed < total_time {
        time::sleep(Duration::from_secs(1)).await;
        time_elapsed = start_time.elapsed().as_secs_f64();

        let progress = time_elapsed / total_time;
        current_position.latitude = params.start.latitude
            + progress * (params.destination.latitude - params.start.latitude);
        current_position.longitude = params.start.longitude
            + progress * (params.destination.longitude - params.start.longitude);

        let status = StatusUpdate {
            current_position: current_position.clone(),
            speed: params.speed,
            time_remaining: total_time - time_elapsed,
        };

        let msg = serde_json::to_string(&status).expect("Failed to serialize status update");
        if let Err(e) = write.send(Message::Text(msg)).await {
            eprintln!("Failed to send status update: {:?}, stopping updates.", e);
            break; // Stop sending if the connection is broken
        }
    }
}

#[tokio::main]
async fn main() {
    let ws_receive_url = "ws://localhost:9000/receive";
    let ws_send_url = "ws://localhost:9001/send";

    let params = receive_navigation_params(ws_receive_url).await;
    start_navigation(params, ws_send_url).await;
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
            ws_stream.send(Message::Text("{\"start\": {\"latitude\": 37.7749, \"longitude\": -122.4194}, \"destination\": {\"latitude\": 34.0522, \"longitude\": -118.2437}, \"speed\": 50.0}".to_string())).await.unwrap();
        });

        let params = receive_navigation_params("ws://127.0.0.1:9000").await;
        assert_eq!(params.start.latitude, 37.7749);
        assert_eq!(params.start.longitude, -122.4194);
        assert_eq!(params.destination.latitude, 34.0522);
        assert_eq!(params.destination.longitude, -118.2437);
        assert_eq!(params.speed, 50.0);

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
                assert!(status.current_position.latitude > 37.0);
                assert!(status.current_position.longitude < -118.0);
                assert!(status.time_remaining >= 0.0);

                received_messages += 1;
                if received_messages >= 5 {
                    // Stop after receiving a few updates
                    break;
                }
            }
        });

        let params = NavigationParams {
            start: Waypoint {
                latitude: 37.7749,
                longitude: -122.4194,
            },
            destination: Waypoint {
                latitude: 34.0522,
                longitude: -118.2437,
            },
            speed: 50.0,
        };
        start_navigation(params, "ws://127.0.0.1:9001").await;
        server_task.await.unwrap();
    }
}
