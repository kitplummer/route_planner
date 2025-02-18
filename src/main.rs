use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message, WebSocketStream};
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
struct Waypoint {
    latitude: f64,
    longitude: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RouteCommand {
    destination: Waypoint,
    speed: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RouteUpdate {
    current_position: Waypoint,
    remaining_distance: f64,
    estimated_time: f64,
}

struct RoutePlanner {
    c2_socket: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
    agent_socket: Option<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
}
impl RoutePlanner {
    async fn new(c2_url: &str, agent_url: &str) -> Result<Self> {
        println!("Planner: Waiting before attempting to connect...");
        sleep(Duration::from_secs(2)).await;

        println!("Planner: Connecting to C2 and Agent WebSocket servers");
        let (c2_socket, _) = connect_async(Url::parse(c2_url)?).await?;
        let (agent_socket, _) = connect_async(Url::parse(agent_url)?).await?;
        println!("Planner: Successfully connected to WebSocket servers");
        Ok(Self {
            c2_socket: Some(c2_socket),
            agent_socket: Some(agent_socket),
        })
    }

    async fn run(&mut self) -> Result<()> {
        println!("Planner: Entering message loop");
        while let Some(socket) = self.c2_socket.as_mut() {
            if let Some(msg) = socket.next().await {
                let msg = msg?;
                match msg {
                    Message::Text(text) => {
                        let command: RouteCommand = serde_json::from_str(&text)?;
                        let update = RouteUpdate {
                            current_position: Waypoint {
                                latitude: 0.0,
                                longitude: 0.0,
                            },
                            remaining_distance: 100.0,
                            estimated_time: 100.0 / command.speed,
                        };
                        let update_json = serde_json::to_string(&update)?;
                        if let Some(agent) = self.agent_socket.as_mut() {
                            match agent.send(Message::Text(update_json)).await {
                                Ok(_) => println!("Planner successfully sent update"),
                                Err(e) => {
                                    println!("Planner: Warning - Failed to send update: {}", e)
                                }
                            }
                        }
                    }
                    Message::Close(_) => {
                        println!(
                            "Planner: Received close from C2, ensuring Agent is closed first..."
                        );
                        if let Some(mut agent) = self.agent_socket.take() {
                            if agent.close(None).await.is_err() {
                                println!("Planner: Warning - Agent was already closed");
                            } else {
                                println!("Planner: Successfully closed Agent connection");
                            }
                        }
                        if let Some(mut c2) = self.c2_socket.take() {
                            if c2.close(None).await.is_err() {
                                println!("Planner: Warning - C2 was already closed");
                            } else {
                                println!("Planner: Successfully closed C2 connection");
                            }
                        }
                        println!("Planner: WebSocket shutdown completed");
                        return Ok(());
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;
    use tokio_tungstenite::accept_async;

    async fn setup_test_server(port: u16) -> Result<TcpListener> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
        println!("Test server listening on port {}", port);
        Ok(listener)
    }

    #[tokio::test]
    async fn test_route_planning() -> Result<()> {
        let c2_listener = setup_test_server(8081).await?;
        let agent_listener = setup_test_server(8082).await?;

        let c2_handle = tokio::spawn(async move {
            let (socket, _) = c2_listener.accept().await?;
            let mut ws_stream = accept_async(socket).await?;
            println!("C2 server successfully accepted WebSocket connection");
            ws_stream
                .send(Message::Text(
                    "{\"destination\": {\"latitude\": 1.0, \"longitude\": 1.0}, \"speed\": 50.0}"
                        .to_string(),
                ))
                .await?;
            ws_stream.flush().await?;
            ws_stream.send(Message::Close(None)).await?;
            ws_stream.flush().await?;
            Ok::<_, anyhow::Error>(())
        });

        let agent_handle = tokio::spawn(async move {
            let (socket, _) = agent_listener.accept().await?;
            let mut ws_stream = accept_async(socket).await?;
            println!("Agent server successfully accepted WebSocket connection");
            while let Some(msg) = ws_stream.next().await {
                match msg? {
                    Message::Text(text) => {
                        println!("Agent received update: {}", text);
                    }
                    Message::Close(_) => {
                        println!("Agent: Closing connection");
                        ws_stream.close(None).await.ok();
                        break;
                    }
                    _ => {}
                }
            }
            Ok::<_, anyhow::Error>(())
        });

        let planner_handle = tokio::spawn(async move {
            let mut planner =
                RoutePlanner::new("ws://localhost:8081", "ws://localhost:8082").await?;
            planner.run().await
        });

        let results = tokio::join!(c2_handle, agent_handle, planner_handle);
        results.0??;
        results.1??;
        results.2??;

        Ok(())
    }
}
