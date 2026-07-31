//! End-to-end heartbeat smoke: `HeartbeatClient` from `kaas-broker`
//! connects to `HeartbeatService` from `kaas-controller` over a real
//! tonic TCP socket; the client's outbound `BrokerStatus` registers
//! the broker on the server, and the server's `ASSIGNMENT_CHANGED`
//! push reaches the client's `on_command` handler.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions
)]

use std::sync::Arc;
use std::time::Duration;

use kaas_broker::coordinator::HeartbeatSource;
use kaas_broker::heartbeatpb::controller_command::Type as CmdType;
use kaas_broker::heartbeatpb::controller_heartbeat_server::ControllerHeartbeatServer;
use kaas_broker::HeartbeatClient;
use kaas_controller::{HeartbeatServer, HeartbeatService};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_receives_assignment_changed_pushed_by_server() {
    // 1. Bind the controller-side gRPC server on a free port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // tonic re-binds via incoming
    let server = HeartbeatServer::new();
    let svc = HeartbeatService::new(server.clone());

    let serve_cancel = CancellationToken::new();
    let serve_cancel_c = serve_cancel.clone();
    let serve_task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ControllerHeartbeatServer::new(svc))
            .serve_with_shutdown(addr, async move {
                serve_cancel_c.cancelled().await;
            })
            .await;
    });

    // Wait for the server to become reachable.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("tonic server never bound on {addr}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 2. Spawn the broker-side client pointing at the server.
    let client = HeartbeatClient::new("kaas-0");
    client.set_target(addr.to_string());

    // Capture commands the client receives.
    let captured = Arc::new(Mutex::new(Vec::<i32>::new()));
    let captured_c = captured.clone();
    client.on_command(Arc::new(move |cmd| {
        captured_c.lock().push(cmd.r#type);
    }));

    let client_cancel = CancellationToken::new();
    let client_clone = client.clone();
    let client_cancel_c = client_cancel.clone();
    let client_task = tokio::spawn(async move {
        client_clone.run(client_cancel_c).await;
    });

    // Wait for the client's first BrokerStatus to register on the
    // server.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while server.connected_brokers().is_empty() {
        if std::time::Instant::now() > deadline {
            panic!(
                "client never registered with the server; \
                 connected_brokers={:?}",
                server.connected_brokers()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(server.connected_brokers(), vec!["kaas-0".to_owned()]);

    // 3. Push an ASSIGNMENT_CHANGED from the controller; the client
    // must observe it via on_command.
    server.push_assignment_changed(42);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let observed = captured.lock().clone();
        if observed.contains(&(CmdType::AssignmentChanged as i32)) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("client never received ASSIGNMENT_CHANGED; observed={observed:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 4. Self-fence: client.last_received() is set.
    assert!(
        client.last_received().is_some(),
        "self-fence path must observe a heartbeat"
    );

    // Shutdown.
    client_cancel.cancel();
    let _ = client_task.await;
    serve_cancel.cancel();
    let _ = serve_task.await;
}
