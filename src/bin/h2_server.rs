use bytes::Bytes;
use h2::server;
use http::{Response, StatusCode};
use std::future::poll_fn;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};

#[tokio::main]
pub async fn main() {
    let listener = TcpListener::bind("127.0.0.1:5928").await.unwrap();

    // Accept all incoming TCP connections.
    loop {
        if let Ok((socket, _peer_addr)) = listener.accept().await {
            // Spawn a new task to process each connection.
            tokio::spawn(async {
                // Start the HTTP/2 connection handshake
                let mut h2 = server::handshake(socket).await.unwrap();
                // Accept all inbound HTTP/2 streams sent over the
                // connection.
                timeout(Duration::from_secs(20), async move {
                    while let Some(request) = h2.accept().await {
                        let (request, mut respond) = request.unwrap();
                        println!("Received request: {:?}", request);

                        tokio::spawn(async move {
                            let response =
                                Response::builder().status(StatusCode::OK).body(()).unwrap();

                            let mut send_stream = respond.send_response(response, false).unwrap();

                            send_stream.reserve_capacity(100);
                            let res = poll_fn(|cx| send_stream.poll_capacity(cx)).await;
                            println!("{res:?}");

                            // This queues the data...
                            send_stream
                                .send_data(Bytes::from_static(b"abc\n"), true)
                                .ok();

                            // ...and because this is running in its own task, the sleep
                            // no longer blocks the parent `h2.accept().await` loop.
                            // Curl will receive the data instantly.
                            sleep(Duration::from_secs(5)).await;
                        });
                    }
                })
                .await
                .ok();
            });
        }
    }
}
