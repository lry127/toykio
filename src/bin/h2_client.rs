use http::Request;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tcp = TcpStream::connect("127.0.0.1:5928").await?;
    let (mut send_req, conn) = h2::client::handshake(tcp).await?;

    let conn_handle = tokio::spawn(async move {
        let x = conn.await;
        println!("conn {x:?}");
    });
    let req = Request::builder().method("get").uri("/hello").body(())?;
    let (resp, _) = send_req.send_request(req, true)?;
    match resp.await {
        Ok(data) => {
            println!("{data:?}")
        }
        Err(err) => {
            println!("stream {err:?}")
        }
    }
    conn_handle.await.ok();
    Ok(())
}
