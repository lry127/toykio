# Toykio Proxy

Toykio (TOY toKIO) is a toy (but functional) network proxy written to get my hands dirty with rust and net programming with tokio

## Features

- HTTP/2 based multiplexing of proxy connections over a single transport connection.
- Support for both TCP and KCP (UDP-based) transport protocols.
- Automatic reconnection of the multiplexed connection on failure.
- post-quantum mTLS 1.3 from client to proxy server.
- Basic SOCKS5 on client side.

## run!

1. server (listening on 0.0.0.0:1234, default protocol is TCP)
    ```shell
      cargo run --bin run_server -- --cert-path ./certs/server/server.crt --cert-key ./certs/server/server.key --ca-cert ./certs/ca/ca.crt  --listen-addr 127.0.0.1:1234 --shared-secret my_secret
    ```
   To use KCP on the server:
    ```shell
      cargo run --bin run_server -- --cert-path ./certs/server/server.crt --cert-key ./certs/server/server.key --ca-cert ./certs/ca/ca.crt  --listen-addr 127.0.0.1:1234 --shared-secret my_secret --protocol kcp
    ```

2. client (socks5 listening on 127.0.0.1:1080, relaying proxy request to server via a multiplexed connection)
    ```shell
    cargo run --bin run_client -- --cert-path ./certs/client/client.crt --cert-key ./certs/client/client.key --ca-cert ./certs/ca/ca.crt --socks5-addr 127.0.0.1:1080 --remote-addr 127.0.0.1:1234 --shared-secret my_secret
    ```
   To use KCP on the client:
    ```shell
    cargo run --bin run_client -- --cert-path ./certs/client/client.crt --cert-key ./certs/client/client.key --ca-cert ./certs/ca/ca.crt --socks5-addr 127.0.0.1:1080 --remote-addr 127.0.0.1:1234 --shared-secret my_secret --protocol kcp
    ```

3. try socks5 (on the same machine where client is running)

    ```shell
   curl -x socks5h://127.0.0.1:1080 https://www.example.com -v
    ```

   ```shell
   curl -x socks5h://127.0.0.1:1080 http://1.1.1.1:80 -v
   ```

## Todos

### Basic

- [x] accept certificates path/auth secret/listening port from cmd line: [clap-rs](https://github.com/clap-rs/clap)


### Medium

- [x] server: use constant time comparison for 'auth secret' (by relying on external library)
- [x] socks5: support domain type (ATYP = 3) CONNECT command, see [RFC 1928](https://youtube.com/watch?v=dQw4w9WgXcQ).
  You need to modify the protocol.

### Hard

- [x] support multiplexing of proxy connections using the same underlying tls connection to server (m:n model, currently
  1:1)

