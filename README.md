# Toykio Proxy

Toykio (TOY toKIO) is a toy (but functional) network proxy written to get my hands dirty with rust and net programming with tokio

## Features

- pqs mTLS 1.3 from client to proxy server
- (basic) socks5 on client side
- custom protocol


## run!

1. server (listening on 0.0.0.0:1234, where actual outbound connections to target happen)
    ```shell
      cargo run --bin run_server -- --cert-path ./certs/server/server.crt --cert-key ./certs/server/server.key --ca-cert ./certs/ca/ca.crt  --listen-addr 127.0.0.1:1234 --shared-secret my_secret
    ```

2. client (socks5 listening on 127.0.0.1:1080, relaying proxy request to server (default 127.0.0.1:1234), server and client can reside on different machines)
    ```shell
    cargo run --bin run_client -- --cert-path ./certs/client/client.crt --cert-key ./certs/client/client.key --ca-cert ./certs/ca/ca.crt --socks5-addr 127.0.0.1:1080 --remote-addr 127.0.0.1:1234 --shared-secret my_secret
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

- [ ] support multiplexing of proxy connections using the same underlying tls connection to server (m:n model, currently 1:1)

