# Toykio Proxy

Toykio (TOY toKIO) is a toy (but functional) network proxy written to get my hands dirty with rust and net programming with tokio

## Features

- pqs mTLS 1.3 from client to proxy server
- (basic) socks5 on client side
- custom protocol


## Challenges You Can Try

### Basic

- [ ] accept certificates path/auth secret from cmd line


### Medium

- [ ] server: use constant time compare for 'auth secret' (by relying on external library)
- [ ] socks5: support domain type (ATYP = 3) CONNECT command, see [RFC 1928](https://www.rfc-editor.org/info/rfc1928/). You need to modify the protocol.


### Challenging

- [ ] support multiplexing of proxy connections using the same underlying tls connection to server (m:n model, current is 1:1)

