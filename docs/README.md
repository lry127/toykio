# Generate certificates

in proj/certs dir, generate certificate using the following command

```bash
export SERVICE_IDENTIFIER="toykio"
export SAN="DNS:localhost,IP:127.0.0.1"
```

```bash
# ca
mkdir -p ca && cd ca
openssl genpkey -algorithm ed25519  -out ca.key
openssl req -x509 -new -key ca.key -sha256 -days 3650 -out ca.crt \
  -subj "/C=US/CN=$SERVICE_IDENTIFIER Self Trusted Root"
cd ..

# server
mkdir -p server && cd server
openssl genpkey -algorithm ed25519  -out server.key
openssl req -new -key server.key -out server.csr \
  -subj "/C=US/CN=$SERVICE_IDENTIFIER Server"
echo "subjectAltName=$SAN" > server.ext
openssl x509 -req -in server.csr \
  -CA ../ca/ca.crt -CAkey ../ca/ca.key -CAcreateserial \
  -out server.crt -days 3650 -sha256 \
  -extfile server.ext
openssl pkcs12 -export -out server.p12 \
  -inkey server.key -in server.crt \
  -certfile ../ca/ca.crt \
  -password pass:password
rm server.ext
cd ..

# client
mkdir -p client && cd client
openssl genpkey -algorithm ed25519  -out client.key
openssl req -new -key client.key -out client.csr \
  -subj "/C=US/CN=$SERVICE_IDENTIFIER Client"
openssl x509 -req -in client.csr \
  -CA ../ca/ca.crt -CAkey ../ca/ca.key -CAcreateserial \
  -out client.crt -days 3650 -sha256
openssl pkcs12 -export -out client.p12 \
  -inkey client.key -in client.crt \
  -certfile ../ca/ca.crt \
  -password pass:password
cd ..