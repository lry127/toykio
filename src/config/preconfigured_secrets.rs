// todo: read from file

use crate::config::{CertificateBundle, SecurityConfig};

const SERVER_CERTIFICATE: &str = "
-----BEGIN CERTIFICATE-----
MIIBPDCB76ADAgECAhRal0g+nKZLk5W+wHDAhIDNPopJLDAFBgMrZXAwFDESMBAG
A1UEAwwJbG9jYWxob3N0MB4XDTI2MDcwODEzNTkzN1oXDTI3MDcwODEzNTkzN1ow
FDESMBAGA1UEAwwJbG9jYWxob3N0MCowBQYDK2VwAyEAG1aAAawTnj9sn5i8CiSH
BX7sBbWNdDXcszHBU9Kup6qjUzBRMB0GA1UdDgQWBBRO/+w70Vsq0Cfp8rDE4Lz5
F6HFJjAfBgNVHSMEGDAWgBRO/+w70Vsq0Cfp8rDE4Lz5F6HFJjAPBgNVHRMBAf8E
BTADAQH/MAUGAytlcANBAKzmxWUVhq3P1B9hG2muoqRv1Ia+XIGZW7a2A13VdwnU
BeTbktkZp38hQlqTvOsPqo2ovg8POXObV1c+GwzmnQI=
-----END CERTIFICATE-----";

const SERVER_PRIV_KEY: &str = "
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEILhh88ToxI6mg/VX4ZXJQjTS3n6dnRFp5m6hnGZKQSu7
-----END PRIVATE KEY-----";

const AUTH_SECRET:&str = "hello_world+123###@@@QwQ";

pub fn get_preconfigured_security_config() -> SecurityConfig {
    let server_bundle = CertificateBundle {
        certificate_data: SERVER_CERTIFICATE.into(),
        certificate_priv_key: Some(SERVER_PRIV_KEY.into()),
    };
    
    let client_bundle = CertificateBundle {
        certificate_data: "".to_string(),
        certificate_priv_key: None,
    };

    SecurityConfig {
        server_bundle,client_bundle,
        auth_secret: AUTH_SECRET.into()
    }
}
