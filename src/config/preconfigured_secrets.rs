use crate::config::SecurityConfig;

const AUTH_SECRET: &str = "hello_world+123###@@@QwQ";

pub fn get_server_config() -> anyhow::Result<SecurityConfig> {
    SecurityConfig::new(
        "certs/server/server.crt",
        "certs/server/server.key",
        "certs/ca/ca.crt",
        AUTH_SECRET,
    )
}

pub fn get_client_config() -> anyhow::Result<SecurityConfig> {
    SecurityConfig::new(
        "certs/client/client.crt",
        "certs/client/client.key",
        "certs/ca/ca.crt",
        AUTH_SECRET,
    )
}
