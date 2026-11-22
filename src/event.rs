use std::net::SocketAddr;

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Deserialize, Serialize)]
pub struct Record {
    pub seq: u64,
    pub local_time_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<u64>,
    #[serde(flatten)]
    pub event: Event,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    SessionStart {
        version: u32,
        endpoint: String,
        coin: String,
    },
    TcpConnectFailed {
        error: String,
    },
    TcpOpen {
        local_addr: SocketAddr,
        peer_addr: SocketAddr,
    },
    TcpClose {
        reason: String,
    },
    HttpRequest {
        raw: HttpBytes,
    },
    HttpResponse {
        raw: HttpBytes,
    },
    WsIn {
        message: Message,
    },
    WsOut {
        message: Message,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum HttpBytes {
    Text(String),
    Bytes { base64: String },
}

impl From<Vec<u8>> for HttpBytes {
    fn from(bytes: Vec<u8>) -> Self {
        match String::from_utf8(bytes) {
            Ok(text) => Self::Text(text),
            Err(error) => Self::Bytes {
                base64: STANDARD.encode(error.into_bytes()),
            },
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "opcode", content = "payload", rename_all = "snake_case")]
pub enum Message {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Vec<u8>),
}
