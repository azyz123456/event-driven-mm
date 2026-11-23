use std::net::SocketAddr;

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

use crate::intent::Intent;
use crate::maker;
use crate::order_test::{Config, Input, Outcome};

pub const FORMAT_VERSION: u32 = 1;
pub const ORDER_FORMAT_VERSION: u32 = 2;
pub const REACTOR_FORMAT_VERSION: u32 = 3;
pub const MAKER_FORMAT_VERSION: u32 = 4;

#[derive(Debug, Deserialize, Serialize)]
pub struct Record {
    pub seq: u64,
    pub local_time_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_timing: Option<ReceiveTiming>,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        order_test: Option<Config>,
    },
    MakerStart {
        version: u32,
        config: maker::Config,
        seed: u64,
        observe: bool,
    },
    MakerInput {
        elapsed_ms: u64,
        input: maker::Input,
    },
    MakerIntent {
        intent: maker::Intent,
    },
    MakerPrepared {
        intent_id: u64,
        nonce: u64,
        tx_hash: Option<String>,
    },
    MakerProtectionPrepared {
        request_id: u64,
        nonce: u64,
        expires_unix_ms: Option<u64>,
    },
    MakerNotice {
        venue: maker::Venue,
        reason: String,
    },
    MakerEnd {
        summary: maker::End,
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
    OrderInput {
        elapsed_ms: u64,
        input: Input,
    },
    IntentQueued {
        intent: Intent,
    },
    ActionPrepared {
        intent_id: u64,
        request_id: u64,
        nonce: u64,
        expires_after: u64,
        action: serde_json::Value,
    },
    OrderOutcome {
        outcome: Outcome,
    },
    Timer {
        kind: TimerKind,
        scheduled_elapsed_ns: u64,
        fired_elapsed_ns: u64,
        missed_intervals: u64,
    },
    TxFirstWrite {
        intent_id: u64,
        enqueued_elapsed_ns: u64,
        first_write_elapsed_ns: u64,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerKind {
    StrategySecond,
    OrderDeadlineCheck,
    Heartbeat,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct ReceiveTiming {
    pub batch_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kernel_rx_unix_ns: Option<u64>,
    pub read_unix_ns: Option<u64>,
    pub read_elapsed_ns: u64,
    pub decoded_elapsed_ns: u64,
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
