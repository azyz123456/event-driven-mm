# Event-Driven MM

A Rust recorder and replayer for one public Hyperliquid `l2Book` subscription.

## Run a capture and replay it

With a stable Rust toolchain installed, run these commands from the project root. Choose a new journal path for each capture.

```sh
cargo run --locked -- capture --coin BTC --duration-secs 30 --log btc.jsonl > live.csv
cargo run --locked -- replay --log btc.jsonl > replay.csv
test -s live.csv && cmp live.csv replay.csv
```

Standard output contains `local_time_unix_ms,mid_price` rows without a header. Each usable snapshot produces a row, including snapshots where the midpoint stays the same. A snapshot needs bid and ask levels with positive prices and sizes and a best bid below the best ask. Diagnostics go to stderr. The capture duration starts after sending the subscription, and the client waits up to one additional second for the WebSocket close response. If the peer closes first, the journal records the close and the capture ends.

## Data flow

The network thread owns the blocking TCP socket and supplies buffers to the TLS and WebSocket codecs. It appends each outbound HTTP request or WebSocket message to the journal before sending it. Inbound records get a local wall-clock timestamp at the end of the HTTP upgrade head or complete WebSocket message. Hyperliquid heartbeats and WebSocket ping, pong, and close messages use the same recording path as market data.

The main thread follows the journal, passes records to the order book, and prints midpoint rows. It waits at temporary EOF, including after a partial record. Once the network worker has exited, the main thread drains the remaining records. Replay runs the same processing loop against a completed journal with fresh state and no network worker.

## Journal format

The journal is newline-delimited JSON. The first record, `session_start`, has sequence zero and stores the format version, endpoint, and coin. Each subsequent record includes a sequence number, `local_time_unix_ms`, `connection_id`, and an event type. File order is authoritative even if timestamps move backward.

TCP events include addresses, connection failures, or the close reason. HTTP events store the raw request and response heads as text or Base64 if a head contains non-UTF-8 bytes. WebSocket events identify their direction and opcode. Text payloads retain their original characters and non-text payloads are arrays of byte values. The journal captures application messages above TLS. Appending makes records available to the reader through the filesystem without syncing each record to disk.

## Tests

```sh
cargo test --locked
```

The tests use temporary files and a local TLS peer, so running the suite requires no exchange access. The loopback session checks the HTTP upgrade, outgoing journal order, WebSocket fragmentation and control messages, and byte-for-byte replay of a captured midpoint. Failure tests cover rejected upgrades, malformed WebSocket frames and exchange messages, truncated or invalid journal records, and file write errors.
