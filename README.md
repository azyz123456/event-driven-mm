# Event-Driven MM

A Rust recorder and replayer for one public Hyperliquid `l2Book` subscription.

## Run a capture and replay it

With a stable Rust toolchain installed, run these commands from the project root. Choose a new journal path for each capture.

```sh
cargo run --locked -- capture --coin BTC --duration-secs 30 --log btc.jsonl > live.csv
cargo run --locked -- replay --log btc.jsonl > replay.csv
test -s live.csv && cmp live.csv replay.csv
```

Standard output contains `local_time_unix_ms,mid_price` rows without a header. Each usable snapshot produces a row, including snapshots where the midpoint stays the same. A snapshot needs bid and ask levels with positive prices and sizes and a best bid below the best ask. Diagnostics go to stderr. The capture duration starts when the client queues the subscription. At the end of the capture, it waits up to one second for the WebSocket close response. If the peer closes first, the journal records the close and the capture ends.

## Data flow

The network thread uses Mio to drive nonblocking TCP, TLS, and WebSocket I/O. The command queue wakes the reactor after publishing a command. The reactor appends each outbound HTTP request or WebSocket message to the journal before sending it and preserves partial writes across readiness notifications. The reactor also records a strategy timer once per second and separate timers for order deadlines and Hyperliquid heartbeats. Timer records include the scheduled time, firing time, and skipped intervals after a delay.

The main thread follows the file-backed queue, passes records to the order book, and prints midpoint rows. The queue waits at temporary EOF, including after a partial record, and drains remaining records once the network worker has exited. Replay runs the same processing loop against a completed journal with fresh state and no network worker.

## Journal format

The journal is newline-delimited JSON. The first record, `session_start`, has sequence zero and stores the format version, endpoint, and coin. Each subsequent record includes a sequence number, `local_time_unix_ms`, `connection_id`, and an event type. File order is authoritative even if timestamps move backward.

TCP events include addresses, connection failures, or the close reason. HTTP events store the raw request and response heads as text or Base64 if a head contains non-UTF-8 bytes. WebSocket events identify their direction and opcode. Public capture preserves the original text payload, and non-text payloads are arrays of byte values. The journal captures application messages above TLS. Appending makes records available to the reader through the filesystem. Signed actions also use an explicit sync before dispatch.

## Latency measurements

Pass `--latency` to `capture` or `order-test` to print sample counts and approximate p50, p95, and p99 upper bounds in nanoseconds to stderr. The measurements cover kernel receive batch to read return, read to decode, journal append to engine consumption, intent enqueue to first socket write, and the signed-action journal sync.

New v3 journals record optional receive timing on HTTP responses and WebSocket messages. On Linux, the reactor requests software timestamps via `SO_TIMESTAMPING` and reads them through `recvmsg`. The timestamp belongs to the read batch that completed the TLS record needed by the message. TCP and TLS can combine multiple messages into one receive batch, so treat the kernel value as an estimate of local receive-to-read delay. macOS provides userspace timing. The summary counts missing samples and invalid wall-clock deltas as unavailable.

## Testnet order lifecycle

Use an isolated, funded Hyperliquid testnet account and its approved API wallet. Set `HYPERLIQUID_TESTNET_AGENT_KEY` in your environment and use the account's public address for `--account`. Choose explicit size and price for a default-dex perpetual on testnet. The order must meet exchange precision and minimum-notional rules and the configured `--max-notional` limit.

```sh
cargo run --locked -- order-test --account "$TESTNET_ACCOUNT" --coin "$TESTNET_COIN" \
  --price "$TESTNET_PRICE" --size "$TESTNET_SIZE" --max-notional "$TESTNET_MAX_NOTIONAL" \
  --log order-test.jsonl > order-test.txt
cargo run --locked -- replay --log order-test.jsonl > order-replay.txt
cmp order-test.txt order-replay.txt
```

The command checks the API wallet's account, the instrument metadata, open orders, position, and book before placing one post-only buy. It cancels by client order ID after receiving evidence that the order rested. Success requires verified cancellation, a flat position, and exactly one cancel intent. A fill, rejection, cleanup retry, or unresolved order returns a nonzero status. After an ambiguous reply or disconnect, the command queries the original client order ID and can reconnect once for cleanup. If the process is interrupted or reports an unresolved order, use the printed client order ID to check the order and position on testnet before starting another run.

Order tests write v3 journals with the effective settings, recorded inputs and timers, trade intents, unsigned action details, and the final outcome. The signing key and request signatures stay out of the journal. Replay uses the recorded timer firing times to regenerate the intents and final result, and fails if they differ from the recorded outputs. Replay also accepts v1 capture journals and v2 order journals.

To exercise the verifier without a testnet account, replay `tests/fixtures/order-session.jsonl`. The fixture contains a book snapshot from the original replay fixture and a scripted lifecycle.

## Tests

```sh
cargo test --locked
```

The tests use temporary files and local TLS peers, so running the suite requires no exchange access. They cover idle timers, command wakeups, short writes and backpressure, receive-batch attribution, capture and replay parity, and lost-reply recovery. On Linux the loopback test also checks that kernel timestamps reach the journal.

## Market maker and IOC hedge

The `maker` command uses testnet endpoints. Start from [examples/maker-testnet.json](examples/maker-testnet.json), which configures one matching perpetual, inventory and collateral limits, pricing, and timeouts. The three modes are `hyperliquid`, `lighter`, and `hedged`. The Hyperliquid-only mode takes `"lighter_market": null`.

```sh
cargo run --locked -- maker --config examples/maker-testnet.json \
  --observe --feed-copies 5 --duration-secs 30 --log observe.jsonl
cargo run --locked -- replay --log observe.jsonl
cargo run --locked -- inspect-maker --log observe.jsonl
cargo run --locked -- inspect-maker --log observe.jsonl --step
```

In the viewer, press Enter for the next record or type `q` to quit. The report shows book and trade arrival spreads, matched public and account trade timing, maker fill markouts at 100 ms, 1 second, and 5 seconds, and IOC completion and latency. Trade edge matches opposite fills in FIFO order and subtracts both fill fees. Trade edge and mark-to-market exclude funding. In observe mode, optional account identifiers enable account stream timing comparisons. Observe mode submits zero orders.

Signed sessions run on macOS and Linux. Use dedicated funded testnet accounts with no working orders and flat positions in the configured instrument. Set `HYPERLIQUID_TESTNET_AGENT_KEY` to the approved API wallet key. Set `LIGHTER_TESTNET_API_KEY` to the private API key associated with `--lighter-key-index` and `LIGHTER_TESTNET_SIGNER_LIB` to the matching platform library from the [official Lighter v1.1.9 SDK](https://github.com/elliottech/lighter-python/tree/v1.1.9/lighter/signers). Trade with one process per account and Lighter API key index.

```sh
cargo run --locked -- maker --config examples/maker-testnet.json \
  --hyperliquid-account "$TESTNET_ACCOUNT" \
  --lighter-account "$LIGHTER_ACCOUNT_INDEX" --lighter-key-index "$LIGHTER_API_KEY_INDEX" \
  --duration-secs 60 --log hedged-testnet.jsonl
```

The maker keeps one post-only order per side. In hedged mode it prices Lighter quotes against executable Hyperliquid depth, and account fills trigger price-bounded Hyperliquid IOCs. Partial fills leave a residual for subsequent attempts. If a connection or book goes stale, the engine cancels resting maker quotes and waits for account and book reconciliation before replacing them. A standalone Hyperliquid session requires an acknowledged scheduled cancel. Lighter post-only orders carry an exchange expiry. Signed wire payloads and auth tokens stay out of the journal.

Duration expiry, Ctrl-C, and SIGTERM start cancellation and reconciliation. A hedged run reports failure if it reaches the recovery deadline with residual exposure, unresolved orders, or unverified accounts. A single-venue run can end with confirmed inventory from fills. Inspect the journal and venue positions before starting another session.

For a soak test, run an observe session with `--duration-secs 86400`, check the journal with `replay` and `inspect-maker`, then repeat with dedicated testnet accounts. Check both venue fill totals, residual size and age, and any unresolved orders. The offline signer ABI test can be run with `LIGHTER_TESTNET_SIGNER_LIB` set using `cargo test --locked signs_with_the_official_lighter_library -- --ignored`.
