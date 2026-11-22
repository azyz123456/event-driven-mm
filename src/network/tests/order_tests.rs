use crate::queue::command_queue;
use std::collections::VecDeque;

use hypersdk::hypercore::{Chain, PrivateKeySigner, api::ActionRequest};
use serde_json::{Value, json};

use super::*;
use crate::hyperliquid::TESTNET_HOST;
use crate::intent::Action;
use crate::order_test::{Config, Outcome};
use crate::signing::Signer;

const TEST_ACCOUNT: &str = "0x0101010101010101010101010101010101010101";
// Public, deterministic key for loopback test vectors only.
const TEST_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const TEST_AGENT: &str = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf";
const OID: u64 = 12345;

#[test]
fn place_reply_then_update_produces_one_place_and_cancel() -> Result<()> {
    normal_lifecycle(false, false)
}

#[test]
fn update_then_place_reply_produces_one_place_and_cancel() -> Result<()> {
    normal_lifecycle(true, false)
}

#[test]
fn lost_cancel_reply_recovers_the_terminal_status_without_retrying_placement() -> Result<()> {
    normal_lifecycle(true, true)
}

fn normal_lifecycle(update_first: bool, lose_cancel_reply: bool) -> Result<()> {
    let test = run_order_peers(1, move |mut peers, mut journal| {
        let peer = &mut peers[0];
        let place = preflight(peer)?;
        let cloid = place["request"]["payload"]["action"]["orders"][0]["c"]
            .as_str()
            .context("missing place cloid")?;
        let nonce = check_action(&place, "order", &mut journal)?;
        assert_eq!(
            place["request"]["payload"]["action"]["orders"][0],
            json!({
                "a": 1, "b": true, "p": "98", "s": "0.2", "r": false, "t": {"limit": {"tif": "Alo"}}, "c": cloid,
            })
        );

        let update = order_update(cloid, "open", "0.2");
        let reply = action_reply(&place, "order", json!({"resting": {"oid": OID}}));
        let frames = if update_first {
            [update.clone(), reply]
        } else {
            [reply, update.clone()]
        };
        for frame in frames {
            send_json(peer, frame)?;
        }
        send_json(peer, update)?;

        let cancel = client_json(peer)?;
        assert!(check_action(&cancel, "cancelByCloid", &mut journal)? > nonce);
        assert_eq!(
            cancel["request"]["payload"]["action"]["cancels"],
            json!([{"asset": 1, "cloid": cloid}])
        );
        if !lose_cancel_reply {
            send_json(peer, action_reply(&cancel, "cancel", json!("success")))?;
        }
        complete_verification(peer, cloid)
    })?;
    assert_verified(&test)?;
    if lose_cancel_reply {
        assert!(test.records.iter().any(|record| matches!(
            record.event,
            Event::Timer {
                kind: TimerKind::OrderDeadlineCheck,
                ..
            }
        )));
    }
    Ok(())
}

#[test]
fn reconnects_after_ambiguous_placement_and_cancels_by_the_original_cloid() -> Result<()> {
    let test = run_order_peers(2, |mut peers, mut journal| {
        let mut first = peers.remove(0);
        let place = preflight(&mut first)?;
        let cloid = place["request"]["payload"]["action"]["orders"][0]["c"]
            .as_str()
            .context("missing place cloid")?;
        let place_nonce = check_action(&place, "order", &mut journal)?;
        // The peer received the action. Terminate the connection before replying.
        drop(first);

        let peer = &mut peers[0];
        handshake(peer)?;
        read_subscription(peer, "orderUpdates")?;
        read_subscription(peer, "userFills")?;
        check_agent(peer, TEST_ACCOUNT)?;
        let status = client_json(peer)?;
        assert_info(&status, "orderStatus");
        assert_eq!(status["request"]["payload"]["oid"], cloid);
        send_json(
            peer,
            info_reply(&status, "orderStatus", status_body(cloid, "open", "0.2")),
        )?;

        let cancel = client_json(peer)?;
        assert!(check_action(&cancel, "cancelByCloid", &mut journal)? > place_nonce);
        assert_eq!(
            cancel["request"]["payload"]["action"]["cancels"][0]["cloid"],
            cloid
        );
        send_json(peer, action_reply(&cancel, "cancel", json!("success")))?;
        complete_verification(peer, cloid)
    })?;
    assert_verified(&test)?;
    assert_eq!(
        test.records
            .iter()
            .filter(|record| matches!(record.event, Event::TcpOpen { .. }))
            .count(),
        2
    );
    Ok(())
}

#[test]
fn wrong_testnet_agent_account_stops_before_any_signed_action() -> Result<()> {
    let test = run_order_peers(1, |mut peers, _| {
        let peer = &mut peers[0];
        handshake(peer)?;
        assert_eq!(client_json(peer)?["subscription"]["type"], "l2Book");
        read_subscription(peer, "orderUpdates")?;
        read_subscription(peer, "userFills")?;
        check_agent(peer, "0x0202020202020202020202020202020202020202")?;
        for kind in ["meta", "openOrders", "clearinghouseState"] {
            assert_info(&client_json(peer)?, kind);
        }
        assert_eq!(read_client_frame(peer)?.0, 0x88);
        Ok(())
    })?;
    assert!(
        matches!(test.outcome, Outcome::Rejected { reason } if reason.contains("does not belong"))
    );
    assert_eq!(test.live, test.replay);
    assert!(!test.records.iter().any(|record| matches!(
        record.event,
        Event::IntentQueued { .. } | Event::ActionPrepared { .. }
    )));
    Ok(())
}

struct LocalOrder {
    outcome: Outcome,
    live: Vec<u8>,
    replay: Vec<u8>,
    records: Vec<Record>,
    journal_text: String,
}

fn run_order_peers(
    count: usize,
    serve: impl FnOnce(Vec<PeerStream>, JournalReader) -> Result<()> + Send,
) -> Result<LocalOrder> {
    let key = rcgen::generate_simple_self_signed(vec![TESTNET_HOST.into()])?;
    let cert = key.cert.der().clone();
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone())?;
    let server = Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert],
                PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
            )?,
    );
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let mut sockets = VecDeque::new();
    let mut peers = Vec::new();
    for _ in 0..count {
        let client = StdTcpStream::connect(listener.local_addr()?)?;
        client.set_nonblocking(true)?;
        sockets.push_back(TcpStream::from_std(client));
        let (socket, _) = listener.accept()?;
        socket.set_nodelay(true)?;
        socket.set_read_timeout(Some(SOCKET_TIMEOUT))?;
        socket.set_write_timeout(Some(SOCKET_TIMEOUT))?;
        peers.push(StreamOwned::new(
            ServerConnection::new(Arc::clone(&server))?,
            socket,
        ));
    }

    let mut config = Config::new(
        TEST_ACCOUNT.into(),
        "BTC".into(),
        "98".parse()?,
        "0.2".parse()?,
        "20".parse()?,
    )?;
    config.timings.ack_ms = 500;
    config.timings.probe_interval_ms = 100;
    let file = tempfile::NamedTempFile::new()?;
    let (writer, reader) = file_queue(file.reopen()?, file.reopen()?, Arc::new(Latency::new()));
    let reactor = Reactor::new()?;
    let peer_reader = JournalReader::new(file.reopen()?);
    let (sender, receiver) = command_queue(8, reactor.waker());
    let signer = Signer::new(TEST_KEY)?;
    let mut live = Vec::new();
    let outcome = thread::scope(|scope| -> Result<Outcome> {
        let server = scope.spawn(move || serve(peers, peer_reader));
        let worker = scope.spawn(move || {
            trading::run_with(
                reactor,
                writer,
                config,
                signer,
                receiver,
                |_, _| sockets.pop_front().context("unexpected connection attempt"),
                client_config(roots),
            )
        });
        let result = engine::run_order_test(reader, || worker.is_finished(), &mut live, sender);
        server.join().expect("testnet peer panicked")?;
        worker.join().expect("testnet worker panicked")?;
        result
    })?;
    let mut replay = Vec::new();
    engine::run(RecordSource::replay(file.reopen()?), || true, &mut replay)?;
    let mut reader = JournalReader::new(file.reopen()?);
    let mut records = Vec::new();
    while let Some(record) = reader.next_record()? {
        records.push(record);
    }
    let mut journal_text = String::new();
    file.reopen()?.read_to_string(&mut journal_text)?;
    Ok(LocalOrder {
        outcome,
        live,
        replay,
        records,
        journal_text,
    })
}

fn assert_verified(test: &LocalOrder) -> Result<()> {
    assert_eq!(test.outcome, Outcome::VerifiedCanceled { oid: OID });
    assert_eq!(test.live, test.replay);
    assert!(matches!(
        test.records[0].event,
        Event::SessionStart {
            version: REACTOR_FORMAT_VERSION,
            ..
        }
    ));
    let intents: Vec<_> = test
        .records
        .iter()
        .filter_map(|record| match &record.event {
            Event::IntentQueued { intent } => Some(intent),
            _ => None,
        })
        .collect();
    assert!(
        matches!(intents.as_slice(), [place, cancel] if matches!(place.action, Action::Place { .. }) && cancel.action == (Action::Cancel { oid: OID }))
    );
    assert!(!test.journal_text.contains(TEST_KEY));
    assert!(!test.journal_text.contains("\"signature\""));
    assert!(!test.journal_text.contains("\\\"signature\\\""));
    let writes: Vec<_> = test
        .records
        .iter()
        .filter_map(|record| match record.event {
            Event::TxFirstWrite {
                intent_id,
                enqueued_elapsed_ns,
                first_write_elapsed_ns,
            } => Some((
                intent_id,
                record.seq,
                enqueued_elapsed_ns,
                first_write_elapsed_ns,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(writes.len(), 2);
    for (id, seq, enqueued, written) in writes {
        assert!(enqueued <= written);
        assert!(test.records.iter().any(|record| record.seq < seq
            && matches!(record.event, Event::ActionPrepared { intent_id, .. } if intent_id == id)));
    }
    Ok(())
}

fn handshake(peer: &mut PeerStream) -> Result<()> {
    let request = read_upgrade_request_for_host(peer, TESTNET_HOST)?;
    peer.write_all(&upgrade_response(&request)?)?;
    Ok(())
}

fn preflight(peer: &mut PeerStream) -> Result<Value> {
    handshake(peer)?;
    let book = client_json(peer)?;
    assert_eq!(
        book["subscription"],
        json!({"type": "l2Book", "coin": "BTC"})
    );
    read_subscription(peer, "orderUpdates")?;
    read_subscription(peer, "userFills")?;
    check_agent(peer, TEST_ACCOUNT)?;
    for (kind, payload) in [
        (
            "meta",
            json!({"universe":[{"name":"ETH","szDecimals":4},{"name":"BTC","szDecimals":5}]}),
        ),
        ("openOrders", json!([])),
        ("clearinghouseState", json!({"assetPositions":[]})),
    ] {
        let request = client_json(peer)?;
        assert_info(&request, kind);
        send_json(peer, info_reply(&request, kind, payload))?;
    }
    peer.write_all(&server_frame(0x81, BOOK))?;
    client_json(peer)
}

fn check_agent(peer: &mut PeerStream, owner: &str) -> Result<()> {
    let request = client_json(peer)?;
    assert_eq!(request["request"]["type"], "info");
    assert_eq!(
        request["request"]["payload"],
        json!({"type": "userRole", "user": TEST_AGENT})
    );
    send_json(
        peer,
        info_reply(
            &request,
            "userRole",
            json!({"role": "agent", "data": {"user": owner}}),
        ),
    )
}

fn read_subscription(peer: &mut PeerStream, kind: &str) -> Result<()> {
    let subscribe = client_json(peer)?;
    assert_eq!(
        subscribe,
        json!({"method":"subscribe", "subscription":{"type":kind,"user":TEST_ACCOUNT}})
    );
    send_json(
        peer,
        json!({"channel":"subscriptionResponse", "data":subscribe}),
    )
}

fn complete_verification(peer: &mut PeerStream, cloid: &str) -> Result<()> {
    let status = client_json(peer)?;
    assert_info(&status, "orderStatus");
    assert_eq!(status["request"]["payload"]["oid"], cloid);
    send_json(
        peer,
        info_reply(
            &status,
            "orderStatus",
            status_body(cloid, "canceled", "0.2"),
        ),
    )?;
    let orders = client_json(peer)?;
    assert_info(&orders, "openOrders");
    send_json(peer, info_reply(&orders, "openOrders", json!([])))?;
    let position = client_json(peer)?;
    assert_info(&position, "clearinghouseState");
    send_json(
        peer,
        info_reply(
            &position,
            "clearinghouseState",
            json!({"assetPositions":[]}),
        ),
    )?;
    assert_eq!(read_client_frame(peer)?.0, 0x88);
    Ok(())
}

fn assert_info(request: &Value, kind: &str) {
    assert_eq!(request["request"]["type"], "info");
    assert_eq!(request["request"]["payload"]["type"], kind);
    if kind != "meta" {
        assert_eq!(request["request"]["payload"]["user"], TEST_ACCOUNT);
    }
}

fn check_action(request: &Value, kind: &str, journal: &mut JournalReader) -> Result<u64> {
    assert_eq!(request["request"]["type"], "action");
    let payload = &request["request"]["payload"];
    assert_eq!(payload["action"]["type"], kind);
    let signed: ActionRequest = serde_json::from_value(payload.clone())?;
    let signer = TEST_KEY.parse::<PrivateKeySigner>()?;
    assert_eq!(signed.recover(Chain::Testnet)?, signer.address());
    assert_ne!(signed.recover(Chain::Mainnet)?, signer.address());
    let mut queued = None;
    while let Some(record) = journal.next_record()? {
        match record.event {
            Event::IntentQueued { intent } => queued = Some(intent.id),
            Event::ActionPrepared {
                intent_id,
                request_id,
                action,
                nonce,
                expires_after,
            } if request["id"] == request_id => {
                assert_eq!(queued, Some(intent_id));
                assert_eq!(action, payload["action"]);
                assert_eq!(payload["nonce"], nonce);
                assert_eq!(payload["expiresAfter"], expires_after);
                assert!(expires_after > nonce);
                return Ok(nonce);
            }
            _ => {}
        }
    }
    bail!("signed action reached the peer before its journal records")
}

fn info_reply(request: &Value, kind: &str, body: Value) -> Value {
    json!({"channel":"post", "data":{"id":request["id"], "response":{"type":"info", "payload":{"type":kind,"data":body}}}})
}

fn action_reply(request: &Value, kind: &str, status: Value) -> Value {
    json!({"channel":"post", "data":{"id":request["id"], "response":{"type":"action", "payload":{"status":"ok","response":{"type":kind,"data":{"statuses":[status]}}}}}})
}

fn order_update(cloid: &str, status: &str, remaining: &str) -> Value {
    json!({"channel":"orderUpdates", "data":[status_body(cloid, status, remaining)["order"]]})
}

fn status_body(cloid: &str, status: &str, remaining: &str) -> Value {
    json!({"status":"order","order":{"status":status,"order":{"coin":"BTC","oid":OID,"cloid":cloid,"origSz":"0.2","sz":remaining}}})
}

fn client_json(peer: &mut PeerStream) -> Result<Value> {
    let (opcode, data) = read_client_frame(peer)?;
    assert_eq!(opcode, 0x81);
    Ok(serde_json::from_slice(&data)?)
}

fn send_json(peer: &mut PeerStream, value: Value) -> Result<()> {
    peer.write_all(&server_frame(0x81, value.to_string().as_bytes()))?;
    Ok(())
}
