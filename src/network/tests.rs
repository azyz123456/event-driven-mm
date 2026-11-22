use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream as StdTcpStream};
use std::thread;

use anyhow::ensure;
use base64::{Engine, engine::general_purpose::STANDARD};
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use sha1::{Digest, Sha1};

use super::*;
use crate::engine;
use crate::event::{HttpBytes, Record};
use crate::journal::JournalReader;
use crate::latency::Latency;
use crate::queue::{RecordSource, file_queue};

type PeerStream = StreamOwned<ServerConnection, StdTcpStream>;
const SOCKET_TIMEOUT: Duration = Duration::from_secs(5);
const NORMAL_CLOSE: [u8; 2] = 1000_u16.to_be_bytes();
const BOOK: &[u8] = br#"{"channel":"l2Book","data":{"coin":"BTC","levels":[[{"px":"99","sz":"1"}],[{"px":"101.005","sz":"2"}]]}}"#;

#[test]
fn records_a_loopback_tls_session_and_replays_its_exact_output() -> Result<()> {
    let session = run_local(SOCKET_TIMEOUT, |mut peer, mut journal| {
        let request = read_upgrade_request(&mut peer)?;
        assert!(matches!(
            next_event(&mut journal)?,
            Event::SessionStart { .. }
        ));
        assert!(matches!(next_event(&mut journal)?, Event::TcpOpen { .. }));
        assert!(matches!(
            next_event(&mut journal)?,
            Event::HttpRequest { raw: HttpBytes::Text(raw) } if raw == request
        ));

        let response = upgrade_response(&request)?;
        let split = response.len() - 2;
        peer.write_all(&response[..split])?;
        let mut tail = response[split..].to_vec();
        tail.extend(server_frame(0x89, b"upgrade read-ahead"));
        peer.write_all(&tail)?;

        let (opcode, subscribe) = read_client_frame(&mut peer)?;
        assert_eq!(opcode, 0x81);
        let request: serde_json::Value = serde_json::from_slice(&subscribe)?;
        assert_eq!(request["method"], "subscribe");
        assert_eq!(request["subscription"]["type"], "l2Book");
        assert_eq!(request["subscription"]["coin"], "BTC");
        assert!(matches!(
            next_event(&mut journal)?,
            Event::HttpResponse { raw: HttpBytes::Text(raw) } if raw.as_bytes() == response
        ));
        assert!(matches!(
            next_event(&mut journal)?,
            Event::WsOut { message: Message::Text(text) } if text.as_bytes() == subscribe
        ));
        assert_eq!(
            read_client_frame(&mut peer)?,
            (0x8a, b"upgrade read-ahead".to_vec())
        );

        let mut payload = BOOK.to_vec();
        payload.resize(24 * 1024, b' ');
        let mut bundle = server_frame(0x01, &payload[..31]);
        bundle.extend(server_frame(0x89, b"inside fragmented message"));
        bundle.extend(server_frame(0x80, &payload[31..]));
        peer.write_all(&bundle)?;
        assert_eq!(
            read_client_frame(&mut peer)?,
            (0x8a, b"inside fragmented message".to_vec())
        );

        peer.write_all(&server_frame(0x88, &NORMAL_CLOSE))?;
        assert_eq!(read_client_frame(&mut peer)?, (0x88, NORMAL_CLOSE.to_vec()));
        Ok(())
    })?;
    session.capture_result?;

    let book_record = session
        .records
        .iter()
        .find(|record| matches!(&record.event, Event::WsIn { message: Message::Text(text) } if text.as_bytes().starts_with(BOOK)))
        .context("fragmented book snapshot missing from journal")?;
    assert_eq!(
        session.live,
        format!("{},100.0025\n", book_record.local_time_unix_ms).as_bytes()
    );
    assert_eq!(session.replayed, session.live);
    let timing = |predicate: fn(&Event) -> bool| {
        session
            .records
            .iter()
            .find(|record| predicate(&record.event))
            .and_then(|record| record.receive_timing)
            .context("missing receive batch timing")
    };
    let upgrade = timing(|event| matches!(event, Event::HttpResponse { .. }))?;
    let read_ahead = timing(
        |event| matches!(event, Event::WsIn { message: Message::Ping(bytes) } if bytes == b"upgrade read-ahead"),
    )?;
    let interleaved = timing(
        |event| matches!(event, Event::WsIn { message: Message::Ping(bytes) } if bytes == b"inside fragmented message"),
    )?;
    let book_timing = book_record
        .receive_timing
        .context("missing book receive timing")?;
    assert_eq!(upgrade.batch_id, read_ahead.batch_id);
    assert!(book_timing.batch_id > interleaved.batch_id);
    for stamp in [upgrade, read_ahead, interleaved, book_timing] {
        assert!(stamp.decoded_elapsed_ns >= stamp.read_elapsed_ns);
        assert!(stamp.read_unix_ns.is_some());
        #[cfg(target_os = "linux")]
        assert!(
            stamp.kernel_rx_unix_ns.is_some(),
            "Linux loopback timestamp missing for batch {}",
            stamp.batch_id
        );
        #[cfg(not(target_os = "linux"))]
        assert!(stamp.kernel_rx_unix_ns.is_none());
    }
    assert!(matches!(
        session.records.last().map(|record| &record.event),
        Some(Event::TcpClose { reason }) if reason == "peer_closed"
    ));
    Ok(())
}

#[test]
fn records_a_rejected_upgrade_without_sending_a_subscription() -> Result<()> {
    let response = b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n";
    let session = run_local(SOCKET_TIMEOUT, |mut peer, _| {
        read_upgrade_request(&mut peer)?;
        peer.write_all(response)?;
        Ok(())
    })?;

    assert!(
        session
            .capture_result
            .unwrap_err()
            .to_string()
            .contains("403")
    );
    assert!(session.live.is_empty() && session.replayed.is_empty());
    assert!(matches!(
        session.records.iter().map(|record| &record.event).collect::<Vec<_>>().as_slice(),
        [Event::SessionStart { .. }, Event::TcpOpen { .. }, Event::HttpRequest { .. },
         Event::HttpResponse { raw: HttpBytes::Text(raw) }, Event::TcpClose { reason }]
         if raw.as_bytes() == response && reason.contains("403")
    ));
    Ok(())
}

#[test]
fn replies_to_ping_during_shutdown_and_rejects_a_truncated_frame_at_tls_eof() -> Result<()> {
    let session = run_local(Duration::ZERO, |mut peer, _| {
        let request = read_upgrade_request(&mut peer)?;
        peer.write_all(&upgrade_response(&request)?)?;
        assert_eq!(read_client_frame(&mut peer)?.0, 0x81);
        assert_eq!(read_client_frame(&mut peer)?, (0x88, NORMAL_CLOSE.to_vec()));

        peer.write_all(&server_frame(0x89, b"during shutdown"))?;
        assert_eq!(
            read_client_frame(&mut peer)?,
            (0x8a, b"during shutdown".to_vec())
        );
        peer.write_all(&[0x81])?;
        peer.conn.send_close_notify();
        peer.flush()?;
        Ok(())
    })?;

    let expected = "incomplete WebSocket message at EOF";
    assert!(
        session
            .capture_result
            .unwrap_err()
            .to_string()
            .contains(expected)
    );
    assert!(session.live.is_empty() && session.replayed.is_empty());
    assert!(matches!(
        session.records.last().map(|record| &record.event),
        Some(Event::TcpClose { reason }) if reason.contains(expected)
    ));
    Ok(())
}

#[test]
fn records_a_strategy_timer_on_an_idle_connection() -> Result<()> {
    let session = run_local(Duration::from_millis(1100), |mut peer, _| {
        let request = read_upgrade_request(&mut peer)?;
        peer.write_all(&upgrade_response(&request)?)?;
        assert_eq!(read_client_frame(&mut peer)?.0, 0x81);
        assert_eq!(read_client_frame(&mut peer)?, (0x88, NORMAL_CLOSE.to_vec()));
        peer.write_all(&server_frame(0x88, &NORMAL_CLOSE))?;
        Ok(())
    })?;
    session.capture_result?;
    let strategy_ticks: Vec<_> = session
        .records
        .iter()
        .filter_map(|record| match record.event {
            Event::Timer {
                kind: TimerKind::StrategySecond,
                scheduled_elapsed_ns,
                fired_elapsed_ns,
                missed_intervals,
            } => Some((scheduled_elapsed_ns, fired_elapsed_ns, missed_intervals)),
            _ => None,
        })
        .collect();
    assert!(
        matches!(strategy_ticks.as_slice(), [(scheduled, fired, 0)] if *scheduled >= 1_000_000_000 && fired >= scheduled)
    );
    assert!(session.live.is_empty() && session.replayed.is_empty());
    Ok(())
}

struct LocalCapture {
    capture_result: Result<()>,
    records: Vec<Record>,
    live: Vec<u8>,
    replayed: Vec<u8>,
}

fn run_local(
    duration: Duration,
    serve: impl FnOnce(PeerStream, JournalReader) -> Result<()> + Send,
) -> Result<LocalCapture> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let client = StdTcpStream::connect(listener.local_addr()?)?;
    client.set_nonblocking(true)?;
    let client = TcpStream::from_std(client);
    let (peer_socket, _) = listener.accept()?;
    peer_socket.set_nodelay(true)?;
    peer_socket.set_read_timeout(Some(SOCKET_TIMEOUT))?;
    peer_socket.set_write_timeout(Some(SOCKET_TIMEOUT))?;

    let key = rcgen::generate_simple_self_signed(vec![HOST.into()])?;
    let cert = key.cert.der().clone();
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone())?;
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert],
            PrivatePkcs8KeyDer::from(key.signing_key.serialize_der()).into(),
        )?;
    let peer = StreamOwned::new(ServerConnection::new(Arc::new(server_config))?, peer_socket);

    let file = tempfile::NamedTempFile::new()?;
    let (writer, live_reader) =
        file_queue(file.reopen()?, file.reopen()?, Arc::new(Latency::new()));
    let reactor = Reactor::new()?;
    let peer_reader = JournalReader::new(file.reopen()?);
    let stop = AtomicBool::new(false);
    let mut live = Vec::new();
    let capture_result = thread::scope(|scope| -> Result<_> {
        let mock = scope.spawn(move || serve(peer, peer_reader));
        let worker = scope.spawn(|| {
            capture_session(
                reactor,
                writer,
                "BTC",
                duration,
                &stop,
                |_, _| Ok(client),
                client_config(roots),
            )
        });
        let engine_result = engine::run(live_reader, || worker.is_finished(), &mut live);
        stop.store(true, Ordering::Relaxed);
        let capture_result = worker.join().expect("network worker panicked");
        mock.join().expect("loopback peer panicked")?;
        engine_result?;
        Ok(capture_result)
    })?;

    let mut replayed = Vec::new();
    engine::run(RecordSource::replay(file.reopen()?), || true, &mut replayed)?;
    let mut reader = JournalReader::new(file.reopen()?);
    let mut records = Vec::new();
    while let Some(record) = reader.next_record()? {
        records.push(record);
    }
    reader.finish()?;
    Ok(LocalCapture {
        capture_result,
        records,
        live,
        replayed,
    })
}

fn next_event(reader: &mut JournalReader) -> Result<Event> {
    Ok(reader
        .next_record()?
        .context("event missing from journal before socket write")?
        .event)
}

fn read_upgrade_request(peer: &mut PeerStream) -> Result<String> {
    read_upgrade_request_for_host(peer, HOST)
}

fn read_upgrade_request_for_host(peer: &mut PeerStream, host: &str) -> Result<String> {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        ensure!(request.len() < 4096, "test upgrade request is too long");
        let mut byte = [0];
        peer.read_exact(&mut byte)?;
        request.push(byte[0]);
    }
    let request = String::from_utf8(request)?;
    assert!(request.starts_with(&format!("GET /ws HTTP/1.1\r\nHost: {host}\r\n")));
    Ok(request)
}

fn upgrade_response(request: &str) -> Result<Vec<u8>> {
    let key = request
        .lines()
        .find_map(|line| line.strip_prefix("Sec-WebSocket-Key: "))
        .context("test client omitted Sec-WebSocket-Key")?;
    let accept = STANDARD.encode(Sha1::digest(format!(
        "{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    )));
    Ok(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").into_bytes())
}

fn server_frame(flags_and_opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![flags_and_opcode];
    if payload.len() < 126 {
        frame.push(payload.len() as u8);
    } else {
        frame.push(126);
        frame.extend(
            u16::try_from(payload.len())
                .expect("test payload is too long")
                .to_be_bytes(),
        );
    }
    frame.extend_from_slice(payload);
    frame
}

fn read_client_frame(peer: &mut PeerStream) -> Result<(u8, Vec<u8>)> {
    let mut header = [0; 2];
    peer.read_exact(&mut header)?;
    ensure!(header[1] & 0x80 != 0, "expected a masked client frame");
    let length = match header[1] & 0x7f {
        126 => {
            let mut length = [0; 2];
            peer.read_exact(&mut length)?;
            usize::from(u16::from_be_bytes(length))
        }
        127 => bail!("test client sent a long frame"),
        length => usize::from(length),
    };
    let mut key = [0; 4];
    peer.read_exact(&mut key)?;
    let mut payload = vec![0; length];
    peer.read_exact(&mut payload)?;
    for (byte, mask) in payload.iter_mut().zip(key.into_iter().cycle()) {
        *byte ^= mask;
    }
    Ok((header[0], payload))
}

mod order_tests;
