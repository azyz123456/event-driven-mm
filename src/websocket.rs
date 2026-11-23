use std::mem;
use std::str;

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};
use websocket_sans_io::{
    FrameInfo, Opcode, WebsocketFrameDecoder, WebsocketFrameEncoder, WebsocketFrameEvent,
};

use crate::event::Message;

const ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const MAX_MESSAGE_BYTES: u64 = 1024 * 1024;

pub fn upgrade_request(host: &str, path: &str) -> Result<(String, String)> {
    let key = STANDARD.encode(random_bytes::<16>()?);
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\
         User-Agent: event-driven-mm/{}\r\n\r\n",
        env!("CARGO_PKG_VERSION")
    );
    Ok((request, key))
}

pub fn validate_upgrade(bytes: &[u8], key: &str) -> Result<()> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut response = httparse::Response::new(&mut headers);
    ensure!(
        response.parse(bytes)?.is_complete(),
        "incomplete upgrade response"
    );
    ensure!(
        response.version == Some(1) && response.code == Some(101),
        "WebSocket upgrade failed with HTTP status {:?}",
        response.code
    );

    let has_token = |name: &str, token: &str| {
        response.headers.iter().any(|header| {
            header.name.eq_ignore_ascii_case(name)
                && str::from_utf8(header.value).is_ok_and(|value| {
                    value
                        .split(',')
                        .any(|part| part.trim().eq_ignore_ascii_case(token))
                })
        })
    };
    ensure!(
        has_token("Upgrade", "websocket"),
        "missing WebSocket upgrade token"
    );
    ensure!(
        has_token("Connection", "upgrade"),
        "missing connection upgrade token"
    );

    let mut sha1 = Sha1::new();
    sha1.update(key);
    sha1.update(ACCEPT_GUID);
    let expected = STANDARD.encode(sha1.finalize());
    let mut accept = response
        .headers
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case("Sec-WebSocket-Accept"));
    ensure!(
        accept
            .next()
            .is_some_and(|header| header.value == expected.as_bytes())
            && accept.next().is_none(),
        "invalid Sec-WebSocket-Accept"
    );
    ensure!(
        !response.headers.iter().any(|header| {
            header.name.eq_ignore_ascii_case("Sec-WebSocket-Extensions")
                || header.name.eq_ignore_ascii_case("Sec-WebSocket-Protocol")
        }),
        "server selected an unrequested WebSocket extension or subprotocol"
    );
    Ok(())
}

pub fn encode(message: &Message) -> Result<Vec<u8>> {
    let (opcode, payload) = match message {
        Message::Text(text) => (Opcode::Text, text.as_bytes()),
        Message::Binary(bytes) => (Opcode::Binary, bytes.as_slice()),
        Message::Ping(bytes) => (Opcode::Ping, bytes.as_slice()),
        Message::Pong(bytes) => (Opcode::Pong, bytes.as_slice()),
        Message::Close(bytes) => (Opcode::ConnectionClose, bytes.as_slice()),
    };

    let mut encoder = WebsocketFrameEncoder::new();
    let header = encoder.start_frame(&FrameInfo {
        opcode,
        payload_length: payload.len() as u64,
        mask: Some(random_bytes()?),
        fin: true,
        reserved: 0,
    });
    let mut bytes = Vec::with_capacity(header.len() + payload.len());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(payload);
    encoder.transform_frame_payload(&mut bytes[header.len()..]);
    Ok(bytes)
}

pub struct MessageDecoder {
    frames: WebsocketFrameDecoder,
    // The frame codec exposes the decoded length but not its wire width.
    header_bytes: usize,
    data: Vec<u8>,
    control: Vec<u8>,
    fragmented: bool,
}

impl MessageDecoder {
    pub fn new() -> Self {
        Self {
            frames: WebsocketFrameDecoder::new(),
            header_bytes: 0,
            data: Vec::new(),
            control: Vec::new(),
            fragmented: false,
        }
    }

    pub fn next(&mut self, bytes: &mut [u8]) -> Result<(usize, Option<Message>)> {
        let mut used = 0;
        loop {
            let result = match self.frames.add_data(&mut bytes[used..]) {
                Ok(result) => result,
                Err(error) => match error {},
            };
            let start = used;
            used += result.consumed_bytes;
            match result.event {
                Some(WebsocketFrameEvent::Start { frame_info, .. }) => {
                    self.header_bytes += result.consumed_bytes;
                    self.start_frame(frame_info)?;
                    self.header_bytes = 0;
                }
                Some(WebsocketFrameEvent::PayloadChunk { original_opcode }) => {
                    let buffer = if original_opcode.is_data() {
                        &mut self.data
                    } else {
                        &mut self.control
                    };
                    buffer.extend_from_slice(&bytes[start..used]);
                }
                Some(WebsocketFrameEvent::End {
                    frame_info,
                    original_opcode,
                }) if frame_info.fin => {
                    return Ok((used, Some(self.finish_message(original_opcode)?)));
                }
                Some(WebsocketFrameEvent::End { .. }) => {}
                None => {
                    self.header_bytes += result.consumed_bytes;
                    return Ok((used, None));
                }
            }
        }
    }

    fn start_frame(&mut self, frame: FrameInfo) -> Result<()> {
        ensure!(frame.is_reasonable(), "invalid WebSocket frame header");
        ensure!(frame.mask.is_none(), "WebSocket server sent a masked frame");
        let expected_header_bytes = match frame.payload_length {
            0..=125 => 2,
            126..=65535 => 4,
            _ => 10,
        };
        ensure!(
            self.header_bytes == expected_header_bytes,
            "nonminimal WebSocket payload length encoding"
        );
        if frame.opcode.is_control() {
            ensure!(
                frame.payload_length <= 125,
                "oversized WebSocket control frame"
            );
            return Ok(());
        }

        ensure!(
            (frame.opcode == Opcode::Continuation) == self.fragmented,
            "invalid WebSocket fragmentation sequence"
        );
        self.fragmented = !frame.fin;
        ensure!(
            frame.payload_length <= MAX_MESSAGE_BYTES - self.data.len() as u64,
            "WebSocket message exceeds {MAX_MESSAGE_BYTES} bytes"
        );
        Ok(())
    }

    pub fn finish(&self) -> Result<()> {
        ensure!(
            self.header_bytes == 0 && self.frames.eof_valid() && !self.fragmented,
            "incomplete WebSocket message at EOF"
        );
        Ok(())
    }

    fn finish_message(&mut self, opcode: Opcode) -> Result<Message> {
        Ok(match opcode {
            Opcode::Text => Message::Text(
                String::from_utf8(mem::take(&mut self.data)).context("invalid WebSocket text")?,
            ),
            Opcode::Binary => Message::Binary(mem::take(&mut self.data)),
            Opcode::Ping => Message::Ping(mem::take(&mut self.control)),
            Opcode::Pong => Message::Pong(mem::take(&mut self.control)),
            Opcode::ConnectionClose => {
                let payload = mem::take(&mut self.control);
                if !payload.is_empty() {
                    ensure!(payload.len() >= 2, "incomplete WebSocket close status");
                    let status = u16::from_be_bytes([payload[0], payload[1]]);
                    ensure!(
                        matches!(status, 1000..=1003 | 1007..=1014 | 3000..=4999),
                        "invalid WebSocket close status {status}"
                    );
                    str::from_utf8(&payload[2..]).context("invalid WebSocket close reason")?;
                }
                Message::Close(payload)
            }
            _ => bail!("unexpected WebSocket message opcode {opcode:?}"),
        })
    }
}

fn random_bytes<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(|error| anyhow::anyhow!("OS randomness: {error}"))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembles_split_frames_around_an_interleaved_ping() -> Result<()> {
        let mut wire = [0x01, 2, b'h', 0xc3, 0x89, 0, 0x80, 1, 0xa9];
        let mut messages = Vec::new();
        let mut decoder = MessageDecoder::new();
        for byte in &mut wire {
            let mut input = std::slice::from_mut(byte);
            loop {
                let (used, message) = decoder.next(input)?;
                input = &mut input[used..];
                match message {
                    Some(message) => messages.push(message),
                    None => {
                        assert!(input.is_empty());
                        break;
                    }
                }
            }
        }
        assert_eq!(
            messages,
            [Message::Ping(vec![]), Message::Text("hé".into())]
        );
        Ok(())
    }

    #[test]
    fn validates_the_standard_upgrade_example() -> Result<()> {
        let response = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\
            Connection: keep-alive, Upgrade\r\n\
            Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
        validate_upgrade(response, "dGhlIHNhbXBsZSBub25jZQ==")?;
        assert!(validate_upgrade(response, "wrong key").is_err());
        Ok(())
    }

    #[test]
    fn rejects_invalid_server_frames() {
        let cases: &[(&[u8], &str)] = &[
            (&[0x81, 0x80, 1, 2, 3, 4], "server sent a masked frame"),
            (&[0xc1, 0], "invalid WebSocket frame header"),
            (&[0x09, 0], "invalid WebSocket frame header"),
            (&[0x80, 0], "invalid WebSocket fragmentation sequence"),
            (&[0x89, 126, 0, 126], "oversized WebSocket control frame"),
            (&[0x81, 1, 0xff], "invalid WebSocket text"),
            (&[0x88, 1, 0], "incomplete WebSocket close status"),
            (&[0x88, 2, 3, 237], "invalid WebSocket close status"),
            (&[0x82, 127, 0, 0, 0, 0, 0, 16, 0, 1], "message exceeds"),
        ];
        for (wire, expected) in cases {
            let error = MessageDecoder::new().next(&mut wire.to_vec()).unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "{wire:02x?}: {error:#}"
            );
        }
    }

    #[test]
    fn rejects_nonminimal_payload_lengths_even_when_the_header_is_split() -> Result<()> {
        let mut decoder = MessageDecoder::new();
        assert_eq!(decoder.next(&mut [0x81, 126, 0])?, (3, None));
        let result = decoder.next(&mut [1, b'a']);
        assert!(matches!(result, Err(error) if error.to_string().contains("nonminimal")));

        let mut decoder = MessageDecoder::new();
        let result = decoder.next(&mut [0x82, 127, 0, 0, 0, 0, 0, 0, 0, 126]);
        assert!(matches!(result, Err(error) if error.to_string().contains("nonminimal")));
        Ok(())
    }

    #[test]
    fn accepts_a_canonical_64_bit_payload_length() -> Result<()> {
        let mut decoder = MessageDecoder::new();
        let length = 65_536_u64;
        let mut header = [0; 10];
        header[..2].copy_from_slice(&[0x82, 127]);
        header[2..].copy_from_slice(&length.to_be_bytes());
        assert_eq!(decoder.next(&mut header)?, (header.len(), None));

        let mut payload = vec![42; length as usize];
        let (used, message) = decoder.next(&mut payload)?;
        assert_eq!(used, payload.len());
        assert_eq!(message, Some(Message::Binary(payload)));
        decoder.finish()
    }

    #[test]
    fn rejects_eof_mid_header_payload_or_fragmented_message() -> Result<()> {
        let prefixes: &[&[u8]] = &[&[0x81], &[0x81, 2, b'a'], &[0x01, 1, b'a']];
        for prefix in prefixes {
            let mut decoder = MessageDecoder::new();
            assert_eq!(decoder.next(&mut prefix.to_vec())?, (prefix.len(), None));
            assert!(decoder.finish().is_err(), "{prefix:02x?}");
        }
        Ok(())
    }
}
