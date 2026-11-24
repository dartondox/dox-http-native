//! HTTP/2: the protocol state with no socket, and the client against a
//! small HTTP/2 server.
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use super::super::h2::{Event, H2, WINDOW, frame, hpack};
use super::*;

type Fields = Vec<(String, String)>;

fn field<'a>(fields: &'a Fields, name: &str) -> &'a str {
    fields
        .iter()
        .find(|(n, _)| n == name)
        .map_or("", |(_, v)| v.as_str())
}

fn block(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, value) in fields {
        hpack::encode_field(name.as_bytes(), value.as_bytes(), &mut out);
    }
    out
}

// ---- the protocol state, with no socket ---------------------------------

const GET: &[(&[u8], &[u8])] = &[(b":method", b"GET"), (b":path", b"/")];

/// The frames of a server: SETTINGS first.
fn hello() -> Vec<u8> {
    let mut b = Vec::new();
    frame(&mut b, 4, 0, 0, &[]);
    b
}

fn head_frame(b: &mut Vec<u8>, stream: u32, flags: u8, fields: &[(&str, &str)]) {
    frame(b, 1, flags, stream, &block(fields));
}

#[test]
fn proto_an_answer_in_any_split() {
    let mut b = hello();
    // 100 Continue, then the head in 2 frames, a padded DATA, trailers
    head_frame(&mut b, 1, 4, &[(":status", "100")]);
    let fields = block(&[(":status", "200"), ("x-a", "1"), ("x-b", " two ")]);
    frame(&mut b, 1, 0, 1, &fields[..3]);
    frame(&mut b, 9, 4, 1, &fields[3..]);
    frame(&mut b, 0, 8, 1, b"\x03hello---");
    frame(&mut b, 0x55, 0, 1, b"a frame type nobody knows");
    frame(&mut b, 0, 0, 1, b" world");
    head_frame(&mut b, 1, 5, &[("x-trailer", "t")]);
    let want = vec![
        Event::Head {
            stream: 1,
            status: 200,
            lines: b"x-a:1\nx-b:two\n".to_vec(),
        },
        Event::Data {
            stream: 1,
            bytes: b"hello".to_vec(),
        },
        Event::Data {
            stream: 1,
            bytes: b" world".to_vec(),
        },
        Event::End { stream: 1 },
    ];
    let mut whole = H2::new();
    assert!(whole.out.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"));
    assert_eq!(whole.open(GET, true), 1);
    assert_eq!(whole.recv(&b), Ok(b.len()));
    assert_eq!(whole.events, want);
    assert_eq!(whole.active(), 0);
    // byte by byte
    let mut parts = H2::new();
    parts.open(GET, true);
    let mut buf = Vec::new();
    for &byte in &b {
        buf.push(byte);
        let used = parts.recv(&buf).unwrap();
        buf.drain(..used);
    }
    assert!(buf.is_empty());
    assert_eq!(parts.events, want);
}

#[test]
fn proto_the_request_body_waits_for_the_windows() {
    let mut h = H2::new();
    let stream = h.open(GET, false);
    h.out.clear();
    h.send(stream, &[7u8; 100_000], true);
    // the window of a connection and of a stream starts at 65535
    assert_eq!(h.waiting(stream), 100_000 - 65_535);
    assert_eq!(
        h.events.last(),
        Some(&Event::Wrote {
            stream,
            total: 65_535
        })
    );
    // room for the stream only: the connection has none
    let mut b = hello();
    frame(&mut b, 8, 0, stream, &50_000u32.to_be_bytes());
    h.recv(&b).unwrap();
    assert_eq!(h.waiting(stream), 100_000 - 65_535);
    let mut b = Vec::new();
    frame(&mut b, 8, 0, 0, &20_000u32.to_be_bytes());
    h.recv(&b).unwrap();
    assert_eq!(h.waiting(stream), 100_000 - 85_535);
    // SETTINGS_INITIAL_WINDOW_SIZE changes the open streams too
    let mut b = Vec::new();
    frame(&mut b, 8, 0, 0, &1_000_000u32.to_be_bytes());
    frame(&mut b, 4, 0, 0, &[0, 4, 0, 1, 0, 0]);
    h.recv(&b).unwrap();
    assert_eq!(h.waiting(stream), 0);
    assert_eq!(
        h.events.last(),
        Some(&Event::Wrote {
            stream,
            total: 100_000
        })
    );
    // the last DATA frame has END_STREAM
    let n = h.out.len();
    let last = 100_000 - 85_535;
    assert_eq!(h.out[n - last - 9 + 4], 1);
}

#[test]
fn proto_a_lot_of_request_body_stops_at_the_out_limit() {
    let mut h = H2::new();
    h.recv(&{
        let mut b = hello();
        frame(&mut b, 4, 0, 0, &[0, 4, 0x7f, 0xff, 0xff, 0xff]);
        frame(&mut b, 8, 0, 0, &0x7000_0000u32.to_be_bytes());
        b
    })
    .unwrap();
    let stream = h.open(GET, false);
    h.send(stream, &vec![1u8; 2 << 20], false);
    assert!(h.out.len() < 300 * 1024, "{}", h.out.len());
    let mut total = 0;
    while h.waiting(stream) > 0 {
        total += h.out.len();
        h.out.clear();
        h.flush();
    }
    total += h.out.len();
    assert!(total > 2 << 20);
}

#[test]
fn proto_stream_errors_keep_the_connection() {
    let mut h = H2::new();
    for _ in 0..5 {
        h.open(GET, true);
    }
    let mut b = hello();
    // DATA before the head
    frame(&mut b, 0, 0, 1, b"x");
    // an upper case name, and no `:status`
    head_frame(&mut b, 3, 4, &[(":status", "200"), ("X-A", "1")]);
    head_frame(&mut b, 5, 4, &[("x-a", "1")]);
    // more DATA than the window of the stream
    head_frame(&mut b, 7, 4, &[(":status", "200")]);
    for _ in 0..(WINDOW as usize / 16_384 + 1) {
        frame(&mut b, 0, 0, 7, &[0u8; 16_384]);
    }
    // the server refuses one, and stops another one
    frame(&mut b, 3, 0, 9, &7u32.to_be_bytes());
    assert_eq!(h.recv(&b), Ok(b.len()));
    let resets: Vec<(u32, u16, bool)> = h
        .events
        .iter()
        .filter_map(|e| match e {
            Event::Reset {
                stream,
                error,
                retry,
            } => Some((*stream, *error, *retry)),
            _ => None,
        })
        .collect();
    assert_eq!(
        resets,
        [
            (1, E_BAD, false),
            (3, E_BAD, false),
            (5, E_BAD, false),
            (7, E_BAD, false),
            (9, E_CLOSED, true)
        ]
    );
    assert_eq!(h.active(), 0);
    assert!(h.capacity());
    // frames for the streams that are gone are fine
    let mut b = Vec::new();
    frame(&mut b, 0, 1, 7, b"late");
    frame(&mut b, 3, 0, 1, &8u32.to_be_bytes());
    head_frame(&mut b, 3, 5, &[(":status", "200")]);
    assert_eq!(h.recv(&b), Ok(b.len()));
}

#[test]
fn proto_connection_errors() {
    let bad: Vec<Vec<u8>> = vec![
        // no SETTINGS first
        {
            let mut b = Vec::new();
            frame(&mut b, 6, 0, 0, &[0; 8]);
            b
        },
        // PUSH_PROMISE
        {
            let mut b = hello();
            frame(&mut b, 5, 4, 1, &[0, 0, 0, 2]);
            b
        },
        // a frame over 16384 bytes
        {
            let mut b = hello();
            b.extend_from_slice(&[0, 0x40, 1, 0, 0, 0, 0, 0, 1]);
            b
        },
        // a header block that is not HPACK
        {
            let mut b = hello();
            frame(&mut b, 1, 4, 1, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
            b
        },
        // a frame inside a header block
        {
            let mut b = hello();
            frame(&mut b, 1, 0, 1, &block(&[(":status", "200")]));
            frame(&mut b, 6, 0, 0, &[0; 8]);
            b
        },
        // a stream that was never open
        {
            let mut b = hello();
            frame(&mut b, 0, 0, 99, b"x");
            b
        },
        // a window over 2^31 - 1
        {
            let mut b = hello();
            frame(&mut b, 8, 0, 0, &0x7fff_ffffu32.to_be_bytes());
            b
        },
        // padding longer than the frame
        {
            let mut b = hello();
            frame(&mut b, 0, 8, 1, &[9, 1, 2]);
            b
        },
    ];
    for (i, b) in bad.iter().enumerate() {
        let mut h = H2::new();
        h.open(GET, true);
        h.out.clear();
        assert_eq!(h.recv(b), Err(()), "{i}");
        // GOAWAY is the last frame
        let n = h.out.len();
        assert_eq!(h.out[n - 17 + 3], 7, "{i}");
        assert!(!h.capacity());
        assert_eq!(h.recv(&[]), Err(()));
    }
}

#[test]
fn proto_ping_settings_and_goaway() {
    let mut h = H2::new();
    for _ in 0..3 {
        h.open(GET, true);
    }
    h.out.clear();
    let mut b = hello();
    frame(&mut b, 6, 0, 0, b"12345678");
    frame(&mut b, 6, 1, 0, b"abcdefgh");
    // SETTINGS_MAX_CONCURRENT_STREAMS 3
    frame(&mut b, 4, 0, 0, &[0, 3, 0, 0, 0, 3]);
    assert_eq!(h.recv(&b), Ok(b.len()));
    let mut want = Vec::new();
    frame(&mut want, 4, 1, 0, &[]);
    frame(&mut want, 6, 1, 0, b"12345678");
    frame(&mut want, 4, 1, 0, &[]);
    assert_eq!(h.out, want);
    assert!(!h.capacity());
    // GOAWAY: the server worked on stream 1 only
    let mut b = Vec::new();
    frame(&mut b, 7, 0, 0, &[0, 0, 0, 1, 0, 0, 0, 0]);
    h.recv(&b).unwrap();
    assert!(h.goaway);
    assert_eq!(
        h.events,
        [
            Event::Reset {
                stream: 3,
                error: E_CLOSED,
                retry: true
            },
            Event::Reset {
                stream: 5,
                error: E_CLOSED,
                retry: true
            }
        ]
    );
    assert_eq!(h.active(), 1);
}

#[test]
fn proto_release_gives_the_server_room() {
    let mut h = H2::new();
    let stream = h.open(GET, true);
    let mut b = hello();
    head_frame(&mut b, stream, 4, &[(":status", "200")]);
    h.recv(&b).unwrap();
    h.out.clear();
    // small parts: no frame for each one
    h.release(stream, 1000);
    assert!(h.out.is_empty());
    h.release(stream, WINDOW as u64 / 2);
    let mut want = Vec::new();
    frame(&mut want, 8, 0, stream, &(WINDOW / 2 + 1000).to_be_bytes());
    assert_eq!(h.out, want);
    // an answer before the end of the request: RST_STREAM
    let stream = h.open(GET, false);
    h.out.clear();
    let mut b = Vec::new();
    head_frame(&mut b, stream, 5, &[(":status", "413")]);
    h.recv(&b).unwrap();
    let mut want = Vec::new();
    frame(&mut want, 3, 0, stream, &8u32.to_be_bytes());
    assert_eq!(h.out, want);
}

#[test]
fn the_fields_of_a_request_head() {
    let head = b"POST /a?b=1 HTTP/1.1\r\nHost: example.com:8443\r\nconnection: close\r\nx-name:  Dox \r\ntransfer-encoding: chunked\r\ncontent-type: text/plain\r\n\r\nbody";
    let (len, fields) = h2_fields(head).unwrap();
    assert_eq!(&head[len..], b"body");
    let text: Vec<(String, String)> = fields
        .iter()
        .map(|(n, v)| {
            (
                String::from_utf8(n.clone()).unwrap(),
                String::from_utf8(v.clone()).unwrap(),
            )
        })
        .collect();
    let want = [
        (":method", "POST"),
        (":scheme", "https"),
        (":authority", "example.com:8443"),
        (":path", "/a?b=1"),
        ("x-name", "Dox"),
        ("content-type", "text/plain"),
    ];
    assert_eq!(text.len(), want.len());
    for ((n, v), (wn, wv)) in text.iter().zip(want) {
        assert_eq!((n.as_str(), v.as_str()), (wn, wv));
    }
    assert!(h2_fields(b"GET / HTTP/1.1\r\nhost: x\r\n").is_none());
    assert!(h2_fields(b"GET / HTTP/1.1\r\nx: y\r\n\r\n").is_none());
    assert!(h2_fields(b"GET / HTTP/1.1\r\nno colon\r\n\r\n").is_none());
    assert_eq!(coding_of(b"a:b\ncontent-encoding: GZIP\n"), Coding::Gzip);
    assert_eq!(coding_of(b"content-encoding:br\n"), Coding::Brotli);
    assert_eq!(coding_of(b"content-encoding:gzip, br\n"), Coding::Raw);
    assert_eq!(
        coding_of(b"content-encoding:gzip\ncontent-encoding:br\n"),
        Coding::Raw
    );
}

// ---- a small HTTP/2 server ----------------------------------------------

enum In {
    Head {
        stream: u32,
        fields: Fields,
        end: bool,
    },
    Data {
        stream: u32,
        bytes: Vec<u8>,
        end: bool,
    },
    Rst {
        stream: u32,
        code: u32,
    },
    /// the client gave room (WINDOW_UPDATE): only `data` waits for it
    Room,
}

/// The server side of one HTTP/2 connection.
struct Peer {
    tls: rustls::StreamOwned<rustls::ServerConnection, StdStream>,
    dec: hpack::Decoder,
    /// the room the client gave for the answers
    conn_window: i64,
    start_window: i64,
    windows: HashMap<u32, i64>,
    /// frames that `data` read while it waited for room
    held: VecDeque<In>,
    /// true: the client gets no room for its request bodies
    hold: bool,
    /// body bytes of answers that went out
    sent: Arc<AtomicUsize>,
}

impl Peer {
    fn put(&mut self, ty: u8, flags: u8, stream: u32, payload: &[u8]) -> bool {
        let mut b = Vec::new();
        frame(&mut b, ty, flags, stream, payload);
        self.tls.write_all(&b).is_ok() && self.tls.flush().is_ok()
    }

    fn frame_in(&mut self) -> Option<(u8, u8, u32, Vec<u8>)> {
        let mut head = [0u8; 9];
        self.tls.read_exact(&mut head).ok()?;
        let len = (head[0] as usize) << 16 | (head[1] as usize) << 8 | head[2] as usize;
        let mut payload = vec![0u8; len];
        self.tls.read_exact(&mut payload).ok()?;
        let stream = u32::from_be_bytes([head[5], head[6], head[7], head[8]]) & 0x7fff_ffff;
        Some((head[3], head[4], stream, payload))
    }

    /// The next frame of the client that the test has to see. None: the
    /// client is gone, or it said GOAWAY.
    fn next(&mut self) -> Option<In> {
        loop {
            let frame = match self.held.pop_front() {
                Some(frame) => frame,
                None => self.read()?,
            };
            if !matches!(frame, In::Room) {
                return Some(frame);
            }
        }
    }

    fn read(&mut self) -> Option<In> {
        loop {
            let (ty, flags, stream, p) = self.frame_in()?;
            match ty {
                4 if flags & 1 == 0 => {
                    for setting in p.as_chunks::<6>().0 {
                        if setting[1] == 4 {
                            let value = [setting[2], setting[3], setting[4], setting[5]];
                            self.start_window = u32::from_be_bytes(value) as i64;
                        }
                    }
                    self.put(4, 1, 0, &[]);
                }
                8 => {
                    let n = (u32::from_be_bytes([p[0], p[1], p[2], p[3]]) & 0x7fff_ffff) as i64;
                    if stream == 0 {
                        self.conn_window += n;
                    } else {
                        *self.windows.entry(stream).or_insert(self.start_window) += n;
                    }
                    return Some(In::Room);
                }
                1 => {
                    let mut block = p;
                    let mut last = flags;
                    while last & 4 == 0 {
                        let (ty, flags, _, more) = self.frame_in()?;
                        assert_eq!(ty, 9);
                        block.extend_from_slice(&more);
                        last = flags;
                    }
                    let mut fields = Vec::new();
                    self.dec
                        .decode(&block, usize::MAX, |n, v| {
                            fields.push((
                                String::from_utf8_lossy(n).into_owned(),
                                String::from_utf8_lossy(v).into_owned(),
                            ));
                        })
                        .unwrap();
                    self.windows.entry(stream).or_insert(self.start_window);
                    return Some(In::Head {
                        stream,
                        fields,
                        end: flags & 1 != 0,
                    });
                }
                0 => {
                    // the room is free again at once
                    if !p.is_empty() && !self.hold {
                        let n = (p.len() as u32).to_be_bytes();
                        self.put(8, 0, 0, &n);
                        if flags & 1 == 0 {
                            self.put(8, 0, stream, &n);
                        }
                    }
                    return Some(In::Data {
                        stream,
                        bytes: p,
                        end: flags & 1 != 0,
                    });
                }
                3 => {
                    return Some(In::Rst {
                        stream,
                        code: u32::from_be_bytes([p[0], p[1], p[2], p[3]]),
                    });
                }
                6 if flags & 1 == 0 => {
                    self.put(6, 1, 0, &p);
                }
                7 => return None,
                _ => {}
            }
        }
    }

    /// Reads one whole request: its stream, its fields and its body.
    fn request(&mut self) -> Option<(u32, Fields, Vec<u8>)> {
        loop {
            let In::Head {
                stream,
                fields,
                mut end,
            } = self.next()?
            else {
                continue;
            };
            let mut body = Vec::new();
            while !end {
                match self.next()? {
                    In::Data {
                        stream: s,
                        bytes,
                        end: last,
                    } if s == stream => {
                        body.extend_from_slice(&bytes);
                        end = last;
                    }
                    _ => {}
                }
            }
            return Some((stream, fields, body));
        }
    }

    fn head(&mut self, stream: u32, status: &str, fields: &[(&str, &str)], end: bool) -> bool {
        let mut all = vec![(":status", status)];
        all.extend_from_slice(fields);
        self.put(1, 4 | end as u8, stream, &block(&all))
    }

    /// Body bytes of an answer, as far as the client gives room. False:
    /// the client stopped the stream, or it is gone.
    fn data(&mut self, stream: u32, bytes: &[u8], end: bool) -> bool {
        if bytes.is_empty() {
            return !end || self.put(0, 1, stream, &[]);
        }
        let mut at = 0;
        while at < bytes.len() {
            let window = *self.windows.get(&stream).unwrap_or(&self.start_window);
            let room = self.conn_window.min(window);
            if room <= 0 {
                match self.read() {
                    Some(In::Rst { stream: s, .. }) if s == stream => return false,
                    Some(In::Room) => {}
                    Some(other) => self.held.push_back(other),
                    None => return false,
                }
                continue;
            }
            let n = (bytes.len() - at).min(16_384).min(room as usize);
            let last = end && at + n == bytes.len();
            if !self.put(0, last as u8, stream, &bytes[at..at + n]) {
                return false;
            }
            at += n;
            self.conn_window -= n as i64;
            *self.windows.entry(stream).or_insert(self.start_window) -= n as i64;
            self.sent.fetch_add(n, Ordering::SeqCst);
        }
        true
    }

    fn answer(&mut self, stream: u32, status: &str, fields: &[(&str, &str)], body: &[u8]) {
        self.head(stream, status, fields, body.is_empty());
        if !body.is_empty() {
            self.data(stream, body, true);
        }
    }

    /// Reads until the client is gone.
    fn idle(&mut self) {
        while self.next().is_some() {}
    }
}

struct Server {
    addr: SocketAddr,
    pem: String,
    /// connections so far
    count: Arc<AtomicUsize>,
    /// body bytes of answers that went out
    sent: Arc<AtomicUsize>,
}

/// An HTTP/2 server (TLS with ALPN `h2`) for `localhost`. `serve` gets each
/// connection and its number. `settings` are in its first frame.
fn h2_server(
    settings: &'static [(u16, u32)],
    serve: impl Fn(&mut Peer, usize) + Send + Sync + 'static,
) -> Server {
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let pem = made.cert.pem();
    let key = rustls::pki_types::PrivateKeyDer::try_from(made.signing_key.serialize_der()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![made.cert.der().clone()], key)
        .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let config = Arc::new(config);
    let sent = Arc::new(AtomicUsize::new(0));
    let total = sent.clone();
    let (addr, count) = server(move |s, n| {
        let conn = rustls::ServerConnection::new(config.clone()).unwrap();
        let mut peer = Peer {
            tls: rustls::StreamOwned::new(conn, s),
            dec: hpack::Decoder::new(4096),
            conn_window: 65_535,
            start_window: 65_535,
            windows: HashMap::new(),
            held: VecDeque::new(),
            hold: false,
            sent: total.clone(),
        };
        let mut preface = [0u8; 24];
        if peer.tls.read_exact(&mut preface).is_err() {
            return;
        }
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        let mut payload = Vec::new();
        for (id, value) in settings {
            payload.extend_from_slice(&id.to_be_bytes());
            payload.extend_from_slice(&value.to_be_bytes());
        }
        peer.put(4, 0, 0, &payload);
        serve(&mut peer, n);
        peer.tls.conn.send_close_notify();
        let _ = peer.tls.flush();
    });
    Server {
        addr,
        pem,
        count,
        sent,
    }
}

fn h2_core(pem: &str) -> Core {
    Core::new(0, 0, None, tls_config(pem.as_bytes(), true).unwrap()).unwrap()
}

/// A request record for `localhost` over TLS.
fn req(id: u64, addr: SocketAddr, flags: u8, timeout_ms: u32, request: &str) -> Vec<u8> {
    named(
        id,
        addr,
        "localhost",
        flags | TLS,
        timeout_ms,
        request.as_bytes(),
    )
}

fn get_path(id: u64, addr: SocketAddr, path: &str) -> Vec<u8> {
    let request = format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n");
    req(id, addr, AGAIN, 5000, &request)
}

/// The HTTP/2 connections of `core` and the open requests of each.
fn shared(core: &Core) -> Vec<usize> {
    core.conns
        .iter()
        .filter_map(|(_, c)| c.h2.as_ref())
        .map(|mux| mux.subs.len())
        .collect()
}

/// Answers each request with its path and the length of its body.
fn echo(peer: &mut Peer, _: usize) {
    while let Some((stream, fields, body)) = peer.request() {
        let text = format!(
            "{} {} {}",
            field(&fields, ":method"),
            field(&fields, ":path"),
            body.len()
        );
        peer.answer(stream, "200", &[("x-a", "1")], text.as_bytes());
    }
}

#[test]
fn h2_requests_share_one_connection() {
    let seen: Arc<Mutex<Vec<Fields>>> = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let s = h2_server(&[], move |peer, _| {
        while let Some((stream, fields, body)) = peer.request() {
            let text = format!("hello {} {}", field(&fields, ":path"), body.len());
            log.lock().unwrap().push(fields);
            let fields = [("x-a", "1"), ("content-type", "text/plain")];
            peer.answer(stream, "200", &fields, text.as_bytes());
        }
    });
    let mut core = h2_core(&s.pem);
    for id in 1..=3 {
        let request = format!(
            "GET /p{id}?q=1 HTTP/1.1\r\nhost: localhost:8443\r\nconnection: keep-alive\r\nx-name: Dox\r\naccept-encoding: gzip, br\r\n\r\n"
        );
        let a = run_until(&mut core, &req(id, s.addr, AGAIN, 5000, &request), 1);
        assert_eq!((a[0].status, a[0].error), (200, 0));
        assert_eq!(a[0].body, format!("hello /p{id}?q=1 0").as_bytes());
        assert_eq!(a[0].lines, "x-a:1\ncontent-type:text/plain\n");
    }
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
    assert_eq!(shared(&core), [0]);
    assert_eq!(core.open, 0);
    let seen = seen.lock().unwrap();
    let names: Vec<&str> = seen[0].iter().map(|(n, _)| n.as_str()).collect();
    let want = [
        ":method",
        ":scheme",
        ":authority",
        ":path",
        "x-name",
        "accept-encoding",
    ];
    assert_eq!(names, want);
    assert_eq!(field(&seen[0], ":authority"), "localhost:8443");
    assert_eq!(field(&seen[0], ":scheme"), "https");
    assert_eq!(field(&seen[2], ":path"), "/p3?q=1");
}

#[test]
fn h2_many_requests_at_the_same_time_on_one_connection() {
    let s = h2_server(&[], |peer, _| {
        let (stream, _, _) = peer.request().unwrap();
        peer.answer(stream, "200", &[], b"first");
        // all of them are open before one gets its answer
        let mut open = Vec::new();
        while open.len() < 20 {
            match peer.next() {
                Some(In::Head { stream, fields, .. }) => {
                    open.push((stream, field(&fields, ":path").to_string()));
                }
                Some(_) => {}
                None => return,
            }
        }
        // the last one first
        for (stream, path) in open.iter().rev() {
            peer.answer(*stream, "200", &[], path.as_bytes());
        }
        peer.idle();
    });
    let mut core = h2_core(&s.pem);
    run_until(&mut core, &get_path(1, s.addr, "/"), 1);
    let mut out = Vec::new();
    for id in 0..20 {
        out.extend(get_path(100 + id, s.addr, &format!("/n{id}")));
    }
    let answers = run_until(&mut core, &out, 20);
    assert_eq!(answers[0].id, 119);
    for a in &answers {
        assert_eq!(a.status, 200);
        assert_eq!(a.body, format!("/n{}", a.id - 100).as_bytes());
    }
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
    assert_eq!(core.open, 0);
}

#[test]
fn h2_big_bodies_go_through_the_windows() {
    let s = h2_server(&[], |peer, _| {
        while let Some((stream, _, body)) = peer.request() {
            // the body back, and more
            let mut answer = body;
            answer.extend_from_slice(&pattern(300_000));
            peer.answer(stream, "200", &[], &answer);
        }
    });
    let mut core = h2_core(&s.pem);
    let body = pattern(200_000);
    for id in 1..=2 {
        let mut request = format!(
            "POST /up HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        request.extend_from_slice(&body);
        let out = named(id, s.addr, "localhost", TLS, 10_000, &request);
        let a = run_until(&mut core, &out, 1);
        assert_eq!((a[0].status, a[0].error), (200, 0));
        assert_eq!(a[0].body.len(), 500_000);
        assert!(a[0].body[..200_000] == body[..]);
        assert!(a[0].body[200_000..] == pattern(300_000)[..]);
    }
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
}

#[test]
fn h2_coded_bodies_and_the_body_limit() {
    let s = h2_server(&[], |peer, _| {
        while let Some((stream, fields, _)) = peer.request() {
            let z = gzip(&b"zip ".repeat(5000));
            let coded = [("content-encoding", "gzip"), ("x-a", "1")];
            match field(&fields, ":path") {
                // a body for HEAD is not correct: the client drops it
                "/head" => peer.answer(stream, "200", &coded, b"not for HEAD"),
                "/bad" => peer.answer(stream, "200", &coded, b"this is not gzip"),
                "/moved" => peer.answer(stream, "302", &coded, b"moved"),
                _ => peer.answer(stream, "200", &coded, &z),
            }
        }
    });
    let mut core = Core::new(0, 20_000, None, tls_config(s.pem.as_bytes(), true).unwrap()).unwrap();
    let ask = |core: &mut Core, id: u64, path: &str, flags: u8| {
        let request = format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n");
        run_until(core, &req(id, s.addr, flags, 5000, &request), 1).remove(0)
    };
    let a = ask(&mut core, 1, "/", DECODE);
    assert_eq!(a.body, b"zip ".repeat(5000));
    assert_eq!(a.lines, "x-a:1\n");
    // without the flag the body stays as it is
    let a = ask(&mut core, 2, "/", 0);
    assert_eq!(a.body, gzip(&b"zip ".repeat(5000)));
    assert_eq!(a.lines, "content-encoding:gzip\nx-a:1\n");
    let a = ask(&mut core, 3, "/head", DECODE | HEAD_ONLY);
    assert_eq!((a.status, a.body.len()), (200, 0));
    assert_eq!(a.lines, "content-encoding:gzip\nx-a:1\n");
    assert_eq!(ask(&mut core, 4, "/bad", DECODE).error, E_BAD);
    let a = ask(&mut core, 5, "/moved", DECODE);
    assert_eq!((a.status, a.body.as_slice()), (302, &b"moved"[..]));
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
    // the limit is for the decoded bytes, and for the bytes as they come
    let mut small =
        Core::new(0, 19_999, None, tls_config(s.pem.as_bytes(), true).unwrap()).unwrap();
    assert_eq!(ask(&mut small, 1, "/", DECODE).error, E_BIG);
    let mut small = Core::new(0, 10, None, tls_config(s.pem.as_bytes(), true).unwrap()).unwrap();
    assert_eq!(ask(&mut small, 1, "/", 0).error, E_BIG);
    assert_eq!(ask(&mut small, 2, "/moved", 0).status, 302);
    assert_eq!(shared(&small), [0]);
}

#[test]
fn h2_a_paused_reader_stops_the_server() {
    const SIZE: usize = 4 << 20;
    let s = h2_server(&[], |peer, _| {
        while let Some((stream, _, _)) = peer.request() {
            peer.head(stream, "200", &[("x-a", "1")], false);
            peer.data(stream, &pattern(SIZE), true);
        }
    });
    let mut core = h2_core(&s.pem);
    let mut r = Reader::new(1);
    let out = req(
        1,
        s.addr,
        STREAM,
        5000,
        "GET / HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    // no credit: the reader uses nothing
    r.until(&mut core, &out, false, |r| r.body.len() >= WINDOW as usize);
    for _ in 0..100 {
        r.run(&mut core, &[]);
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(r.body.len(), WINDOW as usize);
    assert_eq!(s.sent.load(Ordering::SeqCst), WINDOW as usize);
    assert_eq!((r.status, r.lines.as_str()), (Some(200), "x-a:1\n"));
    // the reader uses the bytes: the rest comes
    r.until(&mut core, &[], true, |r| r.over());
    assert!(r.ended && r.body == pattern(SIZE));
    assert_eq!(core.open, 0);
    assert_eq!(shared(&core), [0]);
    // one more on the same connection
    let a = run_until(
        &mut core,
        &req(
            2,
            s.addr,
            STREAM,
            5000,
            "HEAD / HTTP/1.1\r\nhost: localhost\r\n\r\n",
        ),
        1,
    );
    assert_eq!(a[0].kind, R_HEAD);
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
}

#[test]
fn h2_a_streamed_gzip_answer_is_decoded_in_parts() {
    let mut seed = 7u32;
    let text: Vec<u8> = (0..2 << 20)
        .map(|_| {
            seed = seed.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            b'a' + ((seed >> 16) % 16) as u8
        })
        .collect();
    let want = text.clone();
    let s = h2_server(&[], move |peer, _| {
        while let Some((stream, _, _)) = peer.request() {
            peer.head(
                stream,
                "200",
                &[("content-encoding", "gzip"), ("x-a", "1")],
                false,
            );
            peer.data(stream, &gzip(&text), true);
        }
    });
    let mut core = h2_core(&s.pem);
    let mut r = Reader::new(1);
    let out = req(
        1,
        s.addr,
        STREAM | DECODE,
        5000,
        "GET / HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    // a reader that waits: Rust holds no more than the window
    r.until(&mut core, &out, false, |r| r.body.len() >= HIGH);
    for _ in 0..50 {
        r.run(&mut core, &[]);
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(r.body.len(), HIGH);
    assert!(s.sent.load(Ordering::SeqCst) <= 2 * WINDOW as usize);
    r.until(&mut core, &[], true, |r| r.over());
    assert!(r.ended, "{:?}", r.error);
    assert_eq!(r.lines, "x-a:1\n");
    assert!(r.body == want);
    assert_eq!(core.open, 0);
}

#[test]
fn h2_an_upload_is_byte_exact() {
    const SIZE: usize = 3 << 20;
    let got = Arc::new(Mutex::new(Vec::new()));
    let seen = got.clone();
    let s = h2_server(&[], move |peer, _| {
        while let Some((stream, fields, body)) = peer.request() {
            // no HTTP/1.1 framing in HTTP/2
            assert_eq!(field(&fields, "transfer-encoding"), "");
            *seen.lock().unwrap() = body;
            peer.answer(stream, "200", &[], b"ok");
        }
    });
    let mut core = h2_core(&s.pem);
    let data = pattern(SIZE);
    for (id, chunked) in [(1u64, true), (2, false)] {
        let head = if chunked {
            "PUT /up HTTP/1.1\r\nhost: localhost\r\ntransfer-encoding: chunked\r\n\r\n".to_string()
        } else {
            format!("PUT /up HTTP/1.1\r\nhost: localhost\r\ncontent-length: {SIZE}\r\n\r\n")
        };
        let flags = STREAM | UPLOAD | if chunked { CHUNKED } else { 0 };
        let mut out = req(id, s.addr, flags, 10_000, &head);
        let mut r = Reader::new(id);
        let (mut at, mut ended, mut most) = (0, false, 0);
        let start = Instant::now();
        while !r.over() {
            assert!(start.elapsed() < Duration::from_secs(20), "stuck");
            // the first report says that the connection works
            if r.wrote > 0 {
                while at < SIZE && head.len() + at - r.wrote as usize <= HIGH {
                    let n = (SIZE - at).min(PART_MAX);
                    out.extend(q_part(id, &data[at..at + n]));
                    at += n;
                }
                if at == SIZE && !ended {
                    ended = true;
                    out.extend(q_end(id, false));
                }
            }
            let n = r.run(&mut core, &out);
            out.clear();
            most = most.max((head.len() + at).saturating_sub(r.wrote as usize));
            if n == 0 && core.again.is_empty() {
                thread::sleep(Duration::from_millis(1));
            }
        }
        assert_eq!((r.status, r.body.as_slice()), (Some(200), &b"ok"[..]));
        assert!(r.ended);
        assert_eq!(r.wrote as usize, head.len() + SIZE);
        assert!(got.lock().unwrap().as_slice() == data);
        assert!(most <= HIGH + PART_MAX + 100, "{most}");
    }
    assert_eq!(core.open, 0);
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
}

#[test]
fn h2_an_early_answer_stops_the_upload_and_keeps_the_connection() {
    let rst = Arc::new(AtomicUsize::new(0));
    let seen = rst.clone();
    let s = h2_server(&[], move |peer, _| {
        peer.hold = true;
        loop {
            match peer.next() {
                Some(In::Head { stream, end, .. }) => {
                    let status = if end { "200" } else { "413" };
                    peer.answer(stream, status, &[], b"no");
                }
                Some(In::Rst { code, .. }) => {
                    seen.fetch_add(code as usize, Ordering::SeqCst);
                }
                Some(_) => {}
                None => return,
            }
        }
    });
    let mut core = h2_core(&s.pem);
    let head = "PUT /up HTTP/1.1\r\nhost: localhost\r\ncontent-length: 999999\r\n\r\n";
    let mut r = Reader::new(1);
    r.until(
        &mut core,
        &req(1, s.addr, STREAM | UPLOAD, 5000, head),
        true,
        |r| r.over(),
    );
    assert_eq!((r.status, r.body.as_slice()), (Some(413), &b"no"[..]));
    // Dart then stops its body stream: nothing more goes out
    r.run(&mut core, &q_end(1, true));
    r.run(&mut core, &q_part(1, b"late"));
    assert_eq!(core.open, 0);
    let a = run_until(&mut core, &get_path(2, s.addr, "/"), 1);
    assert_eq!(a[0].status, 200);
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
    // RST_STREAM with CANCEL for the request that did not end
    assert_eq!(rst.load(Ordering::SeqCst), 8);
}

#[test]
fn h2_a_timeout_and_a_cancel_stop_one_stream_only() {
    let rst = Arc::new(Mutex::new(Vec::new()));
    let seen = rst.clone();
    let s = h2_server(&[], move |peer, _| {
        loop {
            match peer.next() {
                Some(In::Head { stream, fields, .. }) => match field(&fields, ":path") {
                    "/never" => {}
                    "/half" => {
                        peer.head(stream, "200", &[], false);
                        peer.data(stream, b"half", false);
                    }
                    _ => peer.answer(stream, "200", &[], b"fast"),
                },
                Some(In::Rst { stream, code }) => seen.lock().unwrap().push((stream, code)),
                Some(_) => {}
                None => return,
            }
        }
    });
    let mut core = h2_core(&s.pem);
    assert_eq!(
        run_until(&mut core, &get_path(1, s.addr, "/"), 1)[0].status,
        200
    );
    // a request with no answer, and one with an answer, at the same time
    let mut out = req(
        2,
        s.addr,
        AGAIN,
        150,
        "GET /never HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    out.extend(get_path(3, s.addr, "/"));
    let start = Instant::now();
    let mut answers = run_until(&mut core, &out, 2);
    answers.sort_by_key(|a| a.id);
    assert_eq!((answers[0].status, answers[0].error), (0, E_TIMEOUT));
    assert!(start.elapsed() >= Duration::from_millis(150));
    assert_eq!(answers[1].body, b"fast");
    // a streamed answer that the reader cancels
    let mut r = Reader::new(4);
    let out = req(
        4,
        s.addr,
        STREAM,
        5000,
        "GET /half HTTP/1.1\r\nhost: localhost\r\n\r\n",
    );
    r.until(&mut core, &out, true, |r| r.body.len() == 4);
    r.run(&mut core, &q_cancel(4));
    assert_eq!(core.open, 0);
    assert_eq!(shared(&core), [0]);
    // a credit for a request that is gone is dropped
    r.run(&mut core, &q_credit(4, 4));
    assert_eq!(
        run_until(&mut core, &get_path(5, s.addr, "/"), 1)[0].body,
        b"fast"
    );
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
    assert_eq!(*rst.lock().unwrap(), [(3, 8), (7, 8)]);
}

#[test]
fn h2_goaway_and_refused_streams_are_sent_again() {
    let first = Arc::new(AtomicBool::new(true));
    let s = h2_server(&[], move |peer, n| {
        if n == 0 {
            // GOAWAY: no work was done on a stream
            let Some(In::Head { .. }) = peer.next() else {
                return;
            };
            peer.put(7, 0, 0, &[0; 8]);
            return peer.idle();
        }
        loop {
            match peer.next() {
                Some(In::Head { stream, fields, .. }) => {
                    let path = field(&fields, ":path").to_string();
                    if path == "/refused" && first.swap(false, Ordering::SeqCst) {
                        peer.put(3, 0, stream, &7u32.to_be_bytes());
                    } else if path == "/broken" {
                        peer.put(3, 0, stream, &2u32.to_be_bytes());
                    } else {
                        peer.answer(stream, "200", &[], path.as_bytes());
                    }
                }
                Some(_) => {}
                None => return,
            }
        }
    });
    let mut core = h2_core(&s.pem);
    let a = run_until(&mut core, &get_path(1, s.addr, "/a"), 1);
    assert_eq!((a[0].status, a[0].body.as_slice()), (200, &b"/a"[..]));
    assert_eq!(s.count.load(Ordering::SeqCst), 2);
    // the connection that said GOAWAY is gone
    assert_eq!(shared(&core), [0]);
    let a = run_until(&mut core, &get_path(2, s.addr, "/refused"), 1);
    assert_eq!(a[0].body, b"/refused");
    let a = run_until(&mut core, &get_path(3, s.addr, "/broken"), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_CLOSED));
    assert_eq!(s.count.load(Ordering::SeqCst), 2);
    assert_eq!(core.open, 0);
}

#[test]
fn h2_a_request_that_may_not_go_again_fails_on_goaway() {
    let s = h2_server(&[], |peer, _| {
        let Some(In::Head { .. }) = peer.next() else {
            return;
        };
        peer.put(7, 0, 0, &[0; 8]);
        peer.idle();
    });
    let mut core = h2_core(&s.pem);
    let out = req(
        1,
        s.addr,
        0,
        5000,
        "POST / HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n",
    );
    let a = run_until(&mut core, &out, 1);
    assert_eq!((a[0].status, a[0].error), (0, E_CLOSED));
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
    assert!(core.conns.is_empty());
    assert!(core.hosts.is_empty());
}

#[test]
fn h2_a_connection_that_fails_fails_its_requests() {
    let s = h2_server(&[], |peer, n| {
        let Some((stream, _, _)) = peer.request() else {
            return;
        };
        peer.answer(stream, "200", &[], b"first");
        let mut open = Vec::new();
        while open.len() < 2 {
            match peer.next() {
                Some(In::Head { stream, .. }) => open.push(stream),
                Some(_) => {}
                None => return,
            }
        }
        if n == 0 {
            // one whole answer, a head, then the connection closes
            peer.answer(open[0], "200", &[], b"whole");
            peer.head(open[1], "200", &[], false);
        } else {
            // PUSH_PROMISE: a connection error
            peer.put(5, 4, open[0], &[0, 0, 0, 2]);
            peer.idle();
        }
    });
    let mut core = h2_core(&s.pem);
    let two = |from: u64| {
        let post = "POST / HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n";
        let mut out = req(from, s.addr, 0, 5000, post);
        out.extend(req(from + 1, s.addr, 0, 5000, post));
        out
    };
    // both on one connection: the whole answer is an answer
    run_until(&mut core, &get_path(9, s.addr, "/"), 1);
    let mut answers = run_until(&mut core, &two(1), 2);
    answers.sort_by_key(|a| a.id);
    assert_eq!(
        (answers[0].status, answers[0].body.as_slice()),
        (200, &b"whole"[..])
    );
    assert_eq!((answers[1].status, answers[1].error), (0, E_CLOSED));
    assert!(core.conns.is_empty());
    // a connection error fails all its requests
    let mut core = h2_core(&s.pem);
    run_until(&mut core, &get_path(9, s.addr, "/"), 1);
    let answers = run_until(&mut core, &two(1), 2);
    for a in &answers {
        assert_eq!((a.status, a.error), (0, E_BAD));
    }
    assert_eq!(core.open, 0);
    assert!(core.conns.is_empty());
}

#[test]
fn h2_the_stream_limit_of_the_server_makes_requests_wait() {
    let s = h2_server(&[(3, 1)], echo);
    let config = tls_config(s.pem.as_bytes(), true).unwrap();
    let mut core = Core::new(1, 0, None, config).unwrap();
    run_until(&mut core, &get_path(1, s.addr, "/"), 1);
    let mut out = Vec::new();
    for id in 2..=4 {
        out.extend(get_path(id, s.addr, &format!("/n{id}")));
    }
    let mut inb = vec![0u8; 1 << 16];
    let n = core.run(&out, &mut inb).unwrap();
    assert_eq!(n, 0);
    // one stream is open, the others wait for it
    assert_eq!(shared(&core), [1]);
    assert_eq!(core.hosts.values().next().unwrap().queue.len(), 2);
    let answers = run_until(&mut core, &[], 3);
    let ids: Vec<u64> = answers.iter().map(|a| a.id).collect();
    assert_eq!(ids, [2, 3, 4]);
    assert_eq!(s.count.load(Ordering::SeqCst), 1);
    // with no limit of connections, a second connection takes them
    let mut core = h2_core(&s.pem);
    run_until(&mut core, &get_path(1, s.addr, "/"), 1);
    assert_eq!(run_until(&mut core, &out, 3).len(), 3);
    assert!(s.count.load(Ordering::SeqCst) > 2);
}

#[test]
fn h2_a_server_without_it_gets_http_1() {
    let (addr, pem, count) = tls_server(&["localhost"], HELLO.to_vec());
    let mut core = h2_core(&pem);
    for id in 1..=3 {
        let a = run_until(&mut core, &get_path(id, addr, "/"), 1);
        assert_eq!((a[0].status, a[0].body.as_slice()), (200, &b"hello"[..]));
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(shared(&core).is_empty());
    // a client with HTTP/2 off does not ask for it
    let s = h2_server(&[], echo);
    let config = tls_config(s.pem.as_bytes(), false).unwrap();
    let mut core = Core::new(0, 0, None, config).unwrap();
    let a = run_until(&mut core, &get_path(1, s.addr, "/"), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_TLS));
}

#[test]
fn h2_an_idle_connection_closes_after_the_idle_timeout() {
    let s = h2_server(&[], echo);
    let config = tls_config(s.pem.as_bytes(), true).unwrap();
    let idle = Some(Duration::from_millis(100));
    let mut core = Core::new(0, 0, idle, config).unwrap();
    let mut inb = vec![0u8; 4096];
    assert_eq!(
        run_until(&mut core, &get_path(1, s.addr, "/"), 1)[0].status,
        200
    );
    core.run(&[], &mut inb).unwrap();
    assert_eq!(shared(&core), [0]);
    thread::sleep(Duration::from_millis(150));
    core.run(&[], &mut inb).unwrap();
    assert!(core.conns.is_empty());
    assert!(core.hosts.is_empty());
    assert_eq!(
        run_until(&mut core, &get_path(2, s.addr, "/"), 1)[0].status,
        200
    );
    assert_eq!(s.count.load(Ordering::SeqCst), 2);
}
