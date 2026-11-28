//! Tests of the server loop. They drive a `Core` the way Dart does: call
//! `run`, read the records, write answers. A small test app answers.
use super::*;
use std::net::{Ipv4Addr, TcpStream as StdStream};
use tungstenite::WebSocket;

/// The default idle timeout of Dart's `bind`.
const IDLE: Duration = Duration::from_secs(30);

fn limits() -> Limits {
    Limits {
        body: 10,
        multipart: 20,
        timeout: Duration::from_millis(50),
        conns: 0,
        idle: IDLE,
        http2: true,
        max_streams: 250,
        reset_burst: 0,
        reset_rate: crate::h2::RESET_RATE,
    }
}

fn new_core(lim: Limits) -> (Core, u16) {
    let lo = u64::from(u32::from(Ipv4Addr::LOCALHOST));
    let (listener, key) = bind((4, 0, lo, 0)).unwrap();
    (Core::new(listener, key, lim).unwrap(), key.3)
}

/// A record from Rust.
#[derive(Debug, Clone)]
struct Rec {
    id: u64,
    flags: u32,
    target: String,
    body: Vec<u8>,
    code: u16,
}

impl Rec {
    fn kind(&self) -> u32 {
        (self.flags >> 8) & 0xff
    }
}

fn records(b: &[u8]) -> Vec<Rec> {
    let mut out = Vec::new();
    let mut p = 0;
    while p + REQ_HEAD <= b.len() {
        let id = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
        let flags = rd32(b, p + 8) as u32;
        let (ml, tl) = (rd16(b, p + 12) as usize, rd16(b, p + 14) as usize);
        let (hl, bl) = (rd32(b, p + 16), rd32(b, p + 20));
        let q = p + REQ_HEAD;
        let target = String::from_utf8_lossy(&b[q + ml..q + ml + tl]).into_owned();
        let at = q + ml + tl + hl;
        let body = if flags & 4 != 0 {
            let word = |o: usize| u64::from_le_bytes(b[at + o..at + o + 8].try_into().unwrap());
            let v = unsafe { std::slice::from_raw_parts(word(0) as *const u8, word(16) as usize) }
                .to_vec();
            unsafe { dox_free_body(word(8) as *mut c_void) };
            v
        } else {
            b[at..at + bl].to_vec()
        };
        out.push(Rec {
            id,
            flags,
            target,
            body,
            code: rd16(b, p + 24),
        });
        p = at + bl;
    }
    out
}

/// Adds a response record to `out`.
fn ans(out: &mut Vec<u8>, id: u64, status: u16, kind: u16, lines: &[u8], body: &[u8]) {
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&kind.to_le_bytes());
    out.extend_from_slice(&(lines.len() as u32).to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(lines);
    out.extend_from_slice(body);
}

/// The test app. `/ws`, `/bye`, `/flood`, `/big` and `/wide` accept a
/// WebSocket (ws id = request id, max message 8 bytes, `/big` and `/wide`
/// 16 MiB). `/bye` then
/// closes with 4000, `/flood` sends 2 MB at once, `/big` 8 MiB. Messages
/// are echoed. `/stream` sends "ab", "cde" as a streamed body. `/exact`,
/// `/liar` and `/short` do the same with `content-length` 5, 3 and 9,
/// `/trailers` with trailers on the end.
/// `/zip` gzips, `/large` sends 8 MiB. Anything else: the target and the
/// body.
fn app(r: &Rec, out: &mut Vec<u8>) {
    match r.kind() {
        0 => match r.target.as_str() {
            t @ ("/ws" | "/bye" | "/flood" | "/big" | "/wide") if r.flags & 2 != 0 => {
                let max: u64 = if t == "/big" || t == "/wide" {
                    16 << 20
                } else {
                    8
                };
                ans(
                    out,
                    r.id,
                    101,
                    1,
                    b"",
                    &[r.id.to_le_bytes(), max.to_le_bytes()].concat(),
                );
                match t {
                    "/bye" => ans(out, r.id, 4000, 4, b"", b"done"),
                    "/flood" => {
                        for _ in 0..20 {
                            ans(out, r.id, 0, 3, b"", &[0u8; 100_000]);
                        }
                    }
                    "/big" => ans(out, r.id, 0, 3, b"", &vec![0u8; 8 << 20]),
                    _ => {}
                }
            }
            t @ ("/stream" | "/exact" | "/liar" | "/short" | "/trailers") => {
                let lines: &[u8] = match t {
                    "/exact" => b"content-length: 5\r\n",
                    "/liar" => b"content-length: 3\r\n",
                    "/short" => b"content-length: 9\r\n",
                    _ => b"",
                };
                ans(
                    out,
                    r.id,
                    200,
                    6,
                    lines,
                    &[&r.id.to_le_bytes()[..], b"ab"].concat(),
                );
                ans(out, r.id, 0, 7, b"", b"cde");
                let trailers: &[u8] = if t == "/trailers" {
                    b"grpc-status: 0\r\n"
                } else {
                    b""
                };
                ans(out, r.id, 0, 8, trailers, b"");
            }
            "/large" => ans(out, r.id, 200, 0, b"", &vec![b'x'; 8 << 20]),
            "/zip" => ans(
                out,
                r.id,
                200,
                0x100,
                b"content-encoding: gzip\r\n",
                &b"hello ".repeat(100),
            ),
            // the gzip level in kind bits 12 to 15
            t if t.starts_with("/zip/") => ans(
                out,
                r.id,
                200,
                0x100 | (t[5..].parse::<u16>().unwrap() << 12),
                b"content-encoding: gzip\r\n",
                &ZIP_TEXT,
            ),
            t => ans(out, r.id, 200, 0, b"", &[t.as_bytes(), &r.body].concat()),
        },
        // echo text and binary
        k @ (1 | 2) => ans(out, r.id, 0, k as u16 + 1, b"", &r.body),
        _ => {}
    }
}

/// One turn, like Dart: run, then let the app answer. Returns the records.
fn turn(core: &mut Core, inb: &mut [u8], out: &mut Vec<u8>, budget: i64) -> Vec<Rec> {
    let n = core.run(out, inb, budget).expect("room");
    out.clear();
    let recs = records(&inb[..n]);
    for r in &recs {
        app(r, out);
    }
    recs
}

/// Runs the test app on a thread. Returns the port and every record that
/// is not a request.
fn serve(lim: Limits) -> (u16, Arc<Mutex<Vec<Rec>>>) {
    let (core, port) = new_core(lim);
    (port, run_app(core))
}

/// Runs the test app on `core` on a thread. Returns every record that is
/// not a request.
fn run_app(mut core: Core) -> Arc<Mutex<Vec<Rec>>> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let seen = log.clone();
    std::thread::spawn(move || {
        let mut inb = vec![0u8; 1 << 18];
        let mut out = Vec::new();
        loop {
            for r in turn(&mut core, &mut inb, &mut out, 1000) {
                if r.kind() != 0 {
                    seen.lock().unwrap().push(r);
                }
            }
        }
    });
    log
}

fn connect(port: u16) -> StdStream {
    let s = StdStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    s.set_nodelay(true).unwrap();
    s
}

/// Runs the loop until `count` connections are open, or 5 seconds.
/// connect() can return before the server side can accept, and one call
/// that gets WouldBlock does not try again: a test that calls once (or a
/// few times) after connect() fails on a busy machine.
fn accept(core: &mut Core, inb: &mut [u8], count: usize) {
    let end = Instant::now() + Duration::from_secs(5);
    while core.conns.len() < count && Instant::now() < end {
        core.run(&[], inb, 1000).unwrap();
    }
    assert_eq!(core.conns.len(), count, "accepted");
}

/// Sends `req`, reads until the server closes (or 3 seconds).
fn send(port: u16, req: &[u8]) -> String {
    let mut s = connect(port);
    s.write_all(req).unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    String::from_utf8_lossy(&out).into_owned()
}

fn wait_for(log: &Mutex<Vec<Rec>>, what: impl Fn(&Rec) -> bool) -> Rec {
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end {
        if let Some(r) = log.lock().unwrap().iter().find(|r| what(r)) {
            return r.clone();
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("no such record");
}

#[test]
fn answers_and_bad_requests() {
    let (port, _) = serve(limits());
    let ok = send(
        port,
        b"GET /hi HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
    );
    assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"), "{ok:?}");
    assert!(
        ok.contains("content-length: 3\r\n") && ok.ends_with("\r\n\r\n/hi"),
        "{ok:?}"
    );
    assert!(
        send(port, b"HELLO\r\n\r\n").starts_with("HTTP/1.1 400"),
        "400"
    );
    let big = format!(
        "GET / HTTP/1.1\r\nHost: a\r\nX: {}\r\n\r\n",
        "a".repeat(20000)
    );
    assert!(
        send(port, big.as_bytes()).starts_with("HTTP/1.1 431"),
        "431"
    );
    let body =
        b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 11\r\nConnection: close\r\n\r\nhello world";
    assert!(send(port, body).starts_with("HTTP/1.1 413"), "413");
    let two = b"POST / HTTP/1.1\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n";
    assert!(send(port, two).starts_with("HTTP/1.1 400"), "smuggling");
    let http10 = send(port, b"GET /x HTTP/1.0\r\n\r\n");
    assert!(
        http10.starts_with("HTTP/1.0 200") && http10.contains("connection: close"),
        "{http10:?}"
    );
}

/// Sends a 20 byte body (over the limit) in 2 byte parts, 10 ms apart.
fn slow_send(port: u16) -> String {
    let mut s = connect(port);
    s.write_all(b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 20\r\n\r\n")
        .unwrap();
    for _ in 0..10 {
        std::thread::sleep(Duration::from_millis(10));
        s.write_all(b"ab").unwrap();
    }
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    String::from_utf8_lossy(&out).into_owned()
}

#[test]
fn body_limits_and_timeout() {
    let (port, _) = serve(limits());
    // too big, and the client waits for "100 Continue": 413 at once
    let head =
        b"POST / HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 1000000000\r\n\r\n";
    assert!(send(port, head).starts_with("HTTP/1.1 413"), "early 413");
    // too big, whole body sent: read and dropped, then 413 (no reset)
    let full = b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 15\r\n\r\n123456789012345";
    assert!(send(port, full).starts_with("HTTP/1.1 413"), "413");
    // slow but steady: 100 ms in all, but never 50 ms without bytes
    let r = slow_send(port);
    assert!(r.starts_with("HTTP/1.1 413"), "steady {r:?}");
    // a stalled body gets 408 after the idle timeout
    let slow = b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nab";
    assert!(send(port, slow).starts_with("HTTP/1.1 408"), "408");
    // a multipart form may be bigger (15 > 10): no 413, it waits for the body
    let form = b"POST / HTTP/1.1\r\nHost: a\r\nContent-Type: multipart/form-data; boundary=x\r\nContent-Length: 15\r\n\r\n";
    assert!(
        send(port, form).starts_with("HTTP/1.1 408"),
        "multipart limit"
    );
    // a chunked body over the limit: 413
    let chunked =
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nc\r\n123456789012\r\n0\r\n\r\n";
    assert!(
        send(port, chunked).starts_with("HTTP/1.1 413"),
        "chunked 413"
    );
}

#[test]
fn chunked_size_line_needs_a_hex_digit() {
    let (port, _) = serve(limits());
    // an empty size line (or one with only an extension) is not the last chunk
    for body in [
        "\r\nabc\r\n0\r\n\r\n",
        ";x\r\nabc\r\n0\r\n\r\n",
        " 3\r\nabc\r\n0\r\n\r\n",
        // a control byte in an extension (a lone LF ends the line for some proxies)
        "3;a\nb\r\nabc\r\n0\r\n\r\n",
        "3;a\x00\r\nabc\r\n0\r\n\r\n",
        "3;a\x7f\r\nabc\r\n0\r\n\r\n",
        // a lone LF or CR in a trailer: a proxy can end the body there
        "3\r\nabc\r\n0\r\nX: a\n\r\nGET /evil HTTP/1.1\r\n\r\n",
        "3\r\nabc\r\n0\r\nX: a\rb\r\n\r\n",
    ] {
        let req = format!(
            "POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{body}"
        );
        let r = send(port, req.as_bytes());
        assert!(r.starts_with("HTTP/1.1 400"), "{body:?} gave {r:?}");
    }
}

#[test]
fn chunked_body_and_expect_continue() {
    let lim = Limits {
        body: 1 << 20,
        ..limits()
    };
    let (port, _) = serve(lim);
    let req = b"POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n2;x=y\tz\r\nde\r\n0\r\nTrailer: 1\r\n\r\n";
    let r = send(port, req);
    assert!(
        r.starts_with("HTTP/1.1 200") && r.ends_with("/cabcde"),
        "{r:?}"
    );
    // a big body in many reads, then a pipelined request after it
    let body = vec![b'x'; 200_000];
    let mut s = connect(port);
    s.write_all(b"POST /big HTTP/1.1\r\nContent-Length: 200000\r\n\r\n")
        .unwrap();
    for part in body.chunks(30_000) {
        s.write_all(part).unwrap();
    }
    s.write_all(b"GET /after HTTP/1.1\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    let out = String::from_utf8_lossy(&out);
    assert!(
        out.contains(&format!("content-length: {}\r\n", 200_004)),
        "big"
    );
    assert!(out.ends_with("/after"), "pipelined after the body");
    // Expect: 100-continue: the go first, then the answer
    let mut s = connect(port);
    s.write_all(b"POST /e HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 2\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut buf = [0u8; 25];
    s.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"HTTP/1.1 100 Continue\r\n\r\n");
    s.write_all(b"ok").unwrap();
    let mut rest = String::new();
    let _ = s.read_to_string(&mut rest);
    assert!(
        rest.starts_with("HTTP/1.1 200") && rest.ends_with("/eok"),
        "{rest:?}"
    );
}

/// Feeds a chunked body in pieces of `piece` bytes. Gives the result and
/// the body bytes.
fn feed_chunked(body: &[u8], piece: usize) -> (Feed, Vec<u8>) {
    let head = b"POST / HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n";
    let lim = Limits {
        body: 1 << 20,
        ..limits()
    };
    let Step::Body(_, mut b, _) = parse_one(head, 1, &[0; 18], &mut [], 0, &lim, Instant::now())
    else {
        panic!("no body");
    };
    let (mut pos, mut end) = (0, 0);
    loop {
        end = (end + piece).min(body.len());
        let r = b.feed(&body[..end], &mut pos);
        if r != Feed::More || end == body.len() {
            return (r, std::mem::take(&mut b.data));
        }
    }
}

#[test]
fn long_chunk_lines_fail_the_same_whole_or_in_pieces() {
    // found by the chunked_body fuzz target: a chunk size line over 1024
    // bytes failed in small reads, but passed when it came in one read
    let line = |n: usize| {
        let mut l = b"5;".to_vec();
        l.resize(n - 2, b'x');
        l.extend_from_slice(b"\r\nhello\r\n0\r\n\r\n");
        l
    };
    for piece in [7, 100_000] {
        assert_eq!(
            feed_chunked(&line(CHUNK_LINE), piece),
            (Feed::Whole, b"hello".to_vec())
        );
        assert_eq!(
            feed_chunked(&line(CHUNK_LINE + 1), piece).0,
            Feed::Fail(400)
        );
    }
    // the same for a trailer over MAX_HEAD bytes
    let trailer = |n: usize| {
        let mut t = b"0\r\nx: ".to_vec();
        t.resize(3 + n - 4, b'y');
        t.extend_from_slice(b"\r\n\r\n");
        t
    };
    for piece in [7, 100_000] {
        assert_eq!(feed_chunked(&trailer(MAX_HEAD), piece).0, Feed::Whole);
        assert_eq!(
            feed_chunked(&trailer(MAX_HEAD + 1), piece).0,
            Feed::Fail(431)
        );
    }
}

/// Runs turns (with no app) until `count` requests came, or 2 seconds.
fn collect(core: &mut Core, inb: &mut [u8], count: usize) -> Vec<Rec> {
    let mut got = Vec::new();
    let end = Instant::now() + Duration::from_secs(2);
    while got.len() < count && Instant::now() < end {
        let n = core.run(&[], inb, 1000).unwrap();
        got.extend(records(&inb[..n]));
    }
    got
}

#[test]
fn answers_go_in_order_and_inflight_is_limited() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    let all: String = (0..100)
        .map(|i| format!("GET /{i} HTTP/1.1\r\n\r\n"))
        .collect();
    s.write_all(all.as_bytes()).unwrap();
    let first = collect(&mut core, &mut inb, 100);
    assert_eq!(first.len(), MAX_INFLIGHT, "stops at the limit");
    // answer in reverse order: the client still gets them in order
    let mut out = Vec::new();
    for r in first.iter().rev() {
        ans(&mut out, r.id, 200, 0, b"", r.target.as_bytes());
    }
    let n = core.run(&out, &mut inb, 0).unwrap();
    let mut rest = records(&inb[..n]);
    rest.extend(collect(
        &mut core,
        &mut inb,
        100 - MAX_INFLIGHT - rest.len(),
    ));
    assert_eq!(rest.len(), 100 - MAX_INFLIGHT, "reads on after the answers");
    let mut got = vec![0u8; 0];
    let mut buf = [0u8; 65536];
    while !String::from_utf8_lossy(&got).ends_with(&format!("/{}", MAX_INFLIGHT - 1)) {
        let k = s.read(&mut buf).unwrap();
        got.extend_from_slice(&buf[..k]);
    }
    let text = String::from_utf8_lossy(&got);
    let bodies: Vec<&str> = text
        .split("\r\n\r\n")
        .skip(1)
        .map(|b| &b[..b.find("HTTP").unwrap_or(b.len())])
        .collect();
    let want: Vec<String> = (0..MAX_INFLIGHT).map(|i| format!("/{i}")).collect();
    assert_eq!(bodies, want);
}

fn ws_client(port: u16, path: &str) -> WebSocket<StdStream> {
    let (ws, res) =
        tungstenite::client(format!("ws://127.0.0.1:{port}{path}"), connect(port)).unwrap();
    assert_eq!(res.status(), 101);
    ws
}

#[test]
fn websocket_messages_both_ways() {
    let (port, log) = serve(limits());
    let mut ws = ws_client(port, "/ws");
    ws.send(Message::text("hi")).unwrap();
    assert_eq!(ws.read().unwrap(), Message::text("hi"));
    ws.send(Message::binary(vec![1, 2])).unwrap();
    assert_eq!(ws.read().unwrap(), Message::binary(vec![1, 2]));
    ws.send(Message::Ping(Bytes::from_static(b"p"))).unwrap();
    assert_eq!(ws.read().unwrap(), Message::Pong(Bytes::from_static(b"p")));
    ws.close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: Utf8Bytes::from_static("bye"),
    }))
    .unwrap();
    while ws.read().is_ok() {}
    let closed = wait_for(&log, |r| r.kind() == WS_CLOSED);
    assert_eq!((closed.code, closed.body.as_slice()), (1000, &b"bye"[..]));
    let texts: Vec<_> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.id == closed.id)
        .map(|r| r.kind())
        .collect();
    assert_eq!(
        texts,
        vec![WS_TEXT, WS_BINARY, WS_CLOSED],
        "one closed record"
    );
}

#[test]
fn websocket_too_big_closes_with_1009() {
    let (port, log) = serve(limits());
    let mut ws = ws_client(port, "/ws");
    ws.send(Message::text("123456789")).unwrap();
    let Ok(Message::Close(Some(frame))) = ws.read() else {
        panic!("no close frame")
    };
    assert_eq!(frame.code, CloseCode::Size);
    assert_eq!(wait_for(&log, |r| r.kind() == WS_CLOSED).code, 1009);
}

#[test]
fn websocket_close_from_dart() {
    let (port, log) = serve(limits());
    let mut ws = ws_client(port, "/bye");
    let Ok(Message::Close(Some(frame))) = ws.read() else {
        panic!("no close frame")
    };
    assert_eq!(
        (u16::from(frame.code), frame.reason.as_str()),
        (4000, "done")
    );
    while ws.read().is_ok() {} // answers the close
    // the client answered the close: Rust reports its code
    assert_eq!(wait_for(&log, |r| r.kind() == WS_CLOSED).code, 4000);
}

/// Dart queues more than the cap (1 MiB here) at once: the socket closes
/// with 1008, without sending the queued messages.
#[test]
fn websocket_queue_over_the_cap_closes_with_1008() {
    let (port, log) = serve(limits());
    let mut ws = ws_client(port, "/flood");
    let Ok(Message::Close(Some(frame))) = ws.read() else {
        panic!("no close frame")
    };
    assert_eq!(frame.code, CloseCode::Policy);
    assert_eq!(wait_for(&log, |r| r.kind() == WS_CLOSED).code, 1008);
}

/// Upgrades a client (on its own thread) on a core that the test drives.
/// Returns the ws id and the client thread's channel to send messages.
fn ws_on(
    core: &mut Core,
    inb: &mut [u8],
    port: u16,
    path: &'static str,
) -> (u64, mpsc::Sender<Message>) {
    let (tx, rx) = mpsc::channel::<Message>();
    std::thread::spawn(move || {
        let mut ws = ws_client(port, path);
        for m in rx {
            let _ = ws.send(m);
        }
        std::thread::sleep(Duration::from_secs(10)); // never reads
    });
    (upgrade(core, inb), tx)
}

/// Answers the next upgrade request and runs until the WebSocket is live.
/// Returns the ws id (= the request id).
fn upgrade(core: &mut Core, inb: &mut [u8]) -> u64 {
    let req = collect(core, inb, 1).pop().expect("upgrade request");
    let mut out = Vec::new();
    app(&req, &mut out);
    core.run(&out, inb, 0).unwrap();
    for _ in 0..50 {
        core.run(&[], inb, 1000).unwrap();
    }
    req.id
}

/// A client that sends `bytes` of ping frames (`size` bytes of data each,
/// 6 bytes of frame head) after the
/// upgrade. With `read` it reads and drops the pongs, else it never reads.
/// Returns the index of its connection.
fn ping_flood(
    core: &mut Core,
    inb: &mut [u8],
    port: u16,
    read: bool,
    size: u8,
    bytes: usize,
) -> usize {
    std::thread::spawn(move || {
        let ws = ws_client(port, "/wide");
        let mut s = ws.get_ref().try_clone().unwrap();
        // big buffers: many frames wait at once, more than one turn reads
        let _ = socket2::SockRef::from(&s).set_send_buffer_size(4 << 20);
        if read {
            let mut r = s.try_clone().unwrap();
            std::thread::spawn(move || {
                let mut buf = [0u8; 65536];
                while r.read(&mut buf).is_ok_and(|k| k > 0) {}
            });
        }
        let mut frame = vec![0x89, 0x80 | size, 0, 0, 0, 0];
        frame.extend_from_slice(&vec![b'p'; size.into()]);
        let _ = s.write_all(&frame.repeat(bytes / frame.len()));
        std::thread::sleep(Duration::from_secs(10));
    });
    let idx = (upgrade(core, inb) >> 32) as usize;
    let _ = socket2::SockRef::from(&core.conns[idx].sock).set_recv_buffer_size(4 << 20);
    idx
}

/// A client floods pings: one call reads only a share of them, and the
/// request of another client gets its turn in the same call.
#[test]
fn websocket_ping_flood_does_not_hold_the_loop() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let idx = ping_flood(&mut core, &mut inb, port, true, 20, 64 << 20);
    let mut s = connect(port);
    // On a busy machine the accept, the request and the flood bytes can
    // all come late. So do not trust a sleep: call until a call gets the
    // request while the flood is "full": more than two shares of it wait
    // and no pong waits (a pong backlog pauses the flood, see `ws_read`).
    let mut peek = vec![0u8; 2 * WS_READ + 1];
    let mut asked = false;
    // only a limit to give up: the result does not depend on time
    let end = Instant::now() + Duration::from_secs(10);
    let pongs = loop {
        assert!(
            Instant::now() < end,
            "no call got the other request while the flood waited"
        );
        let c = &core.conns[idx];
        let full = c.pending() == 0
            && !c.ws.as_ref().unwrap().paused
            && c.sock.peek(&mut peek).is_ok_and(|k| k > 2 * WS_READ);
        let before = c.written;
        if full && !asked {
            s.write_all(b"GET /other HTTP/1.1\r\n\r\n").unwrap();
            asked = true;
        }
        if !full {
            std::thread::sleep(Duration::from_millis(1)); // the client fills
        }
        let n = core.run(&[], &mut inb, 0).unwrap();
        if records(&inb[..n]).iter().any(|r| r.target == "/other") {
            if full {
                let c = &core.conns[idx];
                break c.written + c.pending() as u64 - before;
            }
            asked = false; // came in a call that proves nothing: ask again
        }
    };
    assert!(
        core.ready.contains(&idx),
        "the flood waits for its next turn"
    );
    // A count, not a time. Each ping is 26 bytes and gets one pong of 22
    // bytes, so the pongs count the bytes read in that call. One share is
    // WS_READ bytes; tungstenite may also hold less than one READ_ROOM of
    // frames from the last call, and owe one pong. The pongs of that
    // (under 235 KiB) stay under the pong cap WS_OUT_MAX, so the cap does
    // not stop the call. Without the share, the cap stops it only after
    // more than WS_OUT_MAX / 22 pings (over 300 KiB read).
    let pings = pongs / 22;
    assert!(
        pings * 26 <= (WS_READ + READ_ROOM + 26) as u64,
        "one call read {pings} pings, more than one share"
    );
}

/// A client sends pings and never reads the pongs: Rust stops reading its
/// frames, so the pongs do not fill the memory.
#[test]
fn websocket_pongs_for_a_client_that_does_not_read_are_capped() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let idx = ping_flood(&mut core, &mut inb, port, false, 125, 8 << 20);
    let end = Instant::now() + Duration::from_millis(500);
    while Instant::now() < end {
        core.run(&[], &mut inb, 1000).unwrap();
    }
    let c = &core.conns[idx];
    assert!(c.pending() <= WS_OUT_MAX + WS_READ, "{} waits", c.pending());
    assert!(c.ws.as_ref().unwrap().paused, "reading stopped");
}

/// While Dart has too many bytes not acknowledged, Rust reads no frame.
/// The ack starts reading again. Nothing is lost.
#[test]
fn websocket_pauses_until_dart_acknowledges() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let (id, tx) = ws_on(&mut core, &mut inb, port, "/ws");
    core.msgs.ws_unacked = WS_UNACKED + 1;
    tx.send(Message::text("hi")).unwrap();
    let paused = collect(&mut core, &mut inb, 1);
    assert!(paused.is_empty(), "read while paused");
    let mut ack = Vec::new();
    ans(&mut ack, (WS_UNACKED + 1) as u64, 0, 5, b"", b"");
    let n = core.run(&ack, &mut inb, 0).unwrap();
    let mut got = records(&inb[..n]);
    got.extend(collect(&mut core, &mut inb, 1 - got.len()));
    assert_eq!(
        (got[0].id, got[0].kind(), got[0].body.as_slice()),
        (id, WS_TEXT, &b"hi"[..])
    );
    assert_eq!(core.msgs.ws_unacked, 2);
}

/// Runs the timers as if `later` passed. Returns the records for Dart.
fn after(core: &mut Core, inb: &mut [u8], later: Duration) -> Vec<Rec> {
    core.now = Instant::now() + later;
    core.run_timers();
    let n = core.fill(inb).unwrap();
    records(&inb[..n])
}

/// The client never answers the close of Dart: after CLOSE_WAIT the
/// socket is dropped, with 1006.
#[test]
fn websocket_close_ends_when_the_client_does_not_answer() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let (id, _tx) = ws_on(&mut core, &mut inb, port, "/ws");
    let mut out = Vec::new();
    ans(&mut out, id, 0, 4, b"", b"");
    core.run(&out, &mut inb, 0).unwrap();
    assert!(
        after(&mut core, &mut inb, CLOSE_WAIT / 2).is_empty(),
        "too early"
    );
    let got = after(&mut core, &mut inb, CLOSE_WAIT + Duration::from_millis(10));
    assert_eq!(
        (got[0].id, got[0].kind(), got[0].code),
        (id, WS_CLOSED, 1006)
    );
}

/// A client that does not read: a send that makes no progress drops the
/// socket after SEND_WAIT, with 1006.
#[test]
fn websocket_send_ends_when_the_client_does_not_read() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let (id, _tx) = ws_on(&mut core, &mut inb, port, "/big");
    assert!(
        core.conns.iter().any(|(_, c)| c.pending() > 0),
        "a send waits"
    );
    assert!(
        after(&mut core, &mut inb, SEND_WAIT / 2).is_empty(),
        "too early"
    );
    let got = after(&mut core, &mut inb, SEND_WAIT + IDLE);
    assert_eq!(
        (got[0].id, got[0].kind(), got[0].code),
        (id, WS_CLOSED, 1006)
    );
}

/// The parts of a streamed body reach the client in order. Dart hears
/// how many bytes went out, part by part.
#[test]
fn streamed_body_sends_parts_and_counts_them() {
    let (port, log) = serve(limits());
    let r = send(port, b"GET /stream HTTP/1.1\r\nConnection: close\r\n\r\n");
    assert!(r.contains("transfer-encoding: chunked\r\n"), "{r:?}");
    assert!(
        r.ends_with("\r\n\r\n2\r\nab\r\n3\r\ncde\r\n0\r\n\r\n"),
        "{r:?}"
    );
    // the byte counts reach 5 (2 + 3)
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let sum: u64 = log
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.kind() == STREAM_SENT)
            .map(|r| u64::from_le_bytes(r.body[..8].try_into().unwrap()))
            .sum();
        if sum == 5 {
            break;
        }
        assert!(Instant::now() < end && sum < 5, "sent {sum}");
        std::thread::sleep(Duration::from_millis(5));
    }
    // HEAD: no body, Dart is told to stop
    let head = send(port, b"HEAD /stream HTTP/1.1\r\nConnection: close\r\n\r\n");
    assert!(
        head.ends_with("\r\n\r\n") && !head.contains("ab"),
        "{head:?}"
    );
    wait_for(&log, |r| r.kind() == STREAM_GONE);
}

/// HTTP/1.1 drops the trailers of a streamed body: the chunked end is
/// the same as with none.
#[test]
fn trailers_are_ignored_on_http1() {
    let (port, _) = serve(limits());
    let r = send(port, b"GET /trailers HTTP/1.1\r\nConnection: close\r\n\r\n");
    assert!(
        r.ends_with("\r\n\r\n2\r\nab\r\n3\r\ncde\r\n0\r\n\r\n"),
        "{r:?}"
    );
    assert!(!r.contains("grpc-status"), "{r:?}");
}

#[test]
fn gzip_runs_on_the_helper_thread() {
    let (port, _) = serve(limits());
    let r = {
        let mut s = connect(port);
        s.write_all(b"GET /zip HTTP/1.1\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        out
    };
    let at = r.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let mut back = Vec::new();
    flate2::read::GzDecoder::new(&r[at..])
        .read_to_end(&mut back)
        .unwrap();
    assert_eq!(back, b"hello ".repeat(100));
}

/// a body that gzips to different bytes at each level
static ZIP_TEXT: std::sync::LazyLock<Vec<u8>> = std::sync::LazyLock::new(|| {
    (0..20_000u32)
        .map(|i| b"abcdefgh ijklmnop"[(i.wrapping_mul(2654435761) >> 28) as usize % 17])
        .collect()
});

#[test]
fn gzip_takes_the_level_of_the_kind() {
    let (port, _) = serve(limits());
    let mut z = Zippers::default();
    for level in 1..=12u8 {
        let r = {
            let mut s = connect(port);
            s.write_all(
                format!("GET /zip/{level} HTTP/1.1\r\nConnection: close\r\n\r\n").as_bytes(),
            )
            .unwrap();
            let mut out = Vec::new();
            let _ = s.read_to_end(&mut out);
            out
        };
        let at = r.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let mut back = Vec::new();
        flate2::read::GzDecoder::new(&r[at..])
            .read_to_end(&mut back)
            .unwrap();
        assert_eq!(back, *ZIP_TEXT, "level {level}");
        // the same bytes as this level here
        assert_eq!(&r[at..], &z.gzip(&ZIP_TEXT, level)[..], "level {level}");
    }
}

/// The gzip thread wakes the poll while a call waits. That call must send
/// the answer: the event of the waker comes only once, so no later call
/// and no idle helper would know about the done job.
#[test]
fn gzip_done_during_the_wait_is_sent_by_the_same_call() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut out = Vec::new();
    let mut s = connect(port);
    accept(&mut core, &mut inb, 1);
    s.write_all(b"GET /zip HTTP/1.1\r\n\r\n").unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while out.is_empty() && Instant::now() < end {
        turn(&mut core, &mut inb, &mut out, 1000);
    }
    assert!(!out.is_empty(), "the app answered");
    // one call: it gives the job to the gzip thread, then waits
    assert_eq!(core.run(&out, &mut inb, 2_000_000), Ok(0));
    let mut head = [0u8; 15];
    s.read_exact(&mut head)
        .expect("the answer, with no other call");
    assert_eq!(&head, b"HTTP/1.1 200 OK");
}

/// A graceful shutdown: no new connections, the open request gets its
/// answer (with `connection: close`), then it is done.
#[test]
fn graceful_shutdown_finishes_open_requests() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    s.write_all(b"GET /open HTTP/1.1\r\n\r\n").unwrap();
    let req = collect(&mut core, &mut inb, 1).pop().unwrap();
    core.start_drain();
    assert!(
        StdStream::connect(("127.0.0.1", port)).is_err(),
        "no new connections"
    );
    assert!(!core.done_due(), "a request is open");
    let mut out = Vec::new();
    ans(&mut out, req.id, 200, 0, b"", b"bye");
    core.run(&out, &mut inb, 0).unwrap();
    let mut got = String::new();
    let _ = s.read_to_string(&mut got);
    assert!(
        got.contains("connection: close\r\n") && got.ends_with("bye"),
        "{got:?}"
    );
    for _ in 0..100 {
        core.run(&[], &mut inb, 1000).unwrap();
        if core.done_due() {
            return;
        }
    }
    panic!("not done");
}

/// A client pipelines requests and never reads the answers: Rust stops
/// reading its requests while the answers wait, so memory stays small.
#[test]
fn pipelined_requests_stop_while_answers_wait() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    let all: String = (0..300)
        .map(|i| format!("GET /{i} HTTP/1.1\r\n\r\n"))
        .collect();
    s.write_all(all.as_bytes()).unwrap();
    let body = vec![b'x'; 200_000];
    let (mut seen, mut out) = (0, Vec::new());
    let end = Instant::now() + Duration::from_millis(500);
    while Instant::now() < end {
        let n = core.run(&out, &mut inb, 1000).unwrap();
        out.clear();
        for r in records(&inb[..n]) {
            seen += 1;
            ans(&mut out, r.id, 200, 0, b"", &body);
        }
    }
    let pending: usize = core.conns.iter().map(|(_, c)| c.pending()).sum();
    assert!(seen < 300, "read all {seen} requests");
    assert!(
        pending < MAX_PENDING + MAX_INFLIGHT * body.len(),
        "{pending} bytes wait"
    );
}

/// A client that never reads its answer: when the write makes no progress
/// for SEND_WAIT, the connection is closed.
#[test]
fn answer_to_a_client_that_does_not_read_times_out() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    s.write_all(b"GET /big HTTP/1.1\r\n\r\n").unwrap();
    let req = collect(&mut core, &mut inb, 1).pop().unwrap();
    let mut out = Vec::new();
    ans(&mut out, req.id, 200, 0, b"", &vec![0u8; 16 << 20]);
    core.run(&out, &mut inb, 0).unwrap();
    assert!(core.conns.iter().any(|(_, c)| c.pending() > 0), "waits");
    after(&mut core, &mut inb, SEND_WAIT / 2);
    assert_eq!(core.conns.len(), 1, "too early");
    after(&mut core, &mut inb, SEND_WAIT + Duration::from_secs(1));
    assert_eq!(core.conns.len(), 0, "closed");
}

/// The idle time of a keep-alive connection counts from its last answer,
/// not from its last request.
#[test]
fn idle_time_counts_from_the_answer() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    s.write_all(b"GET /slow HTTP/1.1\r\n\r\n").unwrap();
    let req = collect(&mut core, &mut inb, 1).pop().unwrap();
    // Dart answers 20 seconds later
    core.now = Instant::now() + Duration::from_secs(20);
    let mut out = Vec::new();
    ans(&mut out, req.id, 200, 0, b"", b"done");
    core.respond(&out);
    after(&mut core, &mut inb, IDLE + Duration::from_secs(5));
    assert_eq!(core.conns.len(), 1, "closed 5 seconds after the answer");
    after(&mut core, &mut inb, IDLE + Duration::from_secs(25));
    assert_eq!(core.conns.len(), 0, "idle for IDLE");
}

/// A longer idle timeout (eg. for a load balancer that keeps connections
/// for 60 seconds) keeps an idle connection past the default 30 seconds.
#[test]
fn idle_timeout_comes_from_the_limits() {
    let (mut core, port) = new_core(Limits {
        idle: Duration::from_secs(65),
        ..limits()
    });
    let mut inb = vec![0u8; 1 << 18];
    let _s = connect(port);
    accept(&mut core, &mut inb, 1);
    after(&mut core, &mut inb, Duration::from_secs(60));
    assert_eq!(core.conns.len(), 1, "open after 60 seconds");
    after(&mut core, &mut inb, Duration::from_secs(66));
    assert_eq!(core.conns.len(), 0, "closed after 65 seconds");
}

/// A streamed body with `content-length`: the right count keeps the
/// connection. Too many bytes stop at the length, too few cut the body.
/// Both close the connection, so the next request gets no answer that a
/// client could mix up with the rest of the body.
#[test]
fn streamed_body_keeps_to_its_content_length() {
    let (port, log) = serve(limits());
    let two = |path: &str| {
        let req =
            format!("GET {path} HTTP/1.1\r\n\r\nGET /next HTTP/1.1\r\nConnection: close\r\n\r\n");
        send(port, req.as_bytes())
    };
    let exact = two("/exact");
    assert!(
        exact.contains("content-length: 5\r\n") && exact.contains("\r\n\r\nabcdeHTTP/1.1 200"),
        "{exact:?}"
    );
    assert!(exact.ends_with("/next"), "{exact:?}");
    let liar = two("/liar");
    assert!(liar.ends_with("\r\n\r\nabc"), "{liar:?}");
    wait_for(&log, |r| r.kind() == STREAM_GONE);
    let short = two("/short");
    assert!(short.ends_with("\r\n\r\nabcde"), "{short:?}");
}

/// `Content-Length` is ASCII digits only. `+5`, a list or other white
/// space gets 400, so no proxy in front can read another length.
#[test]
fn content_length_is_digits_only() {
    let (port, _) = serve(limits());
    for cl in [
        "+5",
        "5, 5",
        "5\u{a0}",
        "0x5",
        "-5",
        "",
        "99999999999999999999999",
    ] {
        let req = format!("POST / HTTP/1.1\r\nContent-Length: {cl}\r\n\r\nhello");
        let r = send(port, req.as_bytes());
        assert!(r.starts_with("HTTP/1.1 400"), "{cl:?}: {r:?}");
    }
    let r = send(
        port,
        b"POST /p HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
    );
    assert!(r.ends_with("/phello"), "the same length twice: {r:?}");
}

/// A raw UTF-8 byte in the target goes to Dart as `%XX`.
#[test]
fn raw_bytes_in_the_target_are_escaped() {
    let (port, _) = serve(limits());
    let r = send(
        port,
        b"GET /\xc3\xa9?q=\xc3\xa9 HTTP/1.1\r\nConnection: close\r\n\r\n",
    );
    assert!(r.ends_with("\r\n\r\n/%C3%A9?q=%C3%A9"), "{r:?}");
}

/// A huge body timeout from Dart does not overflow the clock.
#[test]
fn huge_body_timeout_is_cut() {
    let lim = limits_of(1, 1, i64::MAX, 0, i64::MAX, false, 250, 0, 100);
    assert_eq!(lim.timeout, MAX_TIMEOUT);
    assert_eq!(lim.idle, MAX_TIMEOUT);
    let _ = Instant::now() + lim.timeout + lim.idle; // no panic
}

/// With a connection limit, Rust stops accepting at the limit, and accepts
/// again when a connection closes. An idle connection holds no read buffer.
#[test]
fn connection_limit_and_no_buffer_for_idle_connections() {
    let (mut core, port) = new_core(Limits {
        conns: 1,
        ..limits()
    });
    let mut inb = vec![0u8; 1 << 18];
    let a = connect(port);
    accept(&mut core, &mut inb, 1);
    let (_, c) = core.conns.iter().next().unwrap();
    assert_eq!(c.rbuf.capacity(), 0, "an idle connection holds a buffer");
    let mut b = connect(port); // waits in the listen queue
    b.write_all(b"GET /b HTTP/1.1\r\nConnection: close\r\n\r\n")
        .unwrap();
    assert!(collect(&mut core, &mut inb, 1).is_empty(), "over the limit");
    assert_eq!(core.conns.len(), 1);
    drop(a);
    let got = collect(&mut core, &mut inb, 1);
    assert_eq!(got.first().map(|r| r.target.as_str()), Some("/b"));
}

/// Reads go into the core's one shared buffer. A keep-alive connection
/// holds no read buffer after a whole request, and only the part of a
/// request that is not whole yet. A big answer leaves no big write buffer.
#[test]
fn keep_alive_connections_share_one_read_buffer() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    s.write_all(b"GET /a HTTP/1.1\r\n\r\n").unwrap();
    let got = collect(&mut core, &mut inb, 1);
    assert_eq!(got.len(), 1);
    let rbuf = |core: &Core| core.conns.iter().next().unwrap().1.rbuf.capacity();
    assert_eq!(rbuf(&core), 0, "a whole request left a buffer");
    assert!(core.scratch.capacity() >= READ_ROOM);
    // a big answer: the write buffer is freed once it is written
    let mut out = Vec::new();
    ans(&mut out, got[0].id, 200, 0, b"", &vec![b'x'; 200_000]);
    core.run(&out, &mut inb, 0).unwrap();
    let mut seen = 0;
    let mut buf = [0u8; 65536];
    while seen < 200_000 {
        seen += s.read(&mut buf).unwrap();
        core.run(&[], &mut inb, 0).unwrap();
    }
    for _ in 0..10 {
        core.run(&[], &mut inb, 0).unwrap();
    }
    let c = core.conns.iter().next().unwrap().1;
    assert!(c.wbuf.capacity() <= KEEP_WBUF, "{}", c.wbuf.capacity());
    // a request in two parts: only the first part is kept, then nothing
    let part = b"GET /b HTTP/1.1\r\nHo";
    s.write_all(part).unwrap();
    let end = Instant::now() + Duration::from_secs(2);
    while rbuf(&core) == 0 && Instant::now() < end {
        core.run(&[], &mut inb, 1000).unwrap();
    }
    let c = core.conns.iter().next().unwrap().1;
    assert_eq!(
        (c.rlen - c.rpos, c.rbuf.capacity()),
        (part.len(), part.len())
    );
    s.write_all(b"st: x\r\n\r\nGET /c HTTP/1.1\r\n\r\n")
        .unwrap();
    let got = collect(&mut core, &mut inb, 2);
    let targets: Vec<&str> = got.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(targets, ["/b", "/c"]);
    assert_eq!(rbuf(&core), 0);
}

/// Parses one request with room for its record.
fn parse_req(req: &[u8]) -> Step {
    let mut inb = vec![0u8; 4096];
    parse_one(req, 1, &[0; 18], &mut inb, 0, &limits(), Instant::now())
}

/// Bad framing that a proxy in front may read another way gets 400
/// (RFC 9112 6.1 and 6.3). Found by the fuzz oracle.
#[test]
fn transfer_encoding_in_http10_or_twice_is_refused() {
    for req in [
        &b"POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"[..],
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n",
    ] {
        assert!(matches!(parse_req(req), Step::Fail(400)), "{req:?}");
    }
    let ok = b"POST / HTTP/1.1\r\nTransfer-Encoding: Chunked\r\n\r\n";
    assert!(matches!(parse_req(ok), Step::Body(..)));
}

/// `close` in any `Connection` header closes, also with `keep-alive` in
/// another one (RFC 9112 9.3). Found by the fuzz oracle.
#[test]
fn connection_close_wins_over_keep_alive() {
    let close = |req: &[u8]| match parse_req(req) {
        Step::Done(_, s) => s.close,
        _ => panic!("{req:?}"),
    };
    assert!(close(
        b"GET / HTTP/1.1\r\nConnection: close\r\nConnection: keep-alive\r\n\r\n"
    ));
    assert!(close(
        b"GET / HTTP/1.1\r\nConnection: keep-alive\r\nConnection: close\r\n\r\n"
    ));
    assert!(!close(b"GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n"));
    assert!(close(b"GET / HTTP/1.0\r\n\r\n"));
    assert!(!close(b"GET / HTTP/1.1\r\n\r\n"));
}

/// A WebSocket key or version given twice gives no upgrade (RFC 6455
/// 11.3). `websocket` in any `Upgrade` header counts. Found by the fuzz
/// oracle.
#[test]
fn websocket_upgrade_headers_count_once() {
    let up = |extra: &str| {
        let extra = extra.replace("{K}", "dGhlIHNhbXBsZSBub25jZQ==");
        let req =
            format!("GET /ws HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n{extra}\r\n");
        match parse_req(req.as_bytes()) {
            Step::Done(_, s) => s.key.is_some(),
            _ => panic!("{req:?}"),
        }
    };
    assert!(up(
        "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {K}\r\n"
    ));
    assert!(up(
        "Upgrade: h2c\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {K}\r\n"
    ));
    assert!(!up(
        "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {K}\r\nSec-WebSocket-Key: {K}\r\n"
    ));
    assert!(!up(
        "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {K}\r\n"
    ));
    // `100-continue` in any `Expect` header counts
    let req = b"POST / HTTP/1.1\r\nExpect: 100-continue\r\nExpect: x\r\nContent-Length: 5\r\n\r\n";
    assert!(matches!(parse_req(req), Step::Body(_, _, true)));
}

/// A `Sec-WebSocket-Key` must be base64 of 16 bytes (RFC 6455 4.2.1).
/// Another key gives a normal request, with no upgrade.
#[test]
fn websocket_key_must_be_16_bytes_of_base64() {
    for (key, ok) in [
        ("dGhlIHNhbXBsZSBub25jZQ==", true),
        ("AAAAAAAAAAAAAAAAAAAAA+/=", false),
        ("AAAAAAAAAAAAAAAAAAAAA+==", true),
        ("", false),
        ("a", false),
        ("dGhlIHNhbXBsZSBub25jZQ=", false),
        ("dGhlIHNhbXBsZSBub25jZQ", false),
        ("dGhlIHNhbXBsZSBub25jZQ!=", false),
        ("dGhlIHNhbXBsZSBub25jZ===", false),
        ("dGhlIHNhbXBsZSBub25jZQAA", false),
    ] {
        let req = format!(
            "GET /ws HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {key}\r\n\r\n"
        );
        match parse_req(req.as_bytes()) {
            Step::Done(_, s) => assert_eq!(s.key.is_some(), ok, "{key:?}"),
            _ => panic!("{key:?}"),
        }
    }
}

/// Only `multipart/form-data` gets the multipart limit, not any type that
/// holds the text `form-data`.
#[test]
fn only_multipart_form_data_gets_the_multipart_limit() {
    // body limit 10, multipart limit 20: a 15 byte body
    for (ty, ok) in [
        ("multipart/form-data; boundary=x", true),
        (" Multipart/Form-Data ", true),
        ("multipart/x-form-data", false),
        ("text/plain; a=form-data", false),
        ("application/x-www-form-urlencoded", false),
    ] {
        let req = format!(
            "POST / HTTP/1.1\r\nContent-Type: {ty}\r\nExpect: 100-continue\r\nContent-Length: 15\r\n\r\n"
        );
        let step = parse_req(req.as_bytes());
        assert_eq!(matches!(step, Step::Body(..)), ok, "{ty:?}");
        assert_eq!(matches!(step, Step::Fail(413)), !ok, "{ty:?}");
    }
}

/// Runs turns until the core saw the client close its side.
fn until_eof(core: &mut Core, inb: &mut [u8]) {
    let end = Instant::now() + Duration::from_secs(5);
    while !core.conns.iter().any(|(_, c)| c.eof) {
        assert!(Instant::now() < end, "no eof");
        core.run(&[], inb, 1000).unwrap();
    }
}

/// The client leaves while Dart never answers: after the idle time the
/// connection (and its socket) is gone. A late answer is dropped.
#[test]
fn client_that_left_a_hanging_request_is_closed() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    s.write_all(b"GET /hang HTTP/1.1\r\n\r\n").unwrap();
    let req = collect(&mut core, &mut inb, 1).pop().unwrap();
    drop(s);
    until_eof(&mut core, &mut inb);
    assert!(after(&mut core, &mut inb, IDLE / 2).is_empty());
    assert_eq!(core.conns.len(), 1, "too early");
    let got = after(&mut core, &mut inb, IDLE + Duration::from_secs(1));
    assert_eq!(core.conns.len(), 0, "closed");
    // Dart hears it, so `response.done` completes
    assert_eq!(
        got.iter().map(|r| (r.id, r.kind())).collect::<Vec<_>>(),
        [(req.id, REQUEST_GONE)]
    );
    let mut out = Vec::new();
    ans(&mut out, req.id, 200, 0, b"", b"late");
    assert_eq!(core.run(&out, &mut inb, 0), Ok(0), "late answer dropped");
}

/// A streamed body that writes nothing after the client left: Dart hears
/// STREAM_GONE and the stream entry goes.
#[test]
fn quiet_stream_of_a_client_that_left_is_gone() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    s.write_all(b"GET /sse HTTP/1.1\r\n\r\n").unwrap();
    let req = collect(&mut core, &mut inb, 1).pop().unwrap();
    let mut out = Vec::new();
    ans(
        &mut out,
        req.id,
        200,
        6,
        b"",
        &[&77u64.to_le_bytes()[..], b"hi"].concat(),
    );
    core.run(&out, &mut inb, 0).unwrap();
    assert_eq!(core.streams.len(), 1);
    // read all, so the close is a FIN and not a reset (a reset closes at
    // once: the read fails)
    let mut got = Vec::new();
    let mut buf = [0u8; 1024];
    while !got.ends_with(b"hi\r\n") {
        let k = s.read(&mut buf).unwrap();
        got.extend_from_slice(&buf[..k]);
    }
    drop(s);
    until_eof(&mut core, &mut inb);
    assert!(
        after(&mut core, &mut inb, IDLE / 2)
            .iter()
            .all(|r| r.kind() != STREAM_GONE)
    );
    let got = after(&mut core, &mut inb, IDLE + Duration::from_secs(1));
    assert!(
        got.iter().any(|r| r.id == 77 && r.kind() == STREAM_GONE),
        "{got:?}"
    );
    assert!(core.streams.is_empty() && core.conns.is_empty());
}

/// Half-close: the client sends its request, closes its side and waits.
/// An answer within the idle time still reaches it.
#[test]
fn half_closed_client_gets_its_answer() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    s.write_all(b"GET /half HTTP/1.1\r\n\r\n").unwrap();
    s.shutdown(Shutdown::Write).unwrap();
    let req = collect(&mut core, &mut inb, 1).pop().unwrap();
    until_eof(&mut core, &mut inb);
    after(&mut core, &mut inb, IDLE / 2);
    let mut out = Vec::new();
    ans(&mut out, req.id, 200, 0, b"", b"late but here");
    core.respond(&out);
    let mut got = String::new();
    let _ = s.read_to_string(&mut got);
    assert!(got.ends_with("late but here"), "{got:?}");
}

/// A slow handler with a client that stays: no timer closes it.
#[test]
fn slow_answer_to_a_connected_client_arrives() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    s.write_all(b"GET /slow HTTP/1.1\r\n\r\n").unwrap();
    let req = collect(&mut core, &mut inb, 1).pop().unwrap();
    after(&mut core, &mut inb, IDLE * 3);
    assert_eq!(core.conns.len(), 1, "still open");
    let mut out = Vec::new();
    ans(&mut out, req.id, 200, 0, b"", b"slow");
    core.respond(&out);
    let mut buf = [0u8; 1024];
    let k = s.read(&mut buf).unwrap();
    assert!(buf[..k].ends_with(b"slow"));
}

/// `pingInterval`: a ping goes out after a quiet interval. A client that
/// does not answer is closed one interval later, with 1001 like dart:io.
#[test]
fn websocket_ping_closes_a_client_that_does_not_answer() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let (id, _tx) = ws_on(&mut core, &mut inb, port, "/ws");
    let mut out = Vec::new();
    ans(&mut out, id, 0, 9, b"", &1000u64.to_le_bytes());
    core.run(&out, &mut inb, 0).unwrap();
    assert!(after(&mut core, &mut inb, Duration::from_millis(500)).is_empty());
    assert!(
        after(&mut core, &mut inb, Duration::from_millis(1100)).is_empty(),
        "pinged"
    );
    assert!(
        core.conns
            .iter()
            .any(|(_, c)| c.ws.as_ref().unwrap().pinged.is_some())
    );
    let got = after(&mut core, &mut inb, Duration::from_millis(2200));
    assert_eq!(
        (got[0].id, got[0].kind(), got[0].code),
        (id, WS_CLOSED, 1001)
    );
    assert!(core.conns.is_empty());
}

/// A client that answers the pings stays open. Interval 0 turns it off.
#[test]
fn websocket_ping_keeps_a_client_that_answers() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    std::thread::spawn(move || {
        let mut ws = ws_client(port, "/ws");
        // tungstenite answers pings while it reads
        while ws.read().is_ok() {}
    });
    let id = upgrade(&mut core, &mut inb);
    let mut out = Vec::new();
    ans(&mut out, id, 0, 9, b"", &1000u64.to_le_bytes());
    core.run(&out, &mut inb, 0).unwrap();
    after(&mut core, &mut inb, Duration::from_millis(1100));
    let end = Instant::now() + Duration::from_secs(5);
    while core
        .conns
        .iter()
        .any(|(_, c)| c.ws.as_ref().unwrap().pinged.is_some())
    {
        assert!(Instant::now() < end, "no pong");
        core.run(&[], &mut inb, 1000).unwrap();
    }
    assert!(after(&mut core, &mut inb, Duration::from_millis(1500)).is_empty());
    assert_eq!(core.conns.len(), 1, "open");
    out.clear();
    ans(&mut out, id, 0, 9, b"", &0u64.to_le_bytes());
    core.run(&out, &mut inb, 0).unwrap();
    assert!(after(&mut core, &mut inb, Duration::from_secs(60)).is_empty());
    assert!(
        core.conns
            .iter()
            .all(|(_, c)| c.ws.as_ref().unwrap().pinged.is_none())
    );
}

/// Accepted sockets get TCP keep-alive. On Linux and macOS it is set
/// once on the listener: this checks that they inherit all of it.
#[test]
fn accepted_sockets_have_keepalive() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let _s = connect(port);
    accept(&mut core, &mut inb, 1);
    let (_, c) = core.conns.iter().next().unwrap();
    let sock = socket2::SockRef::from(&c.sock);
    assert!(sock.keepalive().unwrap());
    #[cfg(not(windows))]
    {
        assert_eq!(sock.tcp_keepalive_time().unwrap(), KEEPALIVE);
        assert_eq!(
            sock.tcp_keepalive_interval().unwrap(),
            Duration::from_secs(10)
        );
        assert_eq!(sock.tcp_keepalive_retries().unwrap(), 6);
    }
}

/// The pong is in the socket, but Dart was busy longer than the interval:
/// the timer runs before the read. The client must not be closed.
#[test]
fn websocket_pong_waiting_in_the_socket_keeps_the_client() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    std::thread::spawn(move || {
        let mut ws = ws_client(port, "/ws");
        // tungstenite answers pings while it reads
        while ws.read().is_ok() {}
    });
    let id = upgrade(&mut core, &mut inb);
    let mut out = Vec::new();
    ans(&mut out, id, 0, 9, b"", &1000u64.to_le_bytes());
    core.run(&out, &mut inb, 0).unwrap();
    // the ping goes out
    after(&mut core, &mut inb, Duration::from_millis(1100));
    assert!(
        core.conns
            .iter()
            .all(|(_, c)| c.ws.as_ref().unwrap().pinged.is_some())
    );
    // the pong arrives, but the loop does not read it now
    std::thread::sleep(Duration::from_millis(300));
    // the pong time is over: the timer runs first and reads the pong
    assert!(after(&mut core, &mut inb, Duration::from_millis(2200)).is_empty());
    assert_eq!(core.conns.len(), 1, "open");
    assert!(
        core.conns
            .iter()
            .all(|(_, c)| c.ws.as_ref().unwrap().pinged.is_none())
    );
}

/// A streamed 204 or 304 sends only its head: no chunk end, no body. The
/// keep-alive connection then serves the next request.
#[test]
fn streamed_answer_with_no_body_status_keeps_the_connection() {
    for status in [204u16, 304] {
        let (mut core, port) = new_core(limits());
        let mut inb = vec![0u8; 1 << 18];
        let mut s = connect(port);
        s.write_all(b"GET /a HTTP/1.1\r\n\r\n").unwrap();
        let req = collect(&mut core, &mut inb, 1).pop().unwrap();
        let mut out = Vec::new();
        ans(
            &mut out,
            req.id,
            status,
            6,
            b"",
            &[&9u64.to_le_bytes()[..], b"body"].concat(),
        );
        ans(&mut out, 9, 0, 7, b"", b"more");
        ans(&mut out, 9, 0, 8, b"", b"");
        let n = core.run(&out, &mut inb, 0).unwrap();
        // Dart is told to stop sending
        assert!(
            records(&inb[..n])
                .iter()
                .any(|r| r.id == 9 && r.kind() == STREAM_GONE)
        );
        s.write_all(b"GET /b HTTP/1.1\r\n\r\n").unwrap();
        let req = collect(&mut core, &mut inb, 1).pop().unwrap();
        out.clear();
        ans(&mut out, req.id, 200, 0, b"", b"second");
        core.run(&out, &mut inb, 0).unwrap();
        let mut got = Vec::new();
        let mut buf = [0u8; 1024];
        while !got.ends_with(b"second") {
            let k = s.read(&mut buf).unwrap();
            assert!(k > 0, "closed: {:?}", String::from_utf8_lossy(&got));
            got.extend_from_slice(&buf[..k]);
        }
        let text = String::from_utf8_lossy(&got);
        let first = text.split("HTTP/1.1 200").next().unwrap();
        assert!(first.starts_with(&format!("HTTP/1.1 {status}")), "{text:?}");
        assert!(first.ends_with("\r\n\r\n"), "{text:?}");
        assert!(
            !first.contains("body") && !first.contains("\r\n0\r\n") && !first.contains("chunked"),
            "{text:?}"
        );
    }
}

/// An upgrade request stops reading until Dart answers. A client that
/// leaves meanwhile is still found (the poll sees the close): after the
/// idle time the connection goes and Dart hears it.
#[test]
fn client_that_left_a_waiting_upgrade_is_closed() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    s.write_all(
        b"GET /ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
          Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
    )
    .unwrap();
    let req = collect(&mut core, &mut inb, 1).pop().unwrap();
    assert!(core.conns.iter().all(|(_, c)| c.upgrading));
    drop(s);
    let end = Instant::now() + Duration::from_secs(5);
    while !core.conns.iter().any(|(_, c)| c.hup) {
        assert!(Instant::now() < end, "no hup");
        core.run(&[], &mut inb, 1000).unwrap();
    }
    assert!(after(&mut core, &mut inb, IDLE / 2).is_empty());
    let got = after(&mut core, &mut inb, IDLE + Duration::from_secs(1));
    assert!(core.conns.is_empty(), "closed");
    assert_eq!(
        got.iter().map(|r| (r.id, r.kind())).collect::<Vec<_>>(),
        [(req.id, REQUEST_GONE)]
    );
}

/// With HTTP/2 on, the preface may come in pieces. Answers go out as
/// frames, a gzip answer too. The same port still speaks HTTP/1.1.
#[test]
fn http2_preface_in_pieces_and_http1_on_the_same_port() {
    use crate::h2::frame;
    use crate::h2::hpack::{Decoder, encode_field};
    let (port, _) = serve(limits());
    let mut req = PREFACE.to_vec();
    frame::write(&mut req, frame::SETTINGS, 0, 0, b"");
    for (id, path) in [(1, "/h2"), (3, "/zip")] {
        let mut block = Vec::new();
        for (n, v) in [(":method", "GET"), (":scheme", "http"), (":path", path)] {
            encode_field(n.as_bytes(), v.as_bytes(), &mut block);
        }
        frame::write_headers(&mut req, id, &block, true, 16 * 1024);
    }
    let mut s = connect(port);
    for part in [&req[..3], &req[3..20], &req[20..]] {
        s.write_all(part).unwrap();
        std::thread::sleep(Duration::from_millis(20));
    }
    // read frames until both streams ended: (headers, body) per stream
    let mut got = Vec::new();
    let mut dec = Decoder::new(4096);
    let mut seen: HashMap<u32, (Vec<String>, Vec<u8>)> = HashMap::new();
    let mut ended = 0;
    let mut p = 0;
    while ended < 2 {
        let mut buf = [0u8; 4096];
        let k = s.read(&mut buf).unwrap();
        assert!(k > 0, "closed");
        got.extend_from_slice(&buf[..k]);
        while let Some(h) = frame::read_head(&got[p..]) {
            let end = p + frame::HEAD + h.len;
            if end > got.len() {
                break;
            }
            let payload = &got[p + frame::HEAD..end];
            let s = seen.entry(h.id).or_default();
            match h.kind {
                frame::HEADERS => dec
                    .decode(payload, usize::MAX, |n, v| {
                        s.0.push(format!(
                            "{}: {}",
                            String::from_utf8_lossy(n),
                            String::from_utf8_lossy(v)
                        ))
                    })
                    .map(|_| ())
                    .unwrap(),
                frame::DATA => s.1.extend_from_slice(payload),
                _ => {}
            }
            if h.id != 0 && h.flags & frame::END_STREAM != 0 {
                ended += 1;
            }
            p = end;
        }
    }
    assert_eq!(seen[&1].1, b"/h2");
    assert!(
        seen[&3].0.contains(&"content-encoding: gzip".to_string()),
        "{:?}",
        seen[&3].0
    );
    let mut plain = Vec::new();
    flate2::read::GzDecoder::new(&seen[&3].1[..])
        .read_to_end(&mut plain)
        .unwrap();
    assert_eq!(plain, b"hello ".repeat(100));
    let h1 = send(port, b"GET /h1 HTTP/1.1\r\nConnection: close\r\n\r\n");
    assert!(
        h1.starts_with("HTTP/1.1 200") && h1.ends_with("/h1"),
        "{h1:?}"
    );
}

/// A client with a send window of 0 that keeps sending PINGs is stuck all
/// the same: PING ACKs are no progress, so SEND_WAIT closes the connection.
#[test]
fn http2_pings_do_not_keep_a_blocked_window_alive() {
    use crate::h2::frame;
    use crate::h2::hpack::encode_field;
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut req = PREFACE.to_vec();
    frame::write(&mut req, frame::SETTINGS, 0, 0, b"");
    let mut block = Vec::new();
    for (n, v) in [(":method", "GET"), (":scheme", "http"), (":path", "/big")] {
        encode_field(n.as_bytes(), v.as_bytes(), &mut block);
    }
    frame::write_headers(&mut req, 1, &block, true, 16 * 1024);
    let mut s = connect(port);
    s.write_all(&req).unwrap();
    let r = collect(&mut core, &mut inb, 1).pop().unwrap();
    // more than the default window of 65535 bytes
    let mut out = Vec::new();
    ans(&mut out, r.id, 200, 0, b"", &vec![0u8; 200_000]);
    core.run(&out, &mut inb, 0).unwrap();
    assert_eq!(core.conns.len(), 1);
    // a PING every 20 s, never a WINDOW_UPDATE. `run` reads the real
    // clock, so 20 s pass by moving the start of the wait back.
    for _ in 0..3 {
        let mut ping = Vec::new();
        frame::write(&mut ping, frame::PING, 0, 0, &[7; 8]);
        s.write_all(&ping).unwrap();
        let end = Instant::now() + Duration::from_millis(300);
        while Instant::now() < end {
            core.run(&[], &mut inb, 0).unwrap();
        }
        let ids: Vec<usize> = core.conns.iter().map(|(i, _)| i).collect();
        for i in ids {
            let c = core.conns.get_mut(i).unwrap();
            c.stuck = c.stuck.map(|t| t - Duration::from_secs(20));
            core.arm_timer(i);
        }
        after(&mut core, &mut inb, Duration::ZERO);
    }
    assert_eq!(core.conns.len(), 0, "closed");
}

/// A connection with an open body and a GOAWAY error that can not be
/// written (the client does not read): the body timer must not run again
/// and again. The send stall closes the connection in the end.
#[test]
fn http2_dead_connection_with_an_open_body_does_not_spin() {
    use crate::h2::frame;
    use crate::h2::hpack::encode_field;
    let (mut core, port) = new_core(Limits {
        timeout: Duration::from_secs(5),
        ..limits()
    });
    let mut inb = vec![0u8; 1 << 18];
    let mut req = PREFACE.to_vec();
    frame::write(&mut req, frame::SETTINGS, 0, 0, b"");
    let mut block = Vec::new();
    for (n, v) in [
        (":method", "POST"),
        (":scheme", "http"),
        (":path", "/"),
        ("content-length", "10"),
    ] {
        encode_field(n.as_bytes(), v.as_bytes(), &mut block);
    }
    frame::write_headers(&mut req, 1, &block, false, 16 * 1024);
    frame::write(&mut req, frame::DATA, 0, 1, b"abc");
    let mut s = connect(port);
    s.write_all(&req).unwrap();
    for _ in 0..20 {
        core.run(&[], &mut inb, 0).unwrap();
        if core
            .conns
            .iter()
            .any(|(_, c)| c.h2.as_ref().is_some_and(|h| h.open() == 1))
        {
            break;
        }
    }
    // a bad frame (WINDOW_UPDATE on a stream that never was) makes the
    // connection dead, and GOAWAY waits in a write buffer that is full
    let (idx, _) = core.conns.iter().next().unwrap();
    let c = core.conns.get_mut(idx).unwrap();
    let mut bad = Vec::new();
    frame::write(&mut bad, frame::WINDOW_UPDATE, 0, 99, &1u32.to_be_bytes());
    let (mut pos, mut seq) = (0, 7);
    let now = Instant::now();
    let h2 = c.h2.as_mut().unwrap();
    h2.step(&bad, &mut pos, &mut c.wbuf, &mut seq, b"", now);
    c.wbuf.extend(vec![0u8; 64 << 20]);
    c.stuck = Some(now);
    core.arm_timer(idx);
    let (_, c) = core.conns.iter().next().unwrap();
    assert!(c.h2.as_ref().unwrap().over(), "the connection is dead");
    assert!(c.pending() > 0, "GOAWAY waits");
    // after the body timeout: the timers return (no loop)
    after(&mut core, &mut inb, Duration::from_secs(6));
    assert_eq!(core.conns.len(), 1, "too early");
    // the send stall closes it
    after(&mut core, &mut inb, SEND_WAIT + Duration::from_secs(6));
    assert_eq!(core.conns.len(), 0, "closed");
}

/// An idle HTTP/2 connection (no open stream) gets GOAWAY NO_ERROR after
/// the idle time, then closes. A PING (and our ACK) is no activity.
#[test]
fn http2_idle_connection_gets_goaway() {
    use crate::h2::frame;
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    let mut req = PREFACE.to_vec();
    frame::write(&mut req, frame::SETTINGS, 0, 0, b"");
    s.write_all(&req).unwrap();
    accept(&mut core, &mut inb, 1);
    let idx = core.conns.iter().next().unwrap().0;
    let end = Instant::now() + Duration::from_secs(3);
    while core.conns[idx].h2.is_none() && Instant::now() < end {
        core.run(&[], &mut inb, 1000).unwrap();
    }
    let since = core.conns[idx].since;
    // a PING: the ACK goes out, the idle time does not start again
    let mut ping = Vec::new();
    frame::write(&mut ping, frame::PING, 0, 0, &[7; 8]);
    s.write_all(&ping).unwrap();
    let mut got = Vec::new();
    while !got
        .windows(9)
        .any(|w| w[3] == frame::PING && w[4] == frame::ACK)
    {
        core.run(&[], &mut inb, 1000).unwrap();
        let mut buf = [0u8; 256];
        s.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
        if let Ok(k) = s.read(&mut buf) {
            got.extend_from_slice(&buf[..k]);
        }
    }
    assert_eq!(core.conns[idx].since, since, "a PING is no activity");
    after(&mut core, &mut inb, IDLE / 2);
    assert_eq!(core.conns.len(), 1, "too early");
    after(&mut core, &mut inb, IDLE + Duration::from_secs(1));
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut rest = Vec::new();
    let _ = s.read_to_end(&mut rest);
    // the last frame is GOAWAY NO_ERROR (last stream id 0)
    assert!(rest.len() >= 17, "{rest:?}");
    let g = &rest[rest.len() - 17..];
    assert_eq!(
        (g[3], &g[9..17]),
        (frame::GOAWAY, &[0u8; 8][..]),
        "{rest:?}"
    );
    // closed: only the lingering close is left
    assert!(core.conns.iter().all(|(_, c)| c.linger.is_some()));
}

/// A gRPC request over a real socket: its body parts reach Dart, a
/// BODY_ACK gives the window back (WINDOW_UPDATE on the wire), and when
/// the connection dies the body ends with a non-zero status, once, before
/// REQUEST_GONE.
#[test]
fn http2_streamed_body_acks_and_connection_death() {
    use crate::h2::frame;
    use crate::h2::hpack::encode_field;
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut req = PREFACE.to_vec();
    frame::write(&mut req, frame::SETTINGS, 0, 0, b"");
    let mut block = Vec::new();
    for (n, v) in [
        (":method", "POST"),
        (":scheme", "http"),
        (":path", "/a.B/C"),
        ("content-type", "application/grpc"),
    ] {
        encode_field(n.as_bytes(), v.as_bytes(), &mut block);
    }
    frame::write_headers(&mut req, 1, &block, false, 16 * 1024);
    // 600 KiB: over half the stream window (more than the body limit too)
    let part = vec![5u8; 16 * 1024];
    for _ in 0..(600 / 16) {
        frame::write(&mut req, frame::DATA, 0, 1, &part);
    }
    let mut s = connect(port);
    // 600 KiB is more than the socket buffers hold (smaller on macOS under
    // load), and only `core.run` below reads them. Writing on this thread
    // would block forever, so a second thread writes.
    let mut w = s.try_clone().unwrap();
    let sent = req.clone();
    let writer = std::thread::spawn(move || w.write_all(&sent).unwrap());
    let mut recs = Vec::new();
    let end = Instant::now() + Duration::from_secs(5);
    let want = 600 / 16 * part.len();
    let body = |recs: &[Rec]| -> usize {
        recs.iter()
            .filter(|r| r.kind() == crate::BODY_PART)
            .map(|r| r.body.len())
            .sum()
    };
    while body(&recs) < want && Instant::now() < end {
        let n = core.run(&[], &mut inb, 1000).unwrap();
        recs.extend(records(&inb[..n]));
    }
    writer.join().unwrap();
    assert_eq!(recs[0].flags, 8 | 16, "the request comes first, bit 4");
    assert!(recs[0].body.is_empty());
    assert_eq!(body(&recs), want);
    let id = recs[0].id;
    assert!(
        recs[1..]
            .iter()
            .all(|r| r.id == id && r.kind() == crate::BODY_PART)
    );
    // Dart acks it all: one WINDOW_UPDATE for stream 1
    let mut out = Vec::new();
    ans(&mut out, id, 0, 10, b"", &(want as u64).to_le_bytes());
    core.run(&out, &mut inb, 0).unwrap();
    let mut got = Vec::new();
    s.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let mut buf = [0u8; 4096];
    while let Ok(k @ 1..) = s.read(&mut buf) {
        got.extend_from_slice(&buf[..k]);
    }
    let mut p = 0;
    let mut update = None;
    while let Some(h) = frame::read_head(&got[p..]) {
        if h.kind == frame::WINDOW_UPDATE && h.id == 1 {
            let at = p + frame::HEAD;
            update = Some(u32::from_be_bytes(got[at..at + 4].try_into().unwrap()));
        }
        p += frame::HEAD + h.len;
    }
    assert_eq!(update, Some(want as u32));
    // the connection dies (eg. a read error): the body ends with a
    // non-zero status, once, then the request is gone
    let idx = (id >> 32) as usize;
    core.kill(idx);
    let n = core.run(&[], &mut inb, 0).unwrap();
    let ends: Vec<(u64, u32, u16)> = records(&inb[..n])
        .iter()
        .map(|r| (r.id, r.kind(), r.code))
        .collect();
    assert_eq!(ends, [(id, BODY_END, 1), (id, REQUEST_GONE, 0)]);
    drop(s);

    // the client closes its side: the body ends at once, the answer may
    // still go out. No second end when the connection closes.
    let s = connect(port);
    let writer = write_then_close(
        &s,
        req[..req.len() - 10 * (frame::HEAD + part.len())].to_vec(),
    );
    let mut recs = Vec::new();
    let end = Instant::now() + Duration::from_secs(5);
    while !recs.iter().any(|r: &Rec| r.kind() == BODY_END) && Instant::now() < end {
        let n = core.run(&[], &mut inb, 1000).unwrap();
        recs.extend(records(&inb[..n]));
    }
    writer.join().unwrap();
    let last = recs.last().unwrap();
    assert_eq!((last.kind(), last.code), (BODY_END, 1));
    let id = recs[0].id;
    core.kill((id >> 32) as usize);
    let n = core.run(&[], &mut inb, 0).unwrap();
    let ends: Vec<(u32, u16)> = records(&inb[..n])
        .iter()
        .map(|r| (r.kind(), r.code))
        .collect();
    assert_eq!(ends, [(REQUEST_GONE, 0)]);

    // the client closes its side in the middle of a frame: the body ends
    // at once too
    let s = connect(port);
    let cut = req.len() - 10 * (frame::HEAD + part.len());
    let writer = write_then_close(&s, req[..cut + 5].to_vec());
    let mut recs = Vec::new();
    let end = Instant::now() + Duration::from_secs(5);
    while !recs.iter().any(|r: &Rec| r.kind() == BODY_END) && Instant::now() < end {
        let n = core.run(&[], &mut inb, 1000).unwrap();
        recs.extend(records(&inb[..n]));
    }
    writer.join().unwrap();
    let last = recs.last().unwrap();
    assert_eq!((last.kind(), last.code), (BODY_END, 1));
}

/// The `streams` entry of a started HTTP/2 answer: it stays after the
/// client resets (Dart's end part still has to free the slot), goes when
/// the zombie runs out of time, and goes when the connection closes.
#[test]
fn http2_stream_entry_lives_until_end_expiry_or_close() {
    use crate::h2::frame;
    use crate::h2::hpack::encode_field;
    let (mut core, port) = new_core(Limits {
        max_streams: 1,
        ..limits()
    });
    let mut inb = vec![0u8; 1 << 18];
    let mut block = Vec::new();
    for (n, v) in [(":method", "GET"), (":scheme", "http"), (":path", "/s")] {
        encode_field(n.as_bytes(), v.as_bytes(), &mut block);
    }
    let mut req = PREFACE.to_vec();
    frame::write(&mut req, frame::SETTINGS, 0, 0, b"");
    frame::write_headers(&mut req, 1, &block, true, 16 * 1024);
    let mut s = connect(port);
    s.write_all(&req).unwrap();
    let id = collect(&mut core, &mut inb, 1).pop().unwrap().id;
    let idx = (id >> 32) as usize;
    let mut out = Vec::new();
    ans(
        &mut out,
        id,
        200,
        6,
        b"",
        &[&77u64.to_le_bytes()[..], b"hi"].concat(),
    );
    core.run(&out, &mut inb, 0).unwrap();
    assert!(core.streams.contains_key(&77));
    // the client resets: Dart is told, the entry stays
    let mut rst = Vec::new();
    frame::write(&mut rst, frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
    s.write_all(&rst).unwrap();
    let mut recs = Vec::new();
    let end = Instant::now() + Duration::from_secs(5);
    while !recs.iter().any(|r: &Rec| r.kind() == STREAM_GONE) && Instant::now() < end {
        let n = core.run(&[], &mut inb, 1000).unwrap();
        recs.extend(records(&inb[..n]));
    }
    assert!(recs.iter().any(|r| r.id == 77 && r.kind() == STREAM_GONE));
    assert!(core.streams.contains_key(&77), "the entry stays");
    // a new request, 31 s later (ZOMBIE_WAIT is 30 s): the zombie expires
    // and the entry goes
    let mut again = Vec::new();
    frame::write_headers(&mut again, 3, &block, true, 16 * 1024);
    let (mut pos, mut seq) = (0, 50);
    let late = Instant::now() + Duration::from_secs(31);
    let c = core.conns.get_mut(idx).unwrap();
    let h2 = c.h2.as_mut().unwrap();
    h2.step(&again, &mut pos, &mut c.wbuf, &mut seq, b"", late);
    core.service(idx);
    assert!(!core.streams.contains_key(&77), "expired: the entry goes");
    // a started answer on a new connection goes with that connection
    let mut s2 = connect(port);
    s2.write_all(&req).unwrap();
    let id2 = collect(&mut core, &mut inb, 1).pop().unwrap().id;
    let mut out = Vec::new();
    ans(
        &mut out,
        id2,
        200,
        6,
        b"",
        &[&78u64.to_le_bytes()[..], b"hi"].concat(),
    );
    core.run(&out, &mut inb, 0).unwrap();
    assert!(core.streams.contains_key(&78));
    core.kill((id2 >> 32) as usize);
    assert!(!core.streams.contains_key(&78), "closed: the entry goes");
}

/// Writes `bytes` on a second thread, then closes the write side. A big
/// write can be more than the socket buffers hold (smaller on macOS under
/// load), and only `core.run` on the test thread reads them: on the test
/// thread it would block forever.
fn write_then_close(s: &StdStream, bytes: Vec<u8>) -> std::thread::JoinHandle<()> {
    let mut w = s.try_clone().unwrap();
    std::thread::spawn(move || {
        w.write_all(&bytes).unwrap();
        w.shutdown(std::net::Shutdown::Write).unwrap();
    })
}

// ---- TLS (HTTPS) ----

use crate::tls::tests::{CA, path};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};

type TlsStream<S = StdStream> = rustls::StreamOwned<rustls::ClientConnection, S>;

/// A test core with the certificate `cert1.pem`.
fn tls_core(lim: Limits) -> (Core, u16) {
    let (mut core, port) = new_core(lim);
    let certs = Certs::share(&path("cert1.pem"), &path("key1.pem"), lim.http2).unwrap();
    core.certs = Some(certs);
    (core, port)
}

fn serve_tls(lim: Limits) -> (u16, Arc<Mutex<Vec<Rec>>>) {
    let (core, port) = tls_core(lim);
    (port, run_app(core))
}

/// A client that trusts the test CA and offers `alpn`.
fn tls_client(alpn: &[&[u8]]) -> rustls::ClientConnection {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(CA).unwrap())
        .unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = alpn.iter().map(|a| a.to_vec()).collect();
    let name = ServerName::try_from("localhost").unwrap();
    rustls::ClientConnection::new(Arc::new(cfg), name).unwrap()
}

fn tls_connect(port: u16, alpn: &[&[u8]]) -> TlsStream {
    rustls::StreamOwned::new(tls_client(alpn), connect(port))
}

/// Sends `req` over TLS and reads until the server closes. Err when it
/// closes without `close_notify`.
fn tls_send(port: u16, alpn: &[&[u8]], req: &[u8]) -> (std::io::Result<String>, Option<Vec<u8>>) {
    let mut s = tls_connect(port, alpn);
    s.write_all(req).unwrap();
    let mut out = Vec::new();
    let r = s.read_to_end(&mut out);
    let alpn = s.conn.alpn_protocol().map(<[u8]>::to_vec);
    (r.map(|_| String::from_utf8_lossy(&out).into_owned()), alpn)
}

/// HTTP/1.1 with no ALPN and with ALPN `http/1.1`. The server ends with
/// `close_notify` (a clean `read_to_end`).
#[test]
fn tls_http1_with_and_without_alpn() {
    let (port, _) = serve_tls(limits());
    let req = b"GET /hi HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n";
    for (alpn, want) in [
        (&[][..], None),
        (&[&b"http/1.1"[..]][..], Some(b"http/1.1".to_vec())),
    ] {
        let (got, picked) = tls_send(port, alpn, req);
        let got = got.expect("close_notify");
        assert!(
            got.starts_with("HTTP/1.1 200") && got.ends_with("/hi"),
            "{got:?}"
        );
        assert_eq!(picked, want);
    }
}

/// ALPN `h2`: the connection speaks HTTP/2, the preface comes after the
/// handshake. A client that offers only `h2` to a port without HTTP/2
/// fails the handshake.
#[test]
fn tls_alpn_h2() {
    use crate::h2::frame;
    use crate::h2::hpack::encode_field;
    let (port, _) = serve_tls(limits());
    let mut s = tls_connect(port, &[b"h2", b"http/1.1"]);
    let mut req = PREFACE.to_vec();
    frame::write(&mut req, frame::SETTINGS, 0, 0, b"");
    let mut block = Vec::new();
    for (n, v) in [(":method", "GET"), (":scheme", "https"), (":path", "/h2")] {
        encode_field(n.as_bytes(), v.as_bytes(), &mut block);
    }
    frame::write_headers(&mut req, 1, &block, true, 16 * 1024);
    s.write_all(&req).unwrap();
    assert_eq!(s.conn.alpn_protocol(), Some(&b"h2"[..]));
    let (mut got, mut body, mut p) = (Vec::new(), Vec::new(), 0);
    'read: loop {
        let mut buf = [0u8; 4096];
        let k = s.read(&mut buf).unwrap();
        assert!(k > 0, "closed");
        got.extend_from_slice(&buf[..k]);
        while let Some(h) = frame::read_head(&got[p..]) {
            let end = p + frame::HEAD + h.len;
            if end > got.len() {
                break;
            }
            if h.kind == frame::DATA && h.id == 1 {
                body.extend_from_slice(&got[p + frame::HEAD..end]);
            }
            p = end;
            if h.id == 1 && h.flags & frame::END_STREAM != 0 {
                break 'read;
            }
        }
    }
    assert_eq!(body, b"/h2");

    let (port, _) = serve_tls(Limits {
        http2: false,
        ..limits()
    });
    let mut s = tls_connect(port, &[b"h2"]);
    let got = s.write_all(b"x").and_then(|_| s.read(&mut [0]));
    assert!(got.is_err(), "{got:?}");
}

/// Plain HTTP on a TLS port gets no HTTP answer: the connection closes.
#[test]
fn tls_port_refuses_plain_http() {
    let (port, _) = serve_tls(limits());
    let mut s = connect(port);
    s.write_all(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n").unwrap();
    let mut got = Vec::new();
    // an end or a reset, not the read timeout of `connect`
    if let Err(e) = s.read_to_end(&mut got) {
        assert!(
            !matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut),
            "not closed"
        );
    }
    assert!(!got.starts_with(b"HTTP"), "{got:?}");
}

/// Writes one byte at a time, with a short pause after each 8: a handshake
/// in many small pieces, and slow.
struct Trickle(StdStream, usize);

impl Read for Trickle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for Trickle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.1 += 1;
        if self.1.is_multiple_of(8) {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.0.write(&buf[..buf.len().min(1)])
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

#[test]
fn tls_handshake_in_pieces() {
    let (port, _) = serve_tls(limits());
    let mut s = rustls::StreamOwned::new(tls_client(&[]), Trickle(connect(port), 0));
    s.write_all(b"GET /slow HTTP/1.1\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).unwrap();
    let got = String::from_utf8_lossy(&out);
    assert!(
        got.starts_with("HTTP/1.1 200") && got.ends_with("/slow"),
        "{got:?}"
    );
}

/// A handshake that does not end in the idle timeout closes.
#[test]
fn tls_handshake_times_out() {
    let (mut core, port) = tls_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    // half a record head
    s.write_all(&[0x16, 0x03]).unwrap();
    accept(&mut core, &mut inb, 1);
    core.run(&[], &mut inb, 1000).unwrap();
    after(&mut core, &mut inb, IDLE / 2);
    assert_eq!(core.conns.len(), 1, "too early");
    after(&mut core, &mut inb, IDLE + Duration::from_secs(1));
    assert_eq!(core.conns.len(), 0, "not closed");
}

/// WebSocket over TLS: an echo, and 8 MiB from Dart to a client (more
/// than the rustls buffer, so the write waits for the socket).
#[test]
fn tls_websocket() {
    let (port, _) = serve_tls(limits());
    let (mut ws, res) = tungstenite::client("wss://localhost/ws", tls_connect(port, &[])).unwrap();
    assert_eq!(res.status(), 101);
    ws.send(Message::text("hi")).unwrap();
    assert_eq!(ws.read().unwrap(), Message::text("hi"));
    let (mut ws, _) = tungstenite::client("wss://localhost/big", tls_connect(port, &[])).unwrap();
    match ws.read().unwrap() {
        Message::Binary(b) => assert_eq!(b.len(), 8 << 20),
        m => panic!("{m:?}"),
    }
}

/// A streamed answer over TLS.
#[test]
fn tls_streamed_answer() {
    let (port, _) = serve_tls(limits());
    let (got, _) = tls_send(
        port,
        &[],
        b"GET /stream HTTP/1.1\r\nConnection: close\r\n\r\n",
    );
    let got = got.unwrap();
    assert!(got.contains("transfer-encoding: chunked"), "{got:?}");
    assert!(got.ends_with("2\r\nab\r\n3\r\ncde\r\n0\r\n\r\n"), "{got:?}");
}

/// The DER of the first certificate the server showed.
fn shown(s: &TlsStream) -> Vec<u8> {
    s.conn.peer_certificates().unwrap()[0].to_vec()
}

fn der(pem: &[u8]) -> Vec<u8> {
    CertificateDer::from_pem_slice(pem).unwrap().to_vec()
}

/// A reload gives the new certificate to new connections. An open one
/// keeps working. A bad file changes nothing.
#[test]
fn tls_reload() {
    let dir = std::env::temp_dir().join(format!("dox-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::copy(path("cert1.pem"), &cert).unwrap();
    std::fs::copy(path("key1.pem"), &key).unwrap();
    let (mut core, port) = new_core(limits());
    core.certs = Some(Certs::share(&cert, &key, true).unwrap());
    let _log = run_app(core);
    // a server with the same files and HTTP/2 off: the reload reaches it too
    let (mut core, port2) = new_core(Limits {
        http2: false,
        ..limits()
    });
    core.certs = Some(Certs::share(&cert, &key, false).unwrap());
    let _log2 = run_app(core);
    let get = b"GET /a HTTP/1.1\r\nHost: a\r\n\r\n";
    let answer = |s: &mut TlsStream| {
        s.write_all(get).unwrap();
        let mut buf = [0u8; 4096];
        let k = s.read(&mut buf).unwrap();
        assert!(buf[..k].starts_with(b"HTTP/1.1 200"));
    };
    let mut old = tls_connect(port, &[]);
    answer(&mut old);
    let cert1 = der(&std::fs::read(path("cert1.pem")).unwrap());
    let cert2 = der(&std::fs::read(path("cert2.pem")).unwrap());
    assert_eq!(shown(&old), cert1);

    // reload: what `dox_tls` does with the same files
    std::fs::copy(path("cert2.pem"), &cert).unwrap();
    std::fs::copy(path("key2.pem"), &key).unwrap();
    Certs::share(&cert, &key, true).unwrap();
    let mut new = tls_connect(port, &[]);
    answer(&mut new);
    assert_eq!(shown(&new), cert2);
    answer(&mut old);
    let mut other = tls_connect(port2, &[]);
    answer(&mut other);
    assert_eq!(shown(&other), cert2);

    // a bad file: an error, and the server keeps the certificate
    std::fs::write(&cert, b"junk").unwrap();
    assert_eq!(Certs::share(&cert, &key, true).err(), Some(tls::CERT_BAD));
    let mut after = tls_connect(port, &[]);
    answer(&mut after);
    assert_eq!(shown(&after), cert2);
    let _ = std::fs::remove_dir_all(dir);
}

/// A big answer with `Connection: close` to a client that reads slowly:
/// the socket is full when the answer is out, and `close_notify` must
/// still come before the close.
#[test]
fn tls_close_notify_after_a_big_answer_to_a_slow_reader() {
    let (port, _) = serve_tls(limits());
    let mut s = tls_connect(port, &[]);
    s.write_all(b"GET /large HTTP/1.1\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut out = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(k) => out.extend_from_slice(&buf[..k]),
            Err(e) => panic!("no close_notify: {e}"),
        }
        if out.len() % (1 << 20) < buf.len() {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    assert!(out.starts_with(b"HTTP/1.1 200"));
    assert!(out.ends_with(&[b'x'; 1024]) && out.len() > 8 << 20);
}

/// A client that reads slowly, but reads: the send stall timer must not
/// close it. `tls_write_counts_each_socket_byte` shows that the progress
/// comes per socket write, not per rustls buffer.
#[test]
fn tls_slow_reader_is_not_stalled() {
    let (mut core, port) = tls_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut out = Vec::new();
    let (go, wait) = mpsc::channel::<()>();
    let (stop, stopped) = mpsc::channel::<()>();
    let client = std::thread::spawn(move || {
        let mut s = tls_connect(port, &[]);
        s.write_all(b"GET /large HTTP/1.1\r\nHost: a\r\n\r\n")
            .unwrap();
        wait.recv().unwrap();
        // raw TLS bytes, a little at a time, until the test has seen it
        let mut buf = vec![0u8; 4 * 1024];
        while stopped.try_recv().is_err() {
            let _ = s.sock.read(&mut buf);
            std::thread::sleep(Duration::from_millis(2));
        }
        let _ = wait.recv();
    });
    let end = Instant::now() + Duration::from_secs(5);
    while !core.conns.iter().any(|(_, c)| c.stuck.is_some()) {
        turn(&mut core, &mut inb, &mut out, 1000);
        assert!(Instant::now() < end, "the answer never waits");
    }
    let idx = core.conns.iter().next().unwrap().0;
    let old = Instant::now() - Duration::from_secs(20);
    core.conns[idx].stuck = Some(old);
    go.send(()).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while core.conns[idx].stuck == Some(old) {
        turn(&mut core, &mut inb, &mut out, 1000);
        assert!(Instant::now() < end, "no progress seen");
    }
    stop.send(()).unwrap();
    // the old stall time is over SEND_WAIT ago 15 s from now, the new is not
    after(&mut core, &mut inb, Duration::from_secs(15));
    assert!(core.conns.contains(idx), "closed as stalled");
    drop(go);
    drop(core);
    let _ = client.join();
}

/// A socket with room for this many more bytes, then WouldBlock.
struct Room(usize);

impl Write for Room {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.0 == 0 {
            return Err(ErrorKind::WouldBlock.into());
        }
        let k = buf.len().min(self.0);
        self.0 -= k;
        Ok(k)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The handshake of `server` and `client` in memory.
fn handshake(server: &mut rustls::ServerConnection, client: &mut rustls::ClientConnection) {
    while server.is_handshaking() || client.is_handshaking() {
        let mut b = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut b).unwrap();
        }
        let mut r = &b[..];
        while !r.is_empty() {
            server.read_tls(&mut r).unwrap();
        }
        server.process_new_packets().unwrap();
        let mut b = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut b).unwrap();
        }
        let mut r = &b[..];
        while !r.is_empty() {
            client.read_tls(&mut r).unwrap();
        }
        client.process_new_packets().unwrap();
    }
}

/// A slow socket takes a part of one rustls buffer: no answer byte is
/// written yet (`held` stays), but `sent` grows with each socket write.
/// The stall and idle timers count `sent` (see `Conn::progress`).
#[test]
fn tls_write_counts_each_socket_byte() {
    let certs = Certs::share(&path("cert1.pem"), &path("key1.pem"), false).unwrap();
    let mut t = certs.accept().unwrap();
    let mut client = tls_client(&[]);
    handshake(&mut t.conn, &mut client);
    let sent = t.sent;
    let data = vec![b'x'; 1 << 20];
    assert_eq!(tls::write(&mut t, &mut Room(1000), &data), Some(0));
    assert_eq!(t.sent, sent + 1000);
    assert!(t.held > 0);
    assert_eq!(tls::write(&mut t, &mut Room(1000), &data), Some(0));
    assert_eq!(t.sent, sent + 2000);
    // a big room: the whole rustls buffer goes out
    let held = t.held;
    let done = tls::write(&mut t, &mut Room(usize::MAX), &data);
    assert_eq!(done, Some(data.len()));
    assert!(held > 0 && t.held == 0);
}

/// The answer is out (`pending()` is 0) while the socket is full and
/// rustls still holds records. The close must wait for them and for
/// `close_notify`: no `shutdown` yet, and the client gets a clean end.
#[test]
fn tls_close_waits_for_close_notify_on_a_full_socket() {
    let (mut core, port) = tls_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut out = Vec::new();
    let (go, wait) = mpsc::channel::<()>();
    let (got_tx, got) = mpsc::channel::<std::io::Result<usize>>();
    std::thread::spawn(move || {
        let mut s = tls_connect(port, &[]);
        s.write_all(b"GET /large HTTP/1.1\r\nHost: a\r\n\r\n")
            .unwrap();
        wait.recv().unwrap();
        let mut all = Vec::new();
        got_tx.send(s.read_to_end(&mut all)).unwrap();
    });
    let end = Instant::now() + Duration::from_secs(5);
    while !core.conns.iter().any(|(_, c)| c.stuck.is_some()) {
        turn(&mut core, &mut inb, &mut out, 1000);
        assert!(Instant::now() < end, "the answer never waits");
    }
    let idx = core.conns.iter().next().unwrap().0;
    let c = &mut core.conns[idx];
    // drop the rest of the answer: rustls keeps the records it has
    c.wpos = c.wbuf.len();
    c.tls.as_mut().unwrap().held = 0;
    c.shut = true;
    core.service(idx);
    let c = &core.conns[idx];
    assert!(c.linger.is_none(), "closed before close_notify was out");
    assert!(c.tls.as_ref().unwrap().conn.wants_write());
    go.send(()).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    let r = loop {
        turn(&mut core, &mut inb, &mut out, 1000);
        if let Ok(r) = got.try_recv() {
            break r;
        }
        assert!(Instant::now() < end, "no end");
    };
    r.expect("close_notify");
}

/// A TLS client sends a part of a request head, then KeyUpdate requests
/// with one more byte each. TLS records that are no answer go out (here
/// KeyUpdates of the server: rustls 0.23 holds its answer to the client's
/// request until the next answer bytes), but the idle time still counts
/// from the start, and the connection closes.
#[test]
fn tls_key_updates_do_not_hold_a_partial_head() {
    let (mut core, port) = tls_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut out = Vec::new();
    let (go, wait) = mpsc::channel::<()>();
    let (sent_tx, sent) = mpsc::channel::<()>();
    std::thread::spawn(move || {
        let mut s = tls_connect(port, &[]);
        // the target grows by one byte each time: still a partial head
        s.write_all(b"GET /a").unwrap();
        sent_tx.send(()).unwrap();
        while wait.recv().is_ok() {
            s.conn.refresh_traffic_keys().unwrap();
            s.write_all(b"a").unwrap();
            s.flush().unwrap();
            sent_tx.send(()).unwrap();
        }
    });
    let end = Instant::now() + Duration::from_secs(5);
    let mut head = false;
    while !head || core.conns.iter().all(|(_, c)| c.rlen == 0) {
        head |= sent.try_recv().is_ok();
        turn(&mut core, &mut inb, &mut out, 1000);
        assert!(Instant::now() < end, "no head");
    }
    let idx = core.conns.iter().next().unwrap().0;
    let since = core.conns[idx].since;
    for _ in 0..3 {
        go.send(()).unwrap();
        sent.recv().unwrap();
        // read it at a later time, when an activity would move `since`
        let rlen = core.conns[idx].rlen;
        let end = Instant::now() + Duration::from_secs(5);
        while core.conns[idx].rlen == rlen {
            core.now = Instant::now() + IDLE * 6 / 10;
            core.wait(Some(Duration::from_millis(10)));
            core.fill(&mut inb).unwrap();
            assert!(Instant::now() < end, "the byte did not come");
        }
        let before = core.conns[idx].tls.as_ref().unwrap().sent;
        let t = core.conns[idx].tls.as_mut().unwrap();
        t.conn.refresh_traffic_keys().unwrap();
        core.service(idx);
        assert!(core.conns[idx].tls.as_ref().unwrap().sent > before);
        assert_eq!(core.conns[idx].since, since);
    }
    after(&mut core, &mut inb, IDLE * 12 / 10);
    assert!(
        !core.conns.contains(idx),
        "the partial head holds the connection"
    );
}

/// Waits until the client thread sends its result, with server turns.
fn tls_wait<T>(core: &mut Core, inb: &mut [u8], got: &mpsc::Receiver<T>) -> T {
    let mut out = Vec::new();
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        turn(core, inb, &mut out, 1000);
        if let Ok(r) = got.try_recv() {
            return r;
        }
        assert!(Instant::now() < end, "no result");
    }
}

/// The close of a client that never reads: `close_notify` can not go out,
/// so the send stall timer ends the connection. No busy loop before it.
#[test]
fn tls_close_to_a_client_that_never_reads_ends_by_the_stall_timer() {
    let (mut core, port) = tls_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut out = Vec::new();
    let (done, hold) = mpsc::channel::<()>();
    std::thread::spawn(move || {
        // small socket buffers, so the records rustls holds do not fit
        let sock =
            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        sock.set_recv_buffer_size(4096).unwrap();
        sock.connect(&std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, port)).into())
            .unwrap();
        let mut s = rustls::StreamOwned::new(tls_client(&[]), StdStream::from(sock));
        s.write_all(b"GET /large HTTP/1.1\r\nHost: a\r\n\r\n")
            .unwrap();
        let _ = hold.recv();
    });
    let end = Instant::now() + Duration::from_secs(5);
    while !core.conns.iter().any(|(_, c)| c.stuck.is_some()) {
        for (_, c) in core.conns.iter() {
            let _ = socket2::SockRef::from(&c.sock).set_send_buffer_size(4096);
        }
        turn(&mut core, &mut inb, &mut out, 1000);
        assert!(Instant::now() < end, "the answer never waits");
    }
    let idx = core.conns.iter().next().unwrap().0;
    let c = &mut core.conns[idx];
    c.wpos = c.wbuf.len();
    c.tls.as_mut().unwrap().held = 0;
    c.shut = true;
    core.service(idx);
    for _ in 0..3 {
        turn(&mut core, &mut inb, &mut out, 1000);
        assert!(core.ready.is_empty(), "a busy loop");
    }
    let c = &core.conns[idx];
    assert!(c.linger.is_none() && c.tls.as_ref().unwrap().conn.wants_write());
    after(&mut core, &mut inb, SEND_WAIT / 2);
    assert!(core.conns.contains(idx), "too early");
    after(&mut core, &mut inb, SEND_WAIT + Duration::from_secs(1));
    assert!(!core.conns.contains(idx), "not closed");
    drop(done);
}

/// HTTP/2 over TLS: the idle GOAWAY close ends with `close_notify`. A
/// graceful stop of the server too, on HTTP/1.1.
#[test]
fn tls_goaway_and_drain_send_close_notify() {
    use crate::h2::frame;
    let (mut core, port) = tls_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let (got_tx, got) = mpsc::channel::<std::io::Result<Vec<u8>>>();
    let tx = got_tx.clone();
    std::thread::spawn(move || {
        let mut s = tls_connect(port, &[b"h2"]);
        let mut req = PREFACE.to_vec();
        frame::write(&mut req, frame::SETTINGS, 0, 0, b"");
        s.write_all(&req).unwrap();
        let mut all = Vec::new();
        tx.send(s.read_to_end(&mut all).map(|_| all)).unwrap();
    });
    let mut out = Vec::new();
    let end = Instant::now() + Duration::from_secs(5);
    while !core.conns.iter().any(|(_, c)| c.h2.is_some()) {
        turn(&mut core, &mut inb, &mut out, 1000);
        assert!(Instant::now() < end, "no HTTP/2");
    }
    after(&mut core, &mut inb, IDLE + Duration::from_secs(1));
    let all = tls_wait(&mut core, &mut inb, &got).expect("close_notify after GOAWAY");
    // the last frame is the GOAWAY
    assert!(all.len() >= frame::HEAD + 8);
    let goaway = &all[all.len() - frame::HEAD - 8..];
    assert_eq!(frame::read_head(goaway).unwrap().kind, frame::GOAWAY);

    std::thread::spawn(move || {
        let mut s = tls_connect(port, &[]);
        s.write_all(b"GET /a HTTP/1.1\r\nHost: a\r\n\r\n").unwrap();
        let mut all = Vec::new();
        got_tx.send(s.read_to_end(&mut all).map(|_| all)).unwrap();
    });
    let end = Instant::now() + Duration::from_secs(5);
    // the HTTP/2 connection may still be in its lingering close
    while core
        .conns
        .iter()
        .all(|(_, c)| c.written == 0 || c.linger.is_some())
    {
        turn(&mut core, &mut inb, &mut out, 1000);
        assert!(Instant::now() < end, "no answer");
    }
    core.start_drain();
    let all = tls_wait(&mut core, &mut inb, &got).expect("close_notify after the drain");
    assert!(all.ends_with(b"/a"));
}

/// A clean WebSocket close over TLS ends with `close_notify`.
#[test]
fn tls_websocket_close_sends_close_notify() {
    let (port, _) = serve_tls(limits());
    let (mut ws, _) = tungstenite::client("wss://localhost/ws", tls_connect(port, &[])).unwrap();
    ws.close(None).unwrap();
    loop {
        match ws.read() {
            Ok(_) => {}
            Err(WsError::ConnectionClosed) => break,
            Err(e) => panic!("{e}"),
        }
    }
    let mut b = [0u8; 1];
    assert_eq!(ws.get_mut().read(&mut b).expect("close_notify"), 0);
}

/// An idle keep-alive TLS connection closes with `close_notify`: the
/// client sees a clean end (a read gives 0), not a cut.
#[test]
fn tls_idle_keep_alive_close_sends_close_notify() {
    let (mut core, port) = tls_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut out = Vec::new();
    let (tx, got) = mpsc::channel::<std::io::Result<usize>>();
    std::thread::spawn(move || {
        let mut s = tls_connect(port, &[]);
        s.write_all(b"GET /a HTTP/1.1\r\nHost: a\r\n\r\n").unwrap();
        let mut buf = vec![0u8; 4096];
        let k = s.read(&mut buf).unwrap();
        assert!(buf[..k].ends_with(b"/a"));
        tx.send(s.read(&mut buf)).unwrap();
    });
    let end = Instant::now() + Duration::from_secs(5);
    while core.conns.iter().all(|(_, c)| c.written == 0) {
        turn(&mut core, &mut inb, &mut out, 1000);
        assert!(Instant::now() < end, "no answer");
    }
    after(&mut core, &mut inb, IDLE + Duration::from_secs(1));
    let r = tls_wait(&mut core, &mut inb, &got);
    assert_eq!(r.expect("close_notify"), 0);
}

/// A flood of TLS records with no plain text (KeyUpdates, with one byte
/// of the head after each 30, so rustls allows more) does not hold the
/// loop: one call reads at most `WS_READ` raw bytes of it, queues the
/// connection again, and another connection gets its answer meanwhile.
#[test]
fn tls_records_without_plain_text_do_not_hold_the_loop() {
    const CYCLES: usize = 1500;
    let (mut core, port) = tls_core(limits());
    // the whole flood fits in the socket at once
    let l = core.listener.as_ref().unwrap();
    socket2::SockRef::from(l)
        .set_recv_buffer_size(4 << 20)
        .unwrap();
    let mut inb = vec![0u8; 1 << 18];
    let mut out = Vec::new();
    let (flood_go, flood_wait) = mpsc::channel::<()>();
    let (other_go, other_wait) = mpsc::channel::<()>();
    let (ready_tx, ready) = mpsc::channel::<&str>();
    let tx = ready_tx.clone();
    let (len_tx, flood_len) = mpsc::channel::<usize>();
    std::thread::spawn(move || {
        let sock =
            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        sock.set_send_buffer_size(4 << 20).unwrap();
        sock.connect(&std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, port)).into())
            .unwrap();
        let mut s = rustls::StreamOwned::new(tls_client(&[]), StdStream::from(sock));
        s.write_all(b"GET /").unwrap();
        tx.send("flood").unwrap();
        flood_wait.recv().unwrap();
        let mut blob = Vec::new();
        for _ in 0..CYCLES {
            for _ in 0..30 {
                s.conn.refresh_traffic_keys().unwrap();
            }
            s.conn.writer().write_all(b"a").unwrap();
            while s.conn.wants_write() {
                s.conn.write_tls(&mut blob).unwrap();
            }
        }
        assert!(blob.len() > 4 * WS_READ);
        s.sock.write_all(&blob).unwrap();
        len_tx.send(blob.len()).unwrap();
        tx.send("flooded").unwrap();
        let _ = flood_wait.recv();
    });
    std::thread::spawn(move || {
        let mut s = tls_connect(port, &[]);
        s.conn.complete_io(&mut s.sock).unwrap();
        ready_tx.send("other").unwrap();
        other_wait.recv().unwrap();
        s.write_all(b"GET /other HTTP/1.1\r\nHost: a\r\n\r\n")
            .unwrap();
        ready_tx.send("asked").unwrap();
        let _ = other_wait.recv();
    });
    let mut seen = Vec::new();
    let mut step = |core: &mut Core, inb: &mut [u8], want: &str| {
        let end = Instant::now() + Duration::from_secs(5);
        while !seen.contains(&want.to_string()) {
            if let Ok(w) = ready.try_recv() {
                seen.push(w.to_string());
            }
            turn(core, inb, &mut out, 1000);
            assert!(Instant::now() < end, "no {want}");
        }
    };
    step(&mut core, &mut inb, "flood");
    step(&mut core, &mut inb, "other");
    // the flood connection: the one with a partial head
    let flood = core
        .conns
        .iter()
        .find(|(_, c)| c.rlen > 0)
        .map(|(i, _)| i)
        .unwrap();
    // both wait in their sockets, then one call
    let end = Instant::now() + Duration::from_secs(5);
    let mut got = String::new();
    flood_go.send(()).unwrap();
    while ready.recv_timeout(Duration::from_secs(5)).unwrap() != "flooded" {}
    // write_all only means the bytes left the client: macOS hands loopback
    // bytes over in parts. Wait until the whole flood is in the server's
    // socket, so the one call below meets all of it on every OS.
    let want = flood_len.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut peek = vec![0u8; want];
    while core.conns[flood].sock.peek(&mut peek).unwrap_or(0) < want {
        assert!(Instant::now() < end, "the flood did not arrive");
        std::thread::sleep(Duration::from_millis(1));
    }
    other_go.send(()).unwrap();
    while ready.recv_timeout(Duration::from_secs(5)).unwrap() != "asked" {}
    std::thread::sleep(Duration::from_millis(50));
    core.wait(Some(Duration::from_millis(100)));
    let n = core.fill(&mut inb).unwrap();
    for r in records(&inb[..n]) {
        got.push_str(&r.target);
    }
    assert_eq!(got, "/other", "the other request waited");
    let c = &core.conns[flood];
    assert!(c.rlen < 5 + CYCLES, "the whole flood in one call");
    assert!(core.ready.contains(&flood), "not queued again");
    // no lost wakeup: later calls read the rest without a new event
    while core.conns[flood].rlen < 5 + CYCLES {
        core.fill(&mut inb).unwrap();
        assert!(Instant::now() < end, "the rest is not read");
    }
    drop((flood_go, other_go));
}

/// The spare write buffers: `lend` gives one only to a connection with no
/// buffer, `give` takes back only an empty one, and the spares stay under
/// WPOOL_BYTES (a big buffer is freed, not kept).
#[test]
fn wpool_lends_and_takes_back_only_empty_buffers() {
    let mut pool = WPool::default();
    let mut wbuf = Vec::with_capacity(1024);
    wbuf.extend_from_slice(b"not written");
    let mut wpos = 3;
    pool.give(&mut wbuf, &mut wpos);
    assert_eq!(
        (pool.bufs.len(), wbuf.len(), wpos),
        (0, 11, 3),
        "bytes wait"
    );
    wpos = wbuf.len();
    pool.give(&mut wbuf, &mut wpos);
    assert_eq!((pool.bufs.len(), pool.bytes), (1, 1024));
    assert_eq!((wbuf.capacity(), wpos), (0, 0));
    let mut other = Vec::with_capacity(8);
    pool.lend(&mut other);
    assert_eq!(other.capacity(), 8, "a connection with a buffer keeps it");
    pool.lend(&mut wbuf);
    assert!(wbuf.capacity() >= 1024 && wbuf.is_empty());
    assert_eq!((pool.bufs.len(), pool.bytes), (0, 0));
    pool.keep(Vec::with_capacity(KEEP_WBUF + 1));
    assert!(pool.bufs.is_empty(), "a big buffer is kept");
    for _ in 0..WPOOL_BYTES / KEEP_WBUF + 4 {
        pool.keep(Vec::with_capacity(KEEP_WBUF));
    }
    assert!(pool.bytes <= WPOOL_BYTES, "{}", pool.bytes);
}

/// Writes use the core's spare buffers: once its answers are written, an
/// HTTP/2 or HTTP/1.1 keep-alive connection holds no write buffer, and
/// the buffer waits in the core for the next connection that writes.
#[test]
fn answered_connections_hold_no_write_buffer() {
    use crate::h2::frame;
    use crate::h2::hpack::encode_field;
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut out = Vec::new();
    let mut h2 = PREFACE.to_vec();
    frame::write(&mut h2, frame::SETTINGS, 0, 0, b"");
    let mut block = Vec::new();
    for (n, v) in [(":method", "GET"), (":scheme", "http"), (":path", "/h2")] {
        encode_field(n.as_bytes(), v.as_bytes(), &mut block);
    }
    frame::write_headers(&mut h2, 1, &block, true, 16 * 1024);
    let mut a = connect(port);
    let mut b = connect(port);
    accept(&mut core, &mut inb, 2);
    a.write_all(&h2).unwrap();
    b.write_all(b"GET /h1 HTTP/1.1\r\n\r\n").unwrap();
    // both answers out: "/h2" in a DATA frame, "/h1" as the body
    let (mut got_a, mut got_b) = (Vec::new(), Vec::new());
    a.set_nonblocking(true).unwrap();
    b.set_nonblocking(true).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while !(got_a.ends_with(b"/h2") && got_b.ends_with(b"/h1")) && Instant::now() < end {
        turn(&mut core, &mut inb, &mut out, 1000);
        let mut buf = [0u8; 4096];
        for (s, got) in [(&mut a, &mut got_a), (&mut b, &mut got_b)] {
            if let Ok(k) = s.read(&mut buf) {
                got.extend_from_slice(&buf[..k]);
            }
        }
    }
    assert!(got_a.ends_with(b"/h2"), "{got_a:?}");
    assert!(got_b.ends_with(b"/h1"), "{got_b:?}");
    for (_, c) in core.conns.iter() {
        assert_eq!(
            c.wbuf.capacity(),
            0,
            "an answered connection holds a buffer"
        );
    }
    assert!(!core.wpool.bufs.is_empty(), "no spare buffer");
}

#[test]
fn push_dec_is_the_same_as_format() {
    for n in [0u64, 1, 9, 10, 99, 100, 12345, u32::MAX as u64, u64::MAX] {
        let mut buf = b"x".to_vec();
        push_dec(&mut buf, n);
        assert_eq!(buf, format!("x{n}").into_bytes(), "{n}");
    }
}

/// Answers that come in order go straight into the write buffer; the
/// others wait in their slot. Mixed, in one batch and over two batches,
/// the client still gets every answer in the order of its requests.
#[test]
fn answers_in_and_out_of_order_stay_in_order() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let mut s = connect(port);
    let all: String = (0..10)
        .map(|i| format!("GET /{i} HTTP/1.1\r\n\r\n"))
        .collect();
    s.write_all(all.as_bytes()).unwrap();
    let reqs = collect(&mut core, &mut inb, 10);
    assert_eq!(reqs.len(), 10);
    // batch 1: 0 1 2 in order, then 5, then 3 (in order again), 4
    let mut out = Vec::new();
    for i in [0, 1, 2, 5, 3, 4] {
        ans(&mut out, reqs[i].id, 200, 0, b"", reqs[i].target.as_bytes());
    }
    core.run(&out, &mut inb, 0).unwrap();
    // batch 2: 7 waits for 6, then 6, 8, 9
    let mut out = Vec::new();
    for i in [7, 6, 8, 9] {
        ans(&mut out, reqs[i].id, 200, 0, b"", reqs[i].target.as_bytes());
    }
    core.run(&out, &mut inb, 0).unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 65536];
    while !String::from_utf8_lossy(&got).ends_with("/9") {
        core.run(&[], &mut inb, 0).unwrap();
        let k = s.read(&mut buf).unwrap();
        got.extend_from_slice(&buf[..k]);
    }
    let text = String::from_utf8_lossy(&got);
    let bodies: Vec<&str> = text
        .split("\r\n\r\n")
        .skip(1)
        .map(|b| &b[..b.find("HTTP").unwrap_or(b.len())])
        .collect();
    let want: Vec<String> = (0..10).map(|i| format!("/{i}")).collect();
    assert_eq!(bodies, want);
}
