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
/// `/liar` and `/short` do the same with `content-length` 5, 3 and 9.
/// `/zip` gzips. Anything else: the target and the body.
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
            t @ ("/stream" | "/exact" | "/liar" | "/short") => {
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
                ans(out, r.id, 0, 8, b"", b"");
            }
            "/zip" => ans(
                out,
                r.id,
                200,
                0x100,
                b"content-encoding: gzip\r\n",
                &b"hello ".repeat(100),
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
    let (mut core, port) = new_core(lim);
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
    (port, log)
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
fn chunked_body_and_expect_continue() {
    let lim = Limits {
        body: 1 << 20,
        ..limits()
    };
    let (port, _) = serve(lim);
    let req = b"POST /c HTTP/1.1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n2;x=y\r\nde\r\n0\r\nTrailer: 1\r\n\r\n";
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
    let lim = limits_of(1, 1, i64::MAX, 0, i64::MAX, false, 250);
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
