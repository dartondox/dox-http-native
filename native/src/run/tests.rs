//! Tests of the server loop. They drive a `Core` the way Dart does: call
//! `run`, read the records, write answers. A small test app answers.
use super::*;
use std::net::{Ipv4Addr, TcpStream as StdStream};
use tungstenite::WebSocket;

fn limits() -> Limits {
    Limits {
        body: 10,
        multipart: 20,
        timeout: Duration::from_millis(50),
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
/// are echoed. `/stream` sends "ab", "cde" as a streamed body. `/zip`
/// gzips. Anything else: the target and the body.
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
            "/stream" => {
                ans(
                    out,
                    r.id,
                    200,
                    6,
                    b"",
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

/// A client that sends `bytes` of ping frames (125 bytes each) after the
/// upgrade. With `read` it reads and drops the pongs, else it never reads.
/// Returns the index of its connection.
fn ping_flood(core: &mut Core, inb: &mut [u8], port: u16, read: bool, bytes: usize) -> usize {
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
        let mut frame = vec![0x89, 0x80 | 125, 0, 0, 0, 0];
        frame.extend_from_slice(&[b'p'; 125]);
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
    let idx = ping_flood(&mut core, &mut inb, port, true, 64 << 20);
    std::thread::sleep(Duration::from_millis(200));
    let mut s = connect(port);
    s.write_all(b"GET /other HTTP/1.1\r\n\r\n").unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let n = core.run(&[], &mut inb, 0).unwrap();
    assert!(
        records(&inb[..n]).iter().any(|r| r.target == "/other"),
        "the other request"
    );
    assert!(
        core.ready.contains(&idx),
        "the flood waits for its next turn"
    );
}

/// A client sends pings and never reads the pongs: Rust stops reading its
/// frames, so the pongs do not fill the memory.
#[test]
fn websocket_pongs_for_a_client_that_does_not_read_are_capped() {
    let (mut core, port) = new_core(limits());
    let mut inb = vec![0u8; 1 << 18];
    let idx = ping_flood(&mut core, &mut inb, port, false, 8 << 20);
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
    let got = after(&mut core, &mut inb, SEND_WAIT + HEAD_WAIT);
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
