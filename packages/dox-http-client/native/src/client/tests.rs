use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream as StdStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use super::*;

mod h2;

struct Answer {
    id: u64,
    kind: u8,
    status: u16,
    error: u16,
    lines: String,
    body: Vec<u8>,
}

fn record(id: u64, addr: SocketAddr, flags: u8, timeout_ms: u32, request: &[u8]) -> Vec<u8> {
    named(id, addr, "", flags, timeout_ms, request)
}

/// A request record with a server name (for TLS).
fn named(
    id: u64,
    addr: SocketAddr,
    name: &str,
    flags: u8,
    timeout_ms: u32,
    request: &[u8],
) -> Vec<u8> {
    let mut r = vec![0u8; REQ_HEAD];
    let len = REQ_HEAD + name.len() + request.len();
    r[0..4].copy_from_slice(&(len as u32).to_le_bytes());
    r[36..38].copy_from_slice(&(name.len() as u16).to_le_bytes());
    r[4..12].copy_from_slice(&id.to_le_bytes());
    r[13] = flags;
    r[14..16].copy_from_slice(&addr.port().to_le_bytes());
    match addr {
        SocketAddr::V4(a) => {
            r[12] = 4;
            r[16..20].copy_from_slice(&a.ip().octets());
        }
        SocketAddr::V6(a) => {
            r[12] = 6;
            r[16..32].copy_from_slice(&a.ip().octets());
        }
    }
    r[32..36].copy_from_slice(&timeout_ms.to_le_bytes());
    r.extend_from_slice(name.as_bytes());
    r.extend_from_slice(request);
    r
}

fn new_core(max_per_host: usize, max_body: usize) -> Core {
    Core::new(
        max_per_host,
        max_body,
        None,
        tls_config(&[], false).unwrap(),
    )
    .unwrap()
}

fn get(id: u64, addr: SocketAddr) -> Vec<u8> {
    record(id, addr, AGAIN, 5000, b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
}

fn answers(inb: &[u8]) -> Vec<Answer> {
    let mut list = Vec::new();
    let mut p = 0;
    while p < inb.len() {
        let len = rd32(inb, p);
        let rec = &inb[p..p + len];
        let lines = rd32(rec, 16);
        list.push(Answer {
            id: u64::from_le_bytes(rec[4..12].try_into().unwrap()),
            kind: rec[20],
            status: u16::from_le_bytes(rec[12..14].try_into().unwrap()),
            error: u16::from_le_bytes(rec[14..16].try_into().unwrap()),
            lines: String::from_utf8(rec[RES_HEAD..RES_HEAD + lines].to_vec()).unwrap(),
            body: rec[RES_HEAD + lines..].to_vec(),
        });
        p += len;
    }
    list
}

/// Sends `out` and calls `run` until `count` answers came.
fn run_until(core: &mut Core, out: &[u8], count: usize) -> Vec<Answer> {
    let mut inb = vec![0u8; 1 << 20];
    let mut list = Vec::new();
    let start = Instant::now();
    let mut out = out;
    while list.len() < count {
        assert!(start.elapsed() < Duration::from_secs(10), "no answer");
        let n = core.run(out, &mut inb).unwrap();
        out = &[];
        list.extend(answers(&inb[..n]));
        if n == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }
    list
}

/// Reads one request head from `s`. false: the client closed.
fn read_head(s: &mut impl Read) -> bool {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        match s.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            _ => return false,
        }
    }
    true
}

/// A server that gives each connection to `serve`. Returns its address
/// and the number of connections it got.
fn server(
    serve: impl Fn(StdStream, usize) + Send + Sync + 'static,
) -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let serve = Arc::new(serve);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            let n = seen.fetch_add(1, Ordering::SeqCst);
            let serve = serve.clone();
            thread::spawn(move || serve(stream, n));
        }
    });
    (addr, count)
}

/// Answers each request on a connection with `answer`.
fn keep_alive(answer: &'static [u8]) -> (SocketAddr, Arc<AtomicUsize>) {
    server(move |mut s, _| {
        while read_head(&mut s) {
            if s.write_all(answer).is_err() {
                return;
            }
        }
    })
}

const HELLO: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-A: 1\r\n\r\nhello";

fn head(text: &str) -> Result<Option<Head>, u16> {
    parse_head(text.as_bytes(), false, MAX_BODY)
}

#[test]
fn head_with_folded_values_and_a_reason() {
    let h = head("HTTP/1.1 404 Not Found\r\nX-A: one  \t \r\n \t two\r\n\tthree\r\nX-B: b\r\n\r\n")
        .unwrap()
        .unwrap();
    assert_eq!(h.lines, b":Not Found\nx-a:one two three\nx-b:b\n");
    // no reason phrase: no line for it
    let h = head("HTTP/1.1 200\r\nX-A: 1\r\n\r\n").unwrap().unwrap();
    assert_eq!(h.lines, b"x-a:1\n");
    // a length must be on one line
    assert!(head("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n 0\r\n\r\n").is_err());
}

#[test]
fn head_with_a_length() {
    let h = head("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Name: A b\r\n\r\nhello")
        .unwrap()
        .unwrap();
    assert_eq!(h.status, 200);
    assert!(h.keep);
    assert!(matches!(h.body, Body::Len(5)));
    assert_eq!(h.lines, b":OK\ncontent-length:5\nx-name:A b\n");
    assert_eq!(h.start, 51);
}

#[test]
fn head_not_whole() {
    assert!(head("HTTP/1.1 200 OK\r\nContent-Le").unwrap().is_none());
}

#[test]
fn head_drops_1xx_answers() {
    let h = head("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 201 Created\r\ncontent-length: 0\r\n\r\n")
        .unwrap()
        .unwrap();
    assert_eq!(h.status, 201);
    assert!(
        head("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 20")
            .unwrap()
            .is_none()
    );
}

#[test]
fn head_keep_alive_rules() {
    let close = head("HTTP/1.1 200 OK\r\nConnection: Close\r\ncontent-length: 0\r\n\r\n");
    assert!(!close.unwrap().unwrap().keep);
    let old = head("HTTP/1.0 200 OK\r\ncontent-length: 0\r\n\r\n");
    assert!(!old.unwrap().unwrap().keep);
    let old_keep = head("HTTP/1.0 200 OK\r\nConnection: keep-alive\r\ncontent-length: 0\r\n\r\n");
    assert!(old_keep.unwrap().unwrap().keep);
    // no length: the body ends with the connection
    let h = head("HTTP/1.1 200 OK\r\n\r\n").unwrap().unwrap();
    assert!(!h.keep);
    assert!(matches!(h.body, Body::Eof));
}

#[test]
fn head_body_rules() {
    let chunked = head("HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, Chunked\r\n\r\n");
    assert!(matches!(chunked.unwrap().unwrap().body, Body::Chunked(_)));
    // chunked is not the last coding: read to the end of the connection
    let other = head("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked, gzip\r\n\r\n");
    assert!(matches!(other.unwrap().unwrap().body, Body::Eof));
    // a length next to chunked: chunked wins and the connection closes
    let both = head("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n")
        .unwrap()
        .unwrap();
    assert!(matches!(both.body, Body::Chunked(_)));
    assert!(!both.keep);
    for status in [204, 304] {
        let h = head(&format!("HTTP/1.1 {status} X\r\nContent-Length: 9\r\n\r\n"));
        assert!(matches!(h.unwrap().unwrap().body, Body::Len(0)));
    }
    let h = parse_head(
        b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n",
        true,
        MAX_BODY,
    );
    assert!(matches!(h.unwrap().unwrap().body, Body::Len(0)));
}

#[test]
fn head_bad_answers() {
    let bad = |text: &str| head(text).err();
    assert_eq!(
        bad("HTTP/1.1 200 OK\r\nContent-Length: +5\r\n\r\n"),
        Some(E_BAD)
    );
    assert_eq!(
        bad("HTTP/1.1 200 OK\r\nContent-Length: 5, 5\r\n\r\n"),
        Some(E_BAD)
    );
    assert_eq!(
        bad("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n"),
        Some(E_BAD)
    );
    assert_eq!(bad("HTTP/1.1 101 Switching\r\n\r\n"), Some(E_BAD));
    assert_eq!(bad("nonsense\r\n\r\n"), Some(E_BAD));
    assert_eq!(
        bad("HTTP/1.1 200 OK\r\nContent-Length: 99999999999999999999999\r\n\r\n"),
        Some(E_BIG)
    );
    let big = parse_head(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n", false, 10);
    assert_eq!(big.err(), Some(E_BIG));
}

fn new_chunked() -> Chunked {
    Chunked {
        pos: 0,
        out: Vec::new(),
        state: ChunkState::Size,
    }
}

#[test]
fn chunked_body_in_any_split() {
    let wire = b"4\r\nWiki\r\n5;ext=1\r\npedia\r\n0\r\nTrailer: 1\r\n\r\nNEXT";
    // the bytes come one by one
    let mut ch = new_chunked();
    let mut end = None;
    for n in 1..=wire.len() {
        if let Some(e) = ch.step(&wire[..n], MAX_BODY).unwrap() {
            end = Some(e);
            break;
        }
    }
    assert_eq!(ch.out, b"Wikipedia");
    assert_eq!(end, Some(wire.len() - 4));
    // all at once
    let mut ch = new_chunked();
    assert_eq!(ch.step(wire, MAX_BODY).unwrap(), Some(wire.len() - 4));
    assert_eq!(ch.out, b"Wikipedia");
}

#[test]
fn chunked_bad_bodies() {
    assert_eq!(new_chunked().step(b"zz\r\n", MAX_BODY), Err(E_BAD));
    assert_eq!(new_chunked().step(b"\r\n", MAX_BODY), Err(E_BAD));
    assert_eq!(new_chunked().step(b"-1\r\n", MAX_BODY), Err(E_BAD));
    assert_eq!(new_chunked().step(b"2\r\nabXX", MAX_BODY), Err(E_BAD));
    assert_eq!(
        new_chunked().step(b"fffffffffffffffff\r\n", MAX_BODY),
        Err(E_BAD)
    );
    assert_eq!(
        new_chunked().step(&[b'1'; CHUNK_LINE + 1], MAX_BODY),
        Err(E_BAD)
    );
    // a lone LF or CR, or a control byte, in a size or trailer line
    for wire in [
        &b"3\nabc\r\n0\r\n\r\n"[..],
        b"3;a\nb\r\nabc\r\n0\r\n\r\n",
        b"3;a\rb\r\nabc\r\n0\r\n\r\n",
        b"3;a\x00\r\nabc\r\n0\r\n\r\n",
        b"3\r\nabc\r\n0\r\nX: a\n\r\n",
        b"3\r\nabc\r\n0\r\n\n",
    ] {
        assert_eq!(new_chunked().step(wire, MAX_BODY), Err(E_BAD), "{wire:?}");
    }
    // a tab is fine
    let ok = b"3;a=\tb\r\nabc\r\n0\r\nX:\t1\r\n\r\n";
    assert_eq!(new_chunked().step(ok, MAX_BODY), Ok(Some(ok.len())));
    // over the limit, in one chunk and in two
    assert_eq!(new_chunked().step(b"b\r\n", 10), Err(E_BIG));
    assert_eq!(new_chunked().step(b"6\r\nabcdef\r\n5\r\n", 10), Err(E_BIG));
    assert_eq!(
        new_chunked().step(b"ffffffffffffffff\r\n", MAX_BODY),
        Err(E_BIG)
    );
}

#[test]
fn requests_use_one_connection_again() {
    let (addr, count) = keep_alive(HELLO);
    let mut core = new_core(0, 0);
    for id in 1..=3 {
        let a = run_until(&mut core, &get(id, addr), 1);
        assert_eq!((a[0].id, a[0].status, a[0].error), (id, 200, 0));
        assert_eq!(a[0].body, b"hello");
        assert_eq!(a[0].lines, ":OK\ncontent-length:5\nx-a:1\n");
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(core.open, 0);
}

#[test]
fn many_requests_at_once() {
    let (addr, count) = keep_alive(HELLO);
    let mut core = new_core(0, 0);
    let mut out = Vec::new();
    for id in 1..=20 {
        out.extend(get(id, addr));
    }
    let mut a = run_until(&mut core, &out, 20);
    a.sort_by_key(|a| a.id);
    assert_eq!(
        a.iter().map(|a| a.id).collect::<Vec<_>>(),
        (1..=20).collect::<Vec<_>>()
    );
    assert!(a.iter().all(|a| a.status == 200 && a.body == b"hello"));
    assert_eq!(count.load(Ordering::SeqCst), 20);
}

#[test]
fn max_per_host_makes_requests_wait() {
    let (addr, count) = keep_alive(HELLO);
    let mut core = new_core(2, 0);
    let mut out = Vec::new();
    for id in 1..=9 {
        out.extend(get(id, addr));
    }
    let a = run_until(&mut core, &out, 9);
    assert!(a.iter().all(|a| a.status == 200));
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(core.hosts[&Key { addr, name: None }].count, 2);
    assert!(core.hosts[&Key { addr, name: None }].queue.is_empty());
}

#[test]
fn a_closed_kept_connection_sends_the_request_again() {
    // each connection answers one request and closes with no notice
    let (addr, count) = server(|mut s, _| {
        if read_head(&mut s) {
            let _ = s.write_all(HELLO);
        }
    });
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &get(1, addr), 1);
    assert_eq!(a[0].status, 200);
    // let the close of the server arrive, but do not look at the socket
    thread::sleep(Duration::from_millis(50));
    let a = run_until(&mut core, &get(2, addr), 1);
    assert_eq!((a[0].status, a[0].error), (200, 0));
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[test]
fn a_request_without_again_is_not_sent_twice() {
    // the first connection answers one request, then takes the next
    // request and closes with no answer
    let (addr, count) = server(|mut s, n| {
        if n == 0 && read_head(&mut s) {
            let _ = s.write_all(HELLO);
            read_head(&mut s);
        }
    });
    let mut core = new_core(0, 0);
    assert_eq!(run_until(&mut core, &get(1, addr), 1)[0].status, 200);
    let post = record(
        2,
        addr,
        0,
        5000,
        b"POST / HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\n\r\n",
    );
    let a = run_until(&mut core, &post, 1);
    assert_eq!((a[0].status, a[0].error), (0, E_CLOSED));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn a_port_with_no_server_fails_to_connect() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &get(1, addr), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_CONNECT));
    assert!(core.conns.is_empty());
    assert!(core.hosts.is_empty());
}

#[test]
fn a_slow_answer_gets_a_timeout() {
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        thread::sleep(Duration::from_secs(2));
    });
    let mut core = new_core(1, 0);
    let mut out = record(1, addr, AGAIN, 50, b"GET / HTTP/1.1\r\nhost: x\r\n\r\n");
    // the second request waits for the connection and also times out
    out.extend(record(
        2,
        addr,
        AGAIN,
        60,
        b"GET / HTTP/1.1\r\nhost: x\r\n\r\n",
    ));
    let start = Instant::now();
    let a = run_until(&mut core, &out, 2);
    assert!(a.iter().all(|a| (a.status, a.error) == (0, E_TIMEOUT)));
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(core.conns.is_empty());
    assert_eq!(core.due, None);
}

#[test]
fn a_chunked_answer_in_parts() {
    let (addr, count) = server(|mut s, _| {
        while read_head(&mut s) {
            let parts: [&[u8]; 4] = [
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nab",
                b"c\r\n",
                b"2\r\nde\r",
                b"\n0\r\n\r\n",
            ];
            for part in parts {
                let _ = s.write_all(part);
                thread::sleep(Duration::from_millis(5));
            }
        }
    });
    let mut core = new_core(0, 0);
    for id in 1..=2 {
        let a = run_until(&mut core, &get(id, addr), 1);
        assert_eq!(
            (a[0].status, a[0].body.as_slice()),
            (200, b"abcde".as_slice())
        );
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn a_body_that_ends_with_the_connection() {
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\n\r\nto the ");
        thread::sleep(Duration::from_millis(5));
        let _ = s.write_all(b"end");
    });
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &get(1, addr), 1);
    assert_eq!(
        (a[0].status, a[0].body.as_slice()),
        (200, b"to the end".as_slice())
    );
    assert!(core.conns.is_empty());
}

#[test]
fn a_cut_answer_fails() {
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort");
    });
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &get(1, addr), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_CLOSED));
}

#[test]
fn a_body_over_the_limit_fails() {
    let (addr, _) = keep_alive(HELLO);
    let mut core = new_core(0, 4);
    let a = run_until(&mut core, &get(1, addr), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_BIG));
    assert!(core.conns.is_empty());
}

#[test]
fn a_big_body_and_a_small_buffer() {
    let (addr, _) = server(|mut s, _| {
        while read_head(&mut s) {
            let body = vec![b'x'; 300_000];
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
            let _ = s.write_all(head.as_bytes());
            let _ = s.write_all(&body);
        }
    });
    let mut core = new_core(0, 0);
    let mut small = vec![0u8; 1024];
    let mut out = get(1, addr);
    let start = Instant::now();
    let need = loop {
        assert!(start.elapsed() < Duration::from_secs(10), "no answer");
        match core.run(&out, &mut small) {
            Err(size) => break size,
            Ok(n) => assert_eq!(n, 0),
        }
        out.clear();
        thread::sleep(Duration::from_millis(1));
    };
    let mut inb = vec![0u8; need];
    let n = core.run(&[], &mut inb).unwrap();
    let a = answers(&inb[..n]);
    assert_eq!(a[0].body.len(), 300_000);
    // the big read buffer is not kept
    assert!(core.conns.iter().all(|(_, c)| c.buf.len() <= KEEP_BUF));
    // the connection still works
    let a = run_until(&mut core, &get(2, addr), 1);
    assert_eq!(a[0].body.len(), 300_000);
}

#[test]
fn a_head_request_has_no_body() {
    let (addr, count) = keep_alive(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n");
    let mut core = new_core(0, 0);
    for id in 1..=2 {
        let req = record(
            id,
            addr,
            HEAD_ONLY | AGAIN,
            5000,
            b"HEAD / HTTP/1.1\r\nhost: x\r\n\r\n",
        );
        let a = run_until(&mut core, &req, 1);
        assert_eq!((a[0].status, a[0].body.len()), (200, 0));
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn an_ipv6_address() {
    let Ok(listener) = TcpListener::bind("[::1]:0") else {
        return; // no IPv6 on this machine
    };
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        read_head(&mut s);
        let _ = s.write_all(HELLO);
    });
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &get(1, addr), 1);
    assert_eq!(a[0].status, 200);
}

/// A TLS server with a new certificate for `names`. It answers each
/// request with `answer`. Returns its address, its certificate (PEM) and
/// the number of connections it got.
fn tls_server(names: &[&str], answer: Vec<u8>) -> (SocketAddr, String, Arc<AtomicUsize>) {
    let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
    let made = rcgen::generate_simple_self_signed(names).unwrap();
    let pem = made.cert.pem();
    let key = rustls::pki_types::PrivateKeyDer::try_from(made.signing_key.serialize_der()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![made.cert.der().clone()], key)
        .unwrap();
    let config = Arc::new(config);
    let (addr, count) = server(move |s, _| {
        let conn = rustls::ServerConnection::new(config.clone()).unwrap();
        let mut tls = rustls::StreamOwned::new(conn, s);
        while read_head(&mut tls) {
            if tls.write_all(&answer).is_err() {
                return;
            }
        }
    });
    (addr, pem, count)
}

fn tls_get(id: u64, addr: SocketAddr, name: &str) -> Vec<u8> {
    let request = b"GET / HTTP/1.1\r\nhost: x\r\n\r\n";
    named(id, addr, name, AGAIN | TLS, 5000, request)
}

fn tls_core(pem: &str) -> Core {
    Core::new(0, 0, None, tls_config(pem.as_bytes(), false).unwrap()).unwrap()
}

#[test]
fn https_requests_use_one_connection_again() {
    let (addr, pem, count) = tls_server(&["localhost"], HELLO.to_vec());
    let mut core = tls_core(&pem);
    for id in 1..=3 {
        let a = run_until(&mut core, &tls_get(id, addr, "localhost"), 1);
        assert_eq!((a[0].status, a[0].error), (200, 0));
        assert_eq!(a[0].body, b"hello");
        assert_eq!(a[0].lines, ":OK\ncontent-length:5\nx-a:1\n");
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    // the same address with no TLS is another connection
    assert_eq!(core.hosts.len(), 1);
    assert!(
        core.hosts
            .keys()
            .all(|k| k.name.as_deref() == Some("localhost"))
    );
}

#[test]
fn https_many_requests_and_a_big_body() {
    let body = vec![b'y'; 300_000];
    let mut answer =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    answer.extend_from_slice(&body);
    let (addr, pem, _) = tls_server(&["localhost"], answer);
    let mut core = tls_core(&pem);
    let mut out = Vec::new();
    for id in 1..=8 {
        out.extend(tls_get(id, addr, "localhost"));
    }
    let mut inb = vec![0u8; 4 << 20];
    let mut got = 0;
    let start = Instant::now();
    while got < 8 {
        assert!(start.elapsed() < Duration::from_secs(10), "no answer");
        let n = core.run(&out, &mut inb).unwrap();
        out.clear();
        for a in answers(&inb[..n]) {
            assert_eq!((a.status, a.body.len()), (200, 300_000));
            assert!(a.body.iter().all(|&b| b == b'y'));
            got += 1;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn https_a_big_request_body() {
    // the server reads the whole body and answers with its length
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let pem = made.cert.pem();
    let key = rustls::pki_types::PrivateKeyDer::try_from(made.signing_key.serialize_der()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![made.cert.der().clone()], key)
        .unwrap();
    let config = Arc::new(config);
    const SIZE: usize = 500_000;
    let (addr, _) = server(move |s, _| {
        let conn = rustls::ServerConnection::new(config.clone()).unwrap();
        let mut tls = rustls::StreamOwned::new(conn, s);
        if !read_head(&mut tls) {
            return;
        }
        let mut body = vec![0u8; SIZE];
        if tls.read_exact(&mut body).is_err() {
            return;
        }
        let sum: u64 = body.iter().map(|&b| u64::from(b)).sum();
        let text = sum.to_string();
        let answer = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{text}",
            text.len()
        );
        let _ = tls.write_all(answer.as_bytes());
    });
    let mut request =
        format!("POST / HTTP/1.1\r\nhost: x\r\ncontent-length: {SIZE}\r\n\r\n").into_bytes();
    request.extend(std::iter::repeat_n(3u8, SIZE));
    let mut core = tls_core(&pem);
    let a = run_until(
        &mut core,
        &named(1, addr, "localhost", TLS, 5000, &request),
        1,
    );
    assert_eq!((a[0].status, a[0].error), (200, 0));
    assert_eq!(a[0].body, (3 * SIZE).to_string().as_bytes());
}

#[test]
fn https_a_certificate_that_is_not_trusted_fails() {
    let (addr, _, _) = tls_server(&["localhost"], HELLO.to_vec());
    // no extra root: only the roots of Mozilla
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &tls_get(1, addr, "localhost"), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_TLS));
    assert!(core.conns.is_empty());
}

#[test]
fn https_a_certificate_for_another_name_fails() {
    let (addr, pem, _) = tls_server(&["localhost"], HELLO.to_vec());
    let mut core = tls_core(&pem);
    let a = run_until(&mut core, &tls_get(1, addr, "other.test"), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_TLS));
    // the right name still works
    let a = run_until(&mut core, &tls_get(2, addr, "localhost"), 1);
    assert_eq!(a[0].status, 200);
}

#[test]
fn https_to_a_server_with_no_tls_fails() {
    // answers with plain HTTP as soon as the client connects
    let (addr, _) = server(|mut s, _| {
        let _ = s.write_all(HELLO);
        thread::sleep(Duration::from_millis(200));
    });
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &tls_get(1, addr, "localhost"), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_TLS));
}

#[test]
fn https_a_bad_server_name_fails() {
    let (addr, pem, _) = tls_server(&["localhost"], HELLO.to_vec());
    let mut core = tls_core(&pem);
    let a = run_until(&mut core, &tls_get(1, addr, "not a name"), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_TLS));
    assert_eq!(core.open, 0);
}

#[test]
fn https_a_port_with_no_server_fails_to_connect() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &tls_get(1, addr, "localhost"), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_CONNECT));
}

#[test]
fn extra_root_certificates_must_be_pem() {
    assert!(tls_config(&[], false).is_some());
    assert!(tls_config(b"not a certificate", false).is_none());
    let broken = b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    assert!(tls_config(broken, false).is_none());
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    assert!(tls_config(made.cert.pem().as_bytes(), false).is_some());
}

#[test]
fn head_one_close_wins_over_keep_alive() {
    let h = head(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nConnection: keep-alive\r\ncontent-length: 0\r\n\r\n",
    );
    assert!(!h.unwrap().unwrap().keep);
    let h = head("HTTP/1.0 200 OK\r\nConnection: keep-alive, close\r\ncontent-length: 0\r\n\r\n");
    assert!(!h.unwrap().unwrap().keep);
}

#[test]
fn bytes_on_an_idle_connection_are_not_the_next_answer() {
    // after its answer, the first connection sends one more answer that
    // nobody asked for
    let (addr, count) = server(|mut s, n| {
        while read_head(&mut s) {
            let _ = s.write_all(HELLO);
            if n == 0 {
                thread::sleep(Duration::from_millis(10));
                let _ = s.write_all(b"HTTP/1.1 500 Stray\r\nContent-Length: 5\r\n\r\nstray");
            }
        }
    });
    let mut core = new_core(0, 0);
    assert_eq!(run_until(&mut core, &get(1, addr), 1)[0].status, 200);
    // the stray answer arrives, and no call looks at the socket
    thread::sleep(Duration::from_millis(60));
    let a = run_until(&mut core, &get(2, addr), 1);
    assert_eq!(
        (a[0].status, a[0].body.as_slice()),
        (200, b"hello".as_slice())
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[test]
fn a_kept_connection_that_closes_on_the_request_sends_it_again() {
    // the first connection answers one request, then reads the next one
    // and closes with no answer. Each later connection answers.
    let (addr, count) = server(|mut s, n| {
        if n == 0 {
            if read_head(&mut s) {
                let _ = s.write_all(HELLO);
                read_head(&mut s);
            }
            return;
        }
        while read_head(&mut s) {
            let _ = s.write_all(HELLO);
        }
    });
    let mut core = new_core(0, 0);
    assert_eq!(run_until(&mut core, &get(1, addr), 1)[0].status, 200);
    let a = run_until(&mut core, &get(2, addr), 1);
    assert_eq!((a[0].status, a[0].error), (200, 0));
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[test]
fn an_idle_connection_closes_after_the_idle_timeout() {
    let (addr, count) = keep_alive(HELLO);
    let idle = Some(Duration::from_millis(50));
    let mut core = Core::new(0, 0, idle, tls_config(&[], false).unwrap()).unwrap();
    let mut inb = vec![0u8; 4096];
    assert_eq!(run_until(&mut core, &get(1, addr), 1)[0].status, 200);
    // not idle for long enough: the connection stays
    thread::sleep(Duration::from_millis(10));
    core.run(&[], &mut inb).unwrap();
    assert_eq!(core.conns.len(), 1);
    thread::sleep(Duration::from_millis(120));
    core.run(&[], &mut inb).unwrap();
    assert!(core.conns.is_empty());
    assert!(core.hosts.is_empty());
    // the next request gets a new connection
    assert_eq!(run_until(&mut core, &get(2, addr), 1)[0].status, 200);
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[test]
fn a_failed_connect_leaves_no_entry() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut core = new_core(1, 0);
    let mut out = get(1, addr);
    out.extend(get(2, addr));
    let a = run_until(&mut core, &out, 2);
    assert!(a.iter().all(|a| (a.status, a.error) == (0, E_CONNECT)));
    assert!(core.hosts.is_empty());
    assert_eq!(core.open, 0);
    let a = run_until(&mut core, &tls_get(3, addr, "not a name"), 1);
    assert_eq!(a[0].error, E_TLS);
    assert!(core.hosts.is_empty());
}

#[test]
fn https_a_closed_kept_connection_is_not_used_again() {
    // each connection answers one request and closes
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let pem = made.cert.pem();
    let key = rustls::pki_types::PrivateKeyDer::try_from(made.signing_key.serialize_der()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![made.cert.der().clone()], key)
        .unwrap();
    let config = Arc::new(config);
    let (addr, count) = server(move |s, _| {
        let conn = rustls::ServerConnection::new(config.clone()).unwrap();
        let mut tls = rustls::StreamOwned::new(conn, s);
        if read_head(&mut tls) {
            let _ = tls.write_all(HELLO);
            tls.conn.send_close_notify();
            let _ = tls.flush();
        }
    });
    let mut core = tls_core(&pem);
    for id in 1..=3 {
        thread::sleep(Duration::from_millis(30));
        let a = run_until(&mut core, &tls_get(id, addr, "localhost"), 1);
        assert_eq!((a[0].status, a[0].error), (200, 0));
    }
    assert_eq!(count.load(Ordering::SeqCst), 3);
}

#[test]
fn https_a_whole_answer_before_a_failure_is_an_answer() {
    // the server answers, then sends bytes that are not TLS
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let pem = made.cert.pem();
    let key = rustls::pki_types::PrivateKeyDer::try_from(made.signing_key.serialize_der()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![made.cert.der().clone()], key)
        .unwrap();
    let config = Arc::new(config);
    let (addr, _) = server(move |s, _| {
        let conn = rustls::ServerConnection::new(config.clone()).unwrap();
        let mut tls = rustls::StreamOwned::new(conn, s);
        if read_head(&mut tls) {
            // the answer and the bad bytes go out in one write, so the
            // client reads them together
            let _ = tls.conn.writer().write_all(HELLO);
            let mut wire = Vec::new();
            let _ = tls.conn.write_tls(&mut wire);
            wire.extend_from_slice(&[0xff; 64]);
            let _ = tls.sock.write_all(&wire);
            thread::sleep(Duration::from_millis(300));
        }
    });
    let mut core = tls_core(&pem);
    let a = run_until(&mut core, &tls_get(1, addr, "localhost"), 1);
    assert_eq!(
        (a[0].status, a[0].body.as_slice()),
        (200, b"hello".as_slice())
    );
    assert!(core.conns.is_empty());
}

#[test]
fn an_answer_before_the_whole_request_closes_the_connection() {
    // the server answers when it has the head, and does not read the body
    let (addr, _) = server(|mut s, _| {
        if read_head(&mut s) {
            let _ = s.write_all(b"HTTP/1.1 401 No\r\nContent-Length: 0\r\n\r\n");
            thread::sleep(Duration::from_millis(500));
        }
    });
    const SIZE: usize = 32 << 20;
    let mut request =
        format!("POST / HTTP/1.1\r\nhost: x\r\ncontent-length: {SIZE}\r\n\r\n").into_bytes();
    request.extend(std::iter::repeat_n(7u8, SIZE));
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &record(1, addr, 0, 5000, &request), 1);
    assert_eq!((a[0].status, a[0].error), (401, 0));
    // the body did not go out whole: the connection is not kept
    assert!(core.conns.is_empty());
    assert!(core.hosts.is_empty());
}

#[test]
fn a_request_with_an_answer_head_is_not_sent_again() {
    // the first connection answers one request. For the next request it
    // sends the head of a chunked answer and closes.
    let (addr, count) = server(|mut s, n| {
        if n == 0 && read_head(&mut s) {
            let _ = s.write_all(HELLO);
            if read_head(&mut s) {
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
                thread::sleep(Duration::from_millis(30));
            }
        }
    });
    let mut core = new_core(0, 0);
    assert_eq!(run_until(&mut core, &get(1, addr), 1)[0].status, 200);
    let a = run_until(&mut core, &get(2, addr), 1);
    assert_eq!((a[0].status, a[0].error), (0, E_CLOSED));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn requests_that_time_out_together_open_no_new_connection() {
    let (addr, count) = server(|mut s, _| {
        read_head(&mut s);
        thread::sleep(Duration::from_secs(2));
    });
    let mut core = new_core(1, 0);
    let request = b"GET / HTTP/1.1\r\nhost: x\r\n\r\n";
    let mut out = record(1, addr, AGAIN, 50, request);
    out.extend(record(2, addr, AGAIN, 50, request));
    let a = run_until(&mut core, &out, 2);
    assert!(a.iter().all(|a| (a.status, a.error) == (0, E_TIMEOUT)));
    assert!(core.conns.is_empty());
    assert!(core.hosts.is_empty());
    thread::sleep(Duration::from_millis(30));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn a_body_over_the_limit_stops_the_read_early() {
    // 64 MB that end with the connection, and a limit of 1 MB
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\n\r\n");
        let part = vec![b'z'; 1 << 20];
        for _ in 0..64 {
            if s.write_all(&part).is_err() {
                return;
            }
        }
    });
    let mut core = new_core(0, 1 << 20);
    let mut inb = vec![0u8; 4096];
    let mut out = get(1, addr);
    let start = Instant::now();
    loop {
        assert!(start.elapsed() < Duration::from_secs(10), "no answer");
        let n = core.run(&out, &mut inb).unwrap();
        out.clear();
        // the read buffer never grows far over the limit
        assert!(core.conns.iter().all(|(_, c)| c.buf.len() <= 4 << 20));
        if n > 0 {
            let a = answers(&inb[..n]);
            assert_eq!((a[0].status, a[0].error), (0, E_BIG));
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(core.conns.is_empty());
}

#[test]
fn a_big_body_comes_whole_through_many_read_steps() {
    const SIZE: usize = 24 << 20;
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {SIZE}\r\n\r\n");
        let _ = s.write_all(head.as_bytes());
        let part: Vec<u8> = (0..(1usize << 20)).map(|i| (i % 251) as u8).collect();
        for _ in 0..SIZE >> 20 {
            if s.write_all(&part).is_err() {
                return;
            }
        }
    });
    let mut core = new_core(0, 0);
    let mut inb = vec![0u8; SIZE + 4096];
    let mut out = get(1, addr);
    let start = Instant::now();
    let n = loop {
        assert!(start.elapsed() < Duration::from_secs(20), "no answer");
        let n = core.run(&out, &mut inb).unwrap();
        out.clear();
        if n > 0 {
            break n;
        }
        // no sleep while a connection has more to read: the run must go
        // on from there with no new socket event
        if core.again.is_empty() {
            thread::sleep(Duration::from_millis(1));
        }
    };
    let a = answers(&inb[..n]);
    assert_eq!(a[0].body.len(), SIZE);
    assert!(
        a[0].body
            .iter()
            .enumerate()
            .all(|(i, &b)| b == ((i % (1 << 20)) % 251) as u8)
    );
}

// On Linux, an answer that is in the socket when a write fails can still
// be read. Other systems can drop it.
#[cfg(target_os = "linux")]
#[test]
fn an_answer_and_a_close_before_the_whole_request_is_an_answer() {
    let (addr, _) = server(|mut s, _| {
        if read_head(&mut s) {
            let _ = s.write_all(
                b"HTTP/1.1 413 Too Big\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
            // close the write side, read a part of the body, then close
            let _ = s.shutdown(std::net::Shutdown::Write);
            let mut some = vec![0u8; 1 << 20];
            let _ = s.read(&mut some);
            thread::sleep(Duration::from_millis(50));
        }
    });
    const SIZE: usize = 64 << 20;
    let mut request =
        format!("POST / HTTP/1.1\r\nhost: x\r\ncontent-length: {SIZE}\r\n\r\n").into_bytes();
    request.extend(std::iter::repeat_n(7u8, SIZE));
    let mut core = new_core(0, 0);
    let a = run_until(&mut core, &record(1, addr, 0, 5000, &request), 1);
    assert_eq!((a[0].status, a[0].error), (413, 0));
    assert!(core.conns.is_empty());
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}

/// "Hello Hello Hello Hello" as brotli (`brotli -c`).
const BROTLI: &[u8] = &[
    0x1f, 0x16, 0x00, 0xf8, 0x8d, 0xd4, 0x4e, 0xf5, 0xd4, 0xa2, 0x4a, 0x36, 0xa4, 0xb7, 0x02, 0x29,
    0x0b, 0x79, 0xe2, 0x00, 0x97, 0x01,
];

/// Sends one GET with `flags` to a server that gives `answer`.
fn one(flags: u8, max_body: usize, answer: Vec<u8>) -> Answer {
    let answer: &'static [u8] = answer.leak();
    let (addr, _) = keep_alive(answer);
    let mut core = new_core(0, max_body);
    let req = record(1, addr, flags, 5000, b"GET / HTTP/1.1\r\nhost: x\r\n\r\n");
    run_until(&mut core, &req, 1).remove(0)
}

fn coded(coding: &str, body: &[u8]) -> Vec<u8> {
    let mut a = format!(
        "HTTP/1.1 200 OK\r\nContent-Encoding: {coding}\r\nx-a: 1\r\ncontent-length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    a.extend_from_slice(body);
    a
}

#[test]
fn decode_reads_brotli_test_bytes() {
    assert_eq!(
        decode(Coding::Brotli, BROTLI, 100).unwrap(),
        b"Hello Hello Hello Hello"
    );
}

#[test]
fn a_gzip_body_is_decoded() {
    let text = b"hello hello hello hello".repeat(100);
    let a = one(DECODE, 0, coded("gzip", &gzip(&text)));
    assert_eq!(a.status, 200);
    assert_eq!(a.body, text);
    assert_eq!(a.lines, ":OK\nx-a:1\n");
    // two members are one body
    let mut two = gzip(b"one ");
    two.extend(gzip(b"two"));
    assert_eq!(one(DECODE, 0, coded("x-gzip", &two)).body, b"one two");
    // padding after the last member is dropped
    two.extend_from_slice(&[0, 0, 0, b'\n']);
    assert_eq!(one(DECODE, 0, coded("gzip", &two)).body, b"one two");
    // a second member that is cut is a bad body
    let mut cut = gzip(b"one ");
    cut.extend_from_slice(&gzip(b"two")[..12]);
    assert_eq!(one(DECODE, 0, coded("gzip", &cut)).error, E_BAD);
}

#[test]
fn a_brotli_body_is_decoded() {
    let a = one(DECODE, 0, coded("BR", BROTLI));
    assert_eq!(a.body, b"Hello Hello Hello Hello");
    assert_eq!(a.lines, ":OK\nx-a:1\n");
}

#[test]
fn a_chunked_gzip_body_is_decoded() {
    let z = gzip(b"chunked text");
    let (x, y) = z.split_at(5);
    let mut a =
        b"HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\ntransfer-encoding: chunked\r\n\r\n5\r\n"
            .to_vec();
    a.extend_from_slice(x);
    a.extend(format!("\r\n{:x}\r\n", y.len()).bytes());
    a.extend_from_slice(y);
    a.extend_from_slice(b"\r\n0\r\n\r\n");
    let got = one(DECODE, 0, a);
    assert_eq!(got.body, b"chunked text");
    assert_eq!(got.lines, ":OK\ntransfer-encoding:chunked\n");
}

#[test]
fn a_body_stays_coded_without_the_flag_or_with_other_codings() {
    let z = gzip(b"text");
    let a = one(0, 0, coded("gzip", &z));
    assert_eq!(a.body, z);
    assert!(a.lines.contains("content-encoding:gzip\n"));
    for coding in ["zstd", "gzip, br", "identity"] {
        let a = one(DECODE, 0, coded(coding, &z));
        assert_eq!(a.body, z, "{coding}");
        assert!(a.lines.contains("content-encoding"), "{coding}");
    }
    // an empty body is not gzip
    let a = one(DECODE, 0, coded("gzip", b""));
    assert_eq!((a.status, a.body.len()), (200, 0));
    assert!(a.lines.contains("content-encoding"));
}

#[test]
fn a_bad_coded_body_fails_and_the_next_request_works() {
    let mut bad = coded("gzip", b"this is not gzip");
    bad.extend(coded("gzip", &gzip(b"good")));
    let bad: &'static [u8] = bad.leak();
    // the server gives the 2 answers one after the other on one connection
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let half = coded("gzip", b"this is not gzip").len();
        assert!(read_head(&mut s));
        s.write_all(&bad[..half]).unwrap();
        assert!(read_head(&mut s));
        s.write_all(&bad[half..]).unwrap();
        thread::sleep(Duration::from_millis(200));
    });
    let mut core = new_core(0, 0);
    let req = |id| record(id, addr, DECODE, 5000, b"GET / HTTP/1.1\r\nhost: x\r\n\r\n");
    let a = run_until(&mut core, &req(1), 1).remove(0);
    assert_eq!((a.status, a.error), (0, E_BAD));
    let a = run_until(&mut core, &req(2), 1).remove(0);
    assert_eq!(a.body, b"good");
    // a redirect with a bad body keeps it: only its location is of use
    let moved = b"HTTP/1.1 302 Found\r\nlocation: /x\r\ncontent-encoding: gzip\r\ncontent-length: 5\r\n\r\nmoved";
    let a = one(DECODE, 0, moved.to_vec());
    assert_eq!((a.status, a.body.as_slice()), (302, &b"moved"[..]));
    assert!(a.lines.contains("content-encoding:gzip\n"));
    // a cut gzip body is bad too
    let z = gzip(&b"long text ".repeat(50));
    let a = one(DECODE, 0, coded("gzip", &z[..z.len() - 9]));
    assert_eq!((a.status, a.error), (0, E_BAD));
}

#[test]
fn the_body_limit_is_for_the_decoded_bytes() {
    // 512 kB of zeros is under 1 kB of gzip
    let z = gzip(&vec![0u8; 1 << 19]);
    assert!(z.len() < 4096);
    let a = one(DECODE, 4096, coded("gzip", &z));
    assert_eq!((a.status, a.error), (0, E_BIG));
    let a = one(DECODE, 1 << 19, coded("gzip", &z));
    assert_eq!(a.body.len(), 1 << 19);
    let a = one(DECODE, (1 << 19) - 1, coded("gzip", &z));
    assert_eq!((a.status, a.error), (0, E_BIG));
}

// ---- streamed bodies -----------------------------------------------------

fn q_rec(id: u64, kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut r = vec![0u8; REQ_HEAD];
    r[0..4].copy_from_slice(&((REQ_HEAD + payload.len()) as u32).to_le_bytes());
    r[4..12].copy_from_slice(&id.to_le_bytes());
    r[38] = kind;
    r.extend_from_slice(payload);
    r
}

fn q_part(id: u64, data: &[u8]) -> Vec<u8> {
    q_rec(id, Q_PART, data)
}

fn q_end(id: u64, aborted: bool) -> Vec<u8> {
    let mut r = q_rec(id, Q_END, &[]);
    r[16] = aborted as u8;
    r
}

fn q_credit(id: u64, total: u64) -> Vec<u8> {
    let mut r = q_rec(id, Q_CREDIT, &[]);
    r[16..24].copy_from_slice(&total.to_le_bytes());
    r
}

fn q_cancel(id: u64) -> Vec<u8> {
    q_rec(id, Q_CANCEL, &[])
}

fn sget(id: u64, addr: SocketAddr, flags: u8, timeout_ms: u32) -> Vec<u8> {
    let request = b"GET / HTTP/1.1\r\nhost: x\r\n\r\n";
    record(id, addr, flags | STREAM, timeout_ms, request)
}

/// The Dart side of one streamed request, for the tests.
struct Reader {
    id: u64,
    inb: Vec<u8>,
    status: Option<u16>,
    lines: String,
    body: Vec<u8>,
    ended: bool,
    error: Option<u16>,
    wrote: u64,
    /// the bytes of the body that the test gave back as credit
    used: u64,
}

impl Reader {
    fn new(id: u64) -> Reader {
        Reader {
            id,
            inb: vec![0u8; 1 << 18],
            status: None,
            lines: String::new(),
            body: Vec::new(),
            ended: false,
            error: None,
            wrote: 0,
            used: 0,
        }
    }

    /// One call of `run`. Returns the bytes it wrote.
    fn run(&mut self, core: &mut Core, out: &[u8]) -> usize {
        let n = core.run(out, &mut self.inb).unwrap();
        for a in answers(&self.inb[..n]) {
            if a.id != self.id {
                continue;
            }
            match a.kind {
                R_HEAD => {
                    self.status = Some(a.status);
                    self.lines = a.lines;
                }
                R_PART => self.body.extend_from_slice(&a.body),
                R_END => self.ended = true,
                R_PROGRESS => self.wrote = u64::from_le_bytes(a.body[..8].try_into().unwrap()),
                _ if a.status == 0 => self.error = Some(a.error),
                _ => panic!("a whole answer for a streamed request"),
            }
        }
        n
    }

    fn over(&self) -> bool {
        self.ended || self.error.is_some()
    }

    /// Runs until `done`. `credit`: the test uses each byte at once.
    fn until(
        &mut self,
        core: &mut Core,
        first: &[u8],
        credit: bool,
        done: impl Fn(&Reader) -> bool,
    ) {
        let start = Instant::now();
        let mut out = first.to_vec();
        while !done(self) {
            assert!(
                start.elapsed() < Duration::from_secs(20),
                "stuck: {:?} {:?} {}",
                self.status,
                self.error,
                self.body.len()
            );
            if credit && self.used < self.body.len() as u64 {
                self.used = self.body.len() as u64;
                out.extend(q_credit(self.id, self.used));
            }
            let n = self.run(core, &out);
            out.clear();
            if n == 0 && core.again.is_empty() {
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

/// A server that answers a GET with `head` and then `size` bytes of
/// [pattern], as fast as the client takes them.
fn big_server(head: String, size: usize) -> SocketAddr {
    let (addr, _) = server(move |mut s, _| {
        while read_head(&mut s) {
            if s.write_all(head.as_bytes()).is_err() {
                return;
            }
            let mut at = 0;
            while at < size {
                let n = (size - at).min(1 << 16);
                let part: Vec<u8> = (at..at + n).map(|i| (i % 251) as u8).collect();
                if s.write_all(&part).is_err() {
                    return;
                }
                at += n;
            }
        }
    });
    addr
}

/// Runs a reader that uses no byte until its body is at the bound, and
/// then reads on. Rust has at most [HIGH] bytes, and the socket has the
/// rest.
fn paused_then_read(core: &mut Core, first: Vec<u8>, size: usize) -> Reader {
    let mut r = Reader::new(1);
    let mut out = first;
    let start = Instant::now();
    let (mut same, mut last) = (0, 0);
    while same < 60 {
        assert!(start.elapsed() < Duration::from_secs(10), "no body");
        let n = r.run(core, &out);
        out.clear();
        let held: usize = core.conns.iter().map(|(_, c)| c.filled).sum();
        let queued: usize = core.ready.iter().map(|d| d.body.len()).sum();
        assert!(
            r.body.len() + held + queued <= HIGH,
            "{} {held}",
            r.body.len()
        );
        assert!(core.conns.iter().all(|(_, c)| c.buf.len() <= 1 << 20));
        same = if !r.body.is_empty() && r.body.len() == last {
            same + 1
        } else {
            0
        };
        last = r.body.len();
        if n == 0 {
            thread::sleep(Duration::from_millis(2));
        }
    }
    assert!(r.body.len() > HIGH - PART_MAX, "{}", r.body.len());
    assert!(!r.over());
    // now the reader uses the bytes
    r.until(core, &[], true, |r| r.over());
    assert!(r.ended, "{:?}", r.error);
    assert_eq!(r.body.len(), size);
    assert!(r.body == pattern(size));
    r
}

#[test]
fn a_paused_reader_leaves_the_socket_alone() {
    const SIZE: usize = 8 << 20;
    let addr = big_server(
        format!("HTTP/1.1 200 OK\r\nContent-Length: {SIZE}\r\n\r\n"),
        SIZE,
    );
    let mut core = new_core(0, 0);
    let r = paused_then_read(&mut core, sget(1, addr, AGAIN, 5000), SIZE);
    assert_eq!(r.status, Some(200));
    assert_eq!(r.lines, format!(":OK\ncontent-length:{SIZE}\n"));
    // the connection is kept
    assert_eq!(core.open, 0);
    assert_eq!(core.conns.len(), 1);
    assert!(core.conns.iter().all(|(_, c)| c.req.is_none()));
}

#[test]
fn a_paused_reader_of_a_chunked_answer_leaves_the_socket_alone() {
    const SIZE: usize = 4 << 20;
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
        let mut at = 0;
        while at < SIZE {
            let n = (SIZE - at).min(50_000);
            let part: Vec<u8> = (at..at + n).map(|i| (i % 251) as u8).collect();
            let mut chunk = format!("{n:x}\r\n").into_bytes();
            chunk.extend_from_slice(&part);
            chunk.extend_from_slice(b"\r\n");
            if s.write_all(&chunk).is_err() {
                return;
            }
            at += n;
        }
        let _ = s.write_all(b"0\r\n\r\n");
        thread::sleep(Duration::from_millis(200));
    });
    let mut core = new_core(0, 0);
    paused_then_read(&mut core, sget(1, addr, 0, 5000), SIZE);
}

#[test]
fn a_paused_reader_of_a_body_with_no_length_leaves_the_socket_alone() {
    const SIZE: usize = 4 << 20;
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n");
        for at in (0..SIZE).step_by(1 << 16) {
            let part: Vec<u8> = (at..at + (1 << 16)).map(|i| (i % 251) as u8).collect();
            if s.write_all(&part).is_err() {
                return;
            }
        }
    });
    let mut core = new_core(0, 0);
    paused_then_read(&mut core, sget(1, addr, 0, 5000), SIZE);
    // the body ended with the connection
    assert!(core.conns.is_empty());
}

#[test]
fn a_paused_reader_of_a_tls_answer_leaves_the_socket_alone() {
    const SIZE: usize = 3 << 20;
    let mut answer = format!("HTTP/1.1 200 OK\r\nContent-Length: {SIZE}\r\n\r\n").into_bytes();
    answer.extend(pattern(SIZE));
    let (addr, pem, _) = tls_server(&["localhost"], answer);
    let mut core = tls_core(&pem);
    let request = b"GET / HTTP/1.1\r\nhost: x\r\n\r\n";
    let out = named(1, addr, "localhost", TLS | STREAM, 5000, request);
    paused_then_read(&mut core, out, SIZE);
}

#[test]
fn streamed_answers_of_each_kind() {
    // a chunked answer in parts
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let parts: [&[u8]; 4] = [
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nab",
            b"c\r\n",
            b"2\r\nde\r",
            b"\n0\r\n\r\n",
        ];
        for part in parts {
            let _ = s.write_all(part);
            thread::sleep(Duration::from_millis(5));
        }
    });
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, AGAIN, 5000), true, |r| r.over());
    assert_eq!((r.status, r.body.as_slice()), (Some(200), &b"abcde"[..]));
    assert_eq!(r.lines, ":OK\ntransfer-encoding:chunked\n");
    // content-length answers use one connection again
    let (addr, count) = keep_alive(HELLO);
    for id in 2..=3 {
        let mut r = Reader::new(id);
        r.until(&mut core, &sget(id, addr, AGAIN, 5000), true, |r| r.over());
        assert_eq!((r.status, r.body.as_slice()), (Some(200), &b"hello"[..]));
        assert_eq!(r.lines, ":OK\ncontent-length:5\nx-a:1\n");
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    // a body that ends with the connection
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\n\r\nto the ");
        thread::sleep(Duration::from_millis(5));
        let _ = s.write_all(b"end");
    });
    let mut r = Reader::new(4);
    r.until(&mut core, &sget(4, addr, 0, 5000), true, |r| r.over());
    assert_eq!(r.body, b"to the end");
    assert!(r.ended);
    // an empty body, and a HEAD answer
    let (addr, _) = keep_alive(b"HTTP/1.1 204 No Content\r\n\r\n");
    let mut r = Reader::new(5);
    r.until(&mut core, &sget(5, addr, 0, 5000), true, |r| r.over());
    assert_eq!((r.status, r.body.len(), r.ended), (Some(204), 0, true));
    let (addr, _) = keep_alive(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n");
    let mut r = Reader::new(6);
    let head = sget(6, addr, HEAD_ONLY, 5000);
    r.until(&mut core, &head, true, |r| r.over());
    assert_eq!((r.status, r.body.len(), r.ended), (Some(200), 0, true));
}

#[test]
fn streamed_answers_that_fail() {
    // over the limit: the head comes, then the failure
    let (addr, _) = keep_alive(HELLO);
    let mut core = new_core(0, 4);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, 0, 5000), true, |r| r.over());
    assert_eq!((r.status, r.error), (Some(200), Some(E_BIG)));
    assert!(r.body.len() <= 4);
    assert!(core.conns.is_empty());
    assert_eq!(core.open, 0);
    // a bad chunk
    let (addr, _) = keep_alive(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabcXX");
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, 0, 5000), true, |r| r.over());
    assert_eq!((r.status, r.error), (Some(200), Some(E_BAD)));
    assert!(core.conns.is_empty());
    // a cut body
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort");
    });
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, 0, 5000), true, |r| r.over());
    assert_eq!((r.status, r.error), (Some(200), Some(E_CLOSED)));
    assert_eq!(r.body, b"short");
    assert_eq!(core.open, 0);
    // no head at all
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"nonsense\r\n\r\n");
    });
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, 0, 5000), true, |r| r.over());
    assert_eq!((r.status, r.error), (None, Some(E_BAD)));
}

#[test]
fn a_streamed_gzip_answer_is_decoded_in_parts() {
    let text = pattern(300_000);
    let mut z = gzip(&text);
    z.extend(gzip(b" and more"));
    // padding after the last member is dropped
    z.extend_from_slice(&[0, 0, 0, b'\n']);
    let mut expect = text.clone();
    expect.extend_from_slice(b" and more");
    let (addr, _) = server(move |mut s, _| {
        read_head(&mut s);
        let head = b"HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\nx-a: 1\r\ntransfer-encoding: chunked\r\n\r\n";
        let _ = s.write_all(head);
        // the coded bytes come in small chunks
        for piece in z.chunks(7) {
            let mut chunk = format!("{:x}\r\n", piece.len()).into_bytes();
            chunk.extend_from_slice(piece);
            chunk.extend_from_slice(b"\r\n");
            if s.write_all(&chunk).is_err() {
                return;
            }
        }
        let _ = s.write_all(b"0\r\n\r\n");
        thread::sleep(Duration::from_millis(100));
    });
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, DECODE, 5000), true, |r| r.over());
    assert!(r.ended, "{:?} {} {}", r.error, r.body.len(), expect.len());
    assert_eq!(r.lines, ":OK\nx-a:1\ntransfer-encoding:chunked\n");
    assert!(r.body == expect);
}

#[test]
fn the_decoder_keeps_no_coded_bytes_that_it_took() {
    // text that does not pack well, so the coded body is big too
    let mut seed = 1u32;
    let text: Vec<u8> = (0..4 << 20)
        .map(|_| {
            seed = seed.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            b'a' + ((seed >> 16) % 16) as u8
        })
        .collect();
    let z = gzip(&text);
    assert!(z.len() > 1 << 20);
    let head = format!(
        "HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\n\r\n",
        z.len()
    );
    let (addr, _) = server(move |mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(head.as_bytes());
        let _ = s.write_all(&z);
        thread::sleep(Duration::from_millis(200));
    });
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    let mut most = 0;
    let mut out = sget(1, addr, DECODE, 5000);
    let start = Instant::now();
    while !r.over() {
        assert!(start.elapsed() < Duration::from_secs(20), "stuck");
        if r.used < r.body.len() as u64 {
            r.used = r.body.len() as u64;
            out.extend(q_credit(1, r.used));
        }
        let n = r.run(&mut core, &out);
        out.clear();
        for (_, c) in core.conns.iter_mut() {
            if let Some(inflate) = c.down.as_mut().and_then(|d| d.inflate.as_mut()) {
                most = most.max(inflate.feed_mut().buf.len());
            }
        }
        if n == 0 && core.again.is_empty() {
            thread::sleep(Duration::from_millis(1));
        }
    }
    assert!(r.ended && r.body == text);
    assert!(most <= HIGH + READ_MAX, "{most}");
}

/// A streamed body has no 4 GiB limit: only a whole body must fit in one
/// record.
#[test]
fn a_streamed_body_can_be_over_4_gib() {
    let head = "HTTP/1.1 200 OK\r\ncontent-length: 5000000000\r\n\r\n";
    assert!(matches!(
        parse_head(head.as_bytes(), false, MAX_BODY),
        Err(E_BIG)
    ));
    let (addr, _) = server(move |mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(head.as_bytes());
        let _ = s.write_all(&[7u8; 1000]);
        thread::sleep(Duration::from_millis(300));
    });
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, 0, 5000), true, |r| {
        r.body.len() >= 1000 || r.error.is_some()
    });
    assert_eq!((r.status, r.error), (Some(200), None));
    assert!(r.lines.contains("content-length:5000000000"));
    r.run(&mut core, &q_cancel(1));
    // with a limit from Dart, the body fails when it is over it
    let mut core = new_core(0, 500);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, 0, 5000), true, |r| r.over());
    assert_eq!(r.error, Some(E_BIG));
}

#[test]
fn a_streamed_brotli_answer_is_decoded() {
    let mut a = format!(
        "HTTP/1.1 200 OK\r\nContent-Encoding: br\r\nContent-Length: {}\r\n\r\n",
        BROTLI.len()
    )
    .into_bytes();
    a.extend_from_slice(BROTLI);
    // one piece, and one byte at a time
    let whole: &'static [u8] = a.leak();
    let (addr, _) = keep_alive(whole);
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, DECODE, 5000), true, |r| r.over());
    assert_eq!(r.body, b"Hello Hello Hello Hello");
    assert_eq!(r.lines, ":OK\n");
    let (addr, _) = server(move |mut s, _| {
        read_head(&mut s);
        for byte in whole {
            if s.write_all(&[*byte]).is_err() {
                return;
            }
            thread::sleep(Duration::from_micros(300));
        }
        thread::sleep(Duration::from_millis(100));
    });
    let mut r = Reader::new(2);
    r.until(&mut core, &sget(2, addr, DECODE, 5000), true, |r| r.over());
    assert_eq!(r.body, b"Hello Hello Hello Hello");
    // a bad brotli body is an error after the head
    let bad = coded("br", b"this is not brotli, no");
    let (addr, _) = keep_alive(bad.leak());
    let mut r = Reader::new(3);
    r.until(&mut core, &sget(3, addr, DECODE, 5000), true, |r| r.over());
    assert_eq!((r.status, r.error), (Some(200), Some(E_BAD)));
}

#[test]
fn a_streamed_decoded_body_has_a_limit_and_a_bounded_memory() {
    // 64 MB of zeros are under 100 kB of gzip
    let z = gzip(&vec![0u8; 64 << 20]);
    assert!(z.len() < 100_000);
    let answer: &'static [u8] = coded("gzip", &z).leak();
    let (addr, _) = keep_alive(answer);
    // with a limit of 1 MB the body fails at 1 MB
    let mut core = new_core(0, 1 << 20);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, DECODE, 5000), true, |r| r.over());
    assert_eq!((r.status, r.error), (Some(200), Some(E_BIG)));
    assert!(r.body.len() <= 1 << 20);
    assert!(core.conns.is_empty());
    // with no limit and a reader that does not use bytes, Rust holds a
    // bounded part of the 64 MB
    let mut core = new_core(0, 0);
    let mut r = Reader::new(2);
    let mut out = sget(2, addr, DECODE, 5000);
    for _ in 0..200 {
        r.run(&mut core, &out);
        out.clear();
        assert!(r.body.len() <= HIGH);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(r.body.len() > HIGH - PART_MAX);
    r.until(&mut core, &[], true, |r| r.over());
    assert!(r.ended, "{:?}", r.error);
    assert_eq!(r.body.len(), 64 << 20);
    assert!(r.body.iter().all(|&b| b == 0));
}

#[test]
fn a_streamed_bad_body_of_a_redirect_ends_quietly() {
    let moved = b"HTTP/1.1 302 Found\r\nlocation: /x\r\ncontent-encoding: gzip\r\ncontent-length: 5\r\n\r\nmoved";
    let (addr, _) = keep_alive(moved);
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, DECODE, 5000), true, |r| r.over());
    assert_eq!((r.status, r.ended, r.error), (Some(302), true, None));
    // the connection with a bad body is not kept
    assert!(core.conns.is_empty());
}

#[test]
fn cancel_closes_the_connection() {
    let closed = Arc::new(AtomicUsize::new(0));
    let seen = closed.clone();
    let (addr, _) = server(move |mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 99999999\r\n\r\n");
        let part = vec![1u8; 1 << 16];
        while s.write_all(&part).is_ok() {}
        seen.fetch_add(1, Ordering::SeqCst);
    });
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, 0, 5000), false, |r| {
        r.body.len() > 1000
    });
    assert_eq!(core.open, 1);
    r.run(&mut core, &q_cancel(1));
    assert_eq!(core.open, 0);
    assert!(core.conns.is_empty());
    assert!(core.hosts.is_empty());
    let start = Instant::now();
    while closed.load(Ordering::SeqCst) == 0 {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "server not closed"
        );
        thread::sleep(Duration::from_millis(5));
    }
    // a cancel for an unknown id does nothing
    r.run(&mut core, &q_cancel(77));
    assert_eq!(core.open, 0);
}

#[test]
fn cancel_a_request_that_waits_for_a_connection() {
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        thread::sleep(Duration::from_secs(1));
    });
    let mut core = new_core(1, 0);
    let mut out = sget(1, addr, 0, 0);
    out.extend(sget(2, addr, 0, 0));
    let mut r = Reader::new(2);
    r.run(&mut core, &out);
    assert_eq!(core.open, 2);
    r.run(&mut core, &q_cancel(2));
    assert_eq!(core.open, 1);
    assert!(core.hosts.values().all(|h| h.queue.is_empty()));
    r.run(&mut core, &q_cancel(1));
    assert_eq!(core.open, 0);
    assert!(core.hosts.is_empty());
}

#[test]
fn the_timeout_of_a_streamed_request_ends_with_its_head() {
    // no head in time
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        thread::sleep(Duration::from_secs(1));
    });
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, 0, 50), true, |r| r.over());
    assert_eq!((r.status, r.error), (None, Some(E_TIMEOUT)));
    assert!(core.conns.is_empty());
    // a head in time and a slow body: no timeout
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhe");
        thread::sleep(Duration::from_millis(250));
        let _ = s.write_all(b"llo");
    });
    let mut r = Reader::new(2);
    r.until(&mut core, &sget(2, addr, 0, 50), true, |r| r.over());
    assert_eq!((r.status, r.ended, r.error), (Some(200), true, None));
    assert_eq!(r.body, b"hello");
}

/// Sends `size` bytes of [pattern] as the body of a POST, the way Dart
/// does: parts while less than [HIGH] bytes wait. Returns the most bytes
/// that waited, and the reader.
fn upload(
    core: &mut Core,
    addr: SocketAddr,
    head: &str,
    chunked: bool,
    size: usize,
) -> (usize, Reader) {
    let mut r = Reader::new(1);
    let head = format!("POST / HTTP/1.1\r\nhost: x\r\n{head}\r\n");
    let flags = STREAM | UPLOAD | if chunked { CHUNKED } else { 0 };
    let mut out = record(1, addr, flags, 10_000, head.as_bytes());
    let mut handed = head.len();
    let (mut at, mut ended, mut most) = (0, false, 0);
    let data = pattern(size);
    let start = Instant::now();
    while !r.over() {
        assert!(start.elapsed() < Duration::from_secs(20), "stuck");
        // the first report says that the connection works
        if r.wrote > 0 {
            while at < size && handed - r.wrote as usize <= HIGH {
                let n = (size - at).min(PART_MAX);
                // Rust makes the chunks
                handed += n;
                out.extend(q_part(1, &data[at..at + n]));
                at += n;
            }
            if at == size && !ended {
                ended = true;
                out.extend(q_end(1, false));
            }
        }
        let n = r.run(core, &out);
        out.clear();
        most = most.max(handed - r.wrote as usize);
        if n == 0 && core.again.is_empty() {
            thread::sleep(Duration::from_millis(1));
        }
    }
    (most, r)
}

/// A server that reads a request with `len` body bytes, slowly, and answers
/// 200. The bytes go to `got`.
fn upload_server(len: usize, got: Arc<std::sync::Mutex<Vec<u8>>>) -> SocketAddr {
    let (addr, _) = server(move |mut s, _| {
        if !read_head(&mut s) {
            return;
        }
        let mut buf = vec![0u8; 32 * 1024];
        let mut all = Vec::new();
        while all.len() < len {
            let want = buf.len().min(len - all.len());
            match s.read(&mut buf[..want]) {
                Ok(0) | Err(_) => return,
                Ok(n) => all.extend_from_slice(&buf[..n]),
            }
            thread::sleep(Duration::from_micros(500));
        }
        *got.lock().unwrap() = all;
        let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        thread::sleep(Duration::from_millis(100));
    });
    addr
}

#[test]
fn an_upload_with_a_length_is_byte_exact_and_bounded() {
    const SIZE: usize = 3 << 20;
    let got = Arc::new(std::sync::Mutex::new(Vec::new()));
    let addr = upload_server(SIZE, got.clone());
    let mut core = new_core(0, 0);
    let head = format!("content-length: {SIZE}\r\n");
    let (most, r) = upload(&mut core, addr, &head, false, SIZE);
    assert_eq!((r.status, r.body.as_slice()), (Some(200), &b"ok"[..]));
    assert!(r.ended);
    assert!(got.lock().unwrap().as_slice() == pattern(SIZE));
    // Dart held back when this many bytes waited
    assert!(most <= HIGH + PART_MAX + 100, "{most}");
    assert_eq!(core.open, 0);
}

#[test]
fn an_upload_with_chunked_framing_is_byte_exact() {
    const SIZE: usize = 1 << 20;
    let data = pattern(SIZE);
    let mut wire = Vec::new();
    for part in data.chunks(PART_MAX) {
        wire.extend(format!("{:x}\r\n", part.len()).bytes());
        wire.extend_from_slice(part);
        wire.extend_from_slice(b"\r\n");
    }
    wire.extend_from_slice(b"0\r\n\r\n");
    let got = Arc::new(std::sync::Mutex::new(Vec::new()));
    let addr = upload_server(wire.len(), got.clone());
    let mut core = new_core(0, 0);
    let (_, r) = upload(
        &mut core,
        addr,
        "transfer-encoding: chunked\r\n",
        true,
        SIZE,
    );
    assert_eq!((r.status, r.body.as_slice()), (Some(200), &b"ok"[..]));
    assert!(*got.lock().unwrap() == wire);
}

#[test]
fn an_upload_over_tls_is_byte_exact() {
    const SIZE: usize = 2 << 20;
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let pem = made.cert.pem();
    let key = rustls::pki_types::PrivateKeyDer::try_from(made.signing_key.serialize_der()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![made.cert.der().clone()], key)
        .unwrap();
    let config = Arc::new(config);
    let got = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = got.clone();
    let (addr, _) = server(move |s, _| {
        let conn = rustls::ServerConnection::new(config.clone()).unwrap();
        let mut tls = rustls::StreamOwned::new(conn, s);
        if !read_head(&mut tls) {
            return;
        }
        let mut all = vec![0u8; SIZE];
        if tls.read_exact(&mut all).is_err() {
            return;
        }
        *seen.lock().unwrap() = all;
        let _ = tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        let _ = tls.flush();
        thread::sleep(Duration::from_millis(100));
    });
    let mut core = tls_core(&pem);
    let mut r = Reader::new(1);
    let request = format!("POST / HTTP/1.1\r\nhost: x\r\ncontent-length: {SIZE}\r\n\r\n");
    let flags = TLS | STREAM | UPLOAD;
    let mut out = named(1, addr, "localhost", flags, 10_000, request.as_bytes());
    let mut handed = request.len();
    let (mut at, mut ended) = (0, false);
    let data = pattern(SIZE);
    let start = Instant::now();
    while !r.over() {
        assert!(start.elapsed() < Duration::from_secs(20), "stuck");
        if r.wrote > 0 {
            while at < SIZE && handed - r.wrote as usize <= HIGH {
                let n = (SIZE - at).min(PART_MAX);
                out.extend(q_part(1, &data[at..at + n]));
                handed += n;
                at += n;
            }
            if at == SIZE && !ended {
                ended = true;
                out.extend(q_end(1, false));
            }
        }
        let n = r.run(&mut core, &out);
        out.clear();
        if n == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }
    assert_eq!((r.status, r.body.as_slice()), (Some(200), &b"ok"[..]));
    assert!(got.lock().unwrap().as_slice() == data);
}

#[test]
fn an_early_answer_stops_the_upload_and_the_connection_is_not_kept() {
    let (addr, _) = server(|mut s, _| {
        if read_head(&mut s) {
            let _ = s.write_all(b"HTTP/1.1 401 No\r\nContent-Length: 0\r\n\r\n");
            // does not read the body
            thread::sleep(Duration::from_millis(600));
        }
    });
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    let head = "POST / HTTP/1.1\r\nhost: x\r\ncontent-length: 99999999\r\n\r\n";
    let mut out = record(1, addr, STREAM | UPLOAD, 10_000, head.as_bytes());
    let mut handed = head.len();
    let start = Instant::now();
    while r.status.is_none() {
        assert!(start.elapsed() < Duration::from_secs(10), "no answer");
        if r.wrote > 0 && handed - r.wrote as usize <= HIGH {
            out.extend(q_part(1, &[7u8; PART_MAX]));
            handed += PART_MAX;
        }
        r.run(&mut core, &out);
        out.clear();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(r.status, Some(401));
    // Dart stops the upload
    r.until(&mut core, &q_end(1, true), true, |r| r.over());
    assert!(r.ended, "{:?}", r.error);
    assert!(core.conns.is_empty());
    assert!(core.hosts.is_empty());
    assert_eq!(core.open, 0);
}

#[test]
fn the_timeout_of_an_upload_starts_again_with_each_part() {
    let got = Arc::new(std::sync::Mutex::new(Vec::new()));
    let addr = upload_server(40, got.clone());
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    let head = "POST / HTTP/1.1\r\nhost: x\r\ncontent-length: 40\r\n\r\n";
    // the whole upload takes about 600 ms, the timeout is 300 ms
    let out = record(1, addr, STREAM | UPLOAD, 300, head.as_bytes());
    r.until(&mut core, &out, true, |r| r.wrote > 0);
    for part in 0..4 {
        thread::sleep(Duration::from_millis(150));
        let mut out = q_part(1, &[b'a' + part; 10]);
        if part == 3 {
            out.extend(q_end(1, false));
        }
        let sent = r.wrote + 10;
        r.until(&mut core, &out, true, |r| {
            r.wrote >= sent || r.error.is_some()
        });
        assert_eq!(r.error, None);
    }
    r.until(&mut core, &[], true, |r| r.over());
    assert_eq!((r.status, r.error), (Some(200), None));
    assert_eq!(got.lock().unwrap().len(), 40);

    // an upload with no byte for the timeout fails
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        thread::sleep(Duration::from_secs(2));
    });
    let mut r = Reader::new(2);
    let out = record(2, addr, STREAM | UPLOAD, 200, head.as_bytes());
    let start = Instant::now();
    r.until(&mut core, &out, true, |r| r.over());
    assert_eq!(r.error, Some(E_TIMEOUT));
    assert!(start.elapsed() >= Duration::from_millis(200));
    assert_eq!(core.open, 0);
}

#[test]
fn a_cancelled_upload_frees_the_request() {
    let (addr, _) = server(|mut s, _| {
        read_head(&mut s);
        thread::sleep(Duration::from_millis(500));
    });
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    let head = "POST / HTTP/1.1\r\nhost: x\r\ncontent-length: 99\r\n\r\n";
    let out = record(1, addr, STREAM | UPLOAD, 0, head.as_bytes());
    r.until(&mut core, &out, true, |r| r.wrote > 0);
    r.run(&mut core, &q_cancel(1));
    assert_eq!(core.open, 0);
    assert!(core.conns.is_empty());
    // parts for a request that is gone are dropped
    r.run(&mut core, &q_part(1, b"late"));
    r.run(&mut core, &q_end(1, false));
    assert!(core.conns.is_empty());
}

#[test]
fn a_connect_failure_of_an_upload_comes_before_any_progress() {
    // nothing listens on this port
    let addr = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    let head = "POST / HTTP/1.1\r\nhost: x\r\ncontent-length: 99\r\n\r\n";
    let out = record(1, addr, STREAM | UPLOAD, 5000, head.as_bytes());
    r.until(&mut core, &out, true, |r| r.over());
    assert_eq!((r.wrote, r.error), (0, Some(E_CONNECT)));
}

#[test]
fn an_upload_is_never_sent_again() {
    // the first connection answers one request and closes on the next
    let (addr, count) = server(|mut s, n| {
        if n == 0 && read_head(&mut s) {
            let _ = s.write_all(HELLO);
            read_head(&mut s);
        }
    });
    let mut core = new_core(0, 0);
    let mut r = Reader::new(1);
    r.until(&mut core, &sget(1, addr, AGAIN, 5000), true, |r| r.over());
    assert!(r.ended);
    let mut r = Reader::new(2);
    let head = "POST / HTTP/1.1\r\nhost: x\r\ncontent-length: 1\r\n\r\n";
    // a wrong AGAIN flag does not matter
    let out = record(2, addr, STREAM | UPLOAD | AGAIN, 5000, head.as_bytes());
    r.until(&mut core, &out, true, |r| r.over());
    assert_eq!(r.error, Some(E_CLOSED));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

// ---- decoder -----------------------------------------------------------

/// All the output of an `Inflate` for `coded`, given `step` bytes at a time.
fn inflate_all(coding: Coding, coded: &[u8], step: usize) -> Result<Vec<u8>, u16> {
    let mut inflate = Inflate::new(coding).unwrap();
    let mut out = Vec::new();
    let mut buf = vec![0u8; 100];
    let mut pieces = coded.chunks(step.max(1)).peekable();
    if pieces.peek().is_none() {
        inflate.push(&[], true);
    }
    while let Some(piece) = pieces.next() {
        inflate.push(piece, pieces.peek().is_none());
        loop {
            match inflate.pull(&mut buf)? {
                Pull::Data(n) => out.extend_from_slice(&buf[..n]),
                Pull::Wait => break,
                Pull::End => return Ok(out),
            }
        }
    }
    Err(E_BAD)
}

#[test]
fn the_decoder_gives_the_same_bytes_for_each_split() {
    let text = pattern(5000);
    let mut z = gzip(&text);
    z.extend(gzip(b"second"));
    z.extend_from_slice(&[0, 0, 0, b'\n']);
    let mut expect = text.clone();
    expect.extend_from_slice(b"second");
    for step in [1, 2, 3, 7, 100, z.len()] {
        assert!(
            inflate_all(Coding::Gzip, &z, step).unwrap() == expect,
            "{step}"
        );
    }
    for step in [1, 3, BROTLI.len()] {
        let got = inflate_all(Coding::Brotli, BROTLI, step).unwrap();
        assert_eq!(got, b"Hello Hello Hello Hello", "{step}");
    }
    // cut bodies are bad
    assert_eq!(inflate_all(Coding::Gzip, &z[..40], 5), Err(E_BAD));
    assert_eq!(inflate_all(Coding::Brotli, &BROTLI[..10], 5), Err(E_BAD));
    // bytes after a brotli body are bad
    let mut extra = BROTLI.to_vec();
    extra.extend_from_slice(b"more");
    assert_eq!(inflate_all(Coding::Brotli, &extra, 4), Err(E_BAD));
    assert_eq!(inflate_all(Coding::Brotli, &extra, extra.len()), Err(E_BAD));
}

// Only in a build with `--cfg fuzzing`: the fuzz entry on real inputs.
#[cfg(fuzzing)]
#[test]
fn the_fuzz_entry_takes_coded_bodies() {
    let mut z = gzip(&pattern(3000));
    z.extend(gzip(b"two"));
    z.extend_from_slice(b"\n\n");
    for data in [&z[..], BROTLI, &z[..20], b"not coded", b"", b"\x1f\x8b"] {
        fuzz(data);
    }
    fuzz(&coded("gzip", &z));
}
