use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream as StdStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use super::*;

struct Answer {
    id: u64,
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
    Core::new(max_per_host, max_body, None, tls_config(&[]).unwrap()).unwrap()
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
fn head_with_a_length() {
    let h = head("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Name: A b\r\n\r\nhello")
        .unwrap()
        .unwrap();
    assert_eq!(h.status, 200);
    assert!(h.keep);
    assert!(matches!(h.body, Body::Len(5)));
    assert_eq!(h.lines, b"content-length:5\nx-name:A b\n");
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
        assert_eq!(a[0].lines, "content-length:5\nx-a:1\n");
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
    Core::new(0, 0, None, tls_config(pem.as_bytes()).unwrap()).unwrap()
}

#[test]
fn https_requests_use_one_connection_again() {
    let (addr, pem, count) = tls_server(&["localhost"], HELLO.to_vec());
    let mut core = tls_core(&pem);
    for id in 1..=3 {
        let a = run_until(&mut core, &tls_get(id, addr, "localhost"), 1);
        assert_eq!((a[0].status, a[0].error), (200, 0));
        assert_eq!(a[0].body, b"hello");
        assert_eq!(a[0].lines, "content-length:5\nx-a:1\n");
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
    assert!(tls_config(&[]).is_some());
    assert!(tls_config(b"not a certificate").is_none());
    let broken = b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    assert!(tls_config(broken).is_none());
    let made = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    assert!(tls_config(made.cert.pem().as_bytes()).is_some());
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
    let mut core = Core::new(0, 0, idle, tls_config(&[]).unwrap()).unwrap();
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
