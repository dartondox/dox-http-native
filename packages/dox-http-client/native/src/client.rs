//! The HTTP/1.1 client. It runs on the thread of the Dart isolate.
//!
//! Dart calls `dox_client_run` with the new requests (request records). One
//! call writes them to connections, reads what the sockets have (it never
//! waits) and gives Dart the finished answers (response records).
//!
//! When a call has no answer and requests are open, the idle helper (one
//! small thread per client) watches the poll file. It posts one byte to the
//! Dart port when a socket has data or a request is over its timeout. Dart
//! then calls `dox_client_run` again. The flag `armed` makes sure only one
//! side tells Dart: the one that clears it.
//!
//! A Dart isolate may move to another OS thread between two event loop
//! turns. So all state here is `Send` and lives behind the mutex.
//!
//! Request record (Dart to Rust, little endian), [REQ_HEAD] bytes, then the
//! server name and then the raw HTTP request (head and body):
//!   0  u32 length of the record
//!   4  u64 id
//!   12 u8  family (4 or 6)
//!   13 u8  flags: [HEAD_ONLY], [AGAIN], [TLS]
//!   14 u16 port
//!   16 16 bytes address (IPv4: the first 4)
//!   32 u32 timeout in milliseconds (0: none)
//!   36 u16 length of the server name (the host of an `https://` URL)
//!
//! Response record (Rust to Dart), [RES_HEAD] bytes, then the header lines
//! (`name:value\n`, names in lower case) and then the body:
//!   0  u32 length of the record
//!   4  u64 id
//!   12 u16 status (0: the request failed)
//!   14 u16 error ([E_CONNECT] ...) when the status is 0
//!   16 u32 length of the header lines
//!
//! TLS (`https://`): rustls, with the root certificates of Mozilla
//! (webpki-roots) and the ones Dart gives. The server name is checked
//! against the certificate. TLS 1.2 and 1.3.
//!
//! Connections:
//! - A connection stays open after an answer (keep-alive) and the next
//!   request to the same address uses it.
//! - One request per connection at a time (no pipelining).
//! - With `max_per_host` > 0, at most that many connections are open to
//!   one address. More requests wait for a free one, in order.
//! - A kept connection may be closed by the server just when the next
//!   request goes out. When no answer byte came, a request with [AGAIN]
//!   is sent one more time, before the requests that wait.
use std::collections::{HashMap, VecDeque};
use std::ffi::c_void;
use std::io::{ErrorKind, Read, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore};
use slab::Slab;

use crate::{POST, post, rd32};

const REQ_HEAD: usize = 40;
const RES_HEAD: usize = 20;
/// Smallest free room in a read buffer before a read.
const READ_ROOM: usize = 16 * 1024;
/// Most bytes one connection reads in one step. Then the limits are
/// checked, and the other connections get their turn.
const READ_MAX: usize = 256 * 1024;
/// Most bytes all connections read in one `dox_client_run` call after the
/// first step of each. Then Dart gets its event loop back.
const RUN_MAX: usize = 4 * 1024 * 1024;
/// A read buffer bigger than this is freed after its answer.
const KEEP_BUF: usize = 64 * 1024;
/// Most bytes of the status line plus headers of an answer.
const MAX_HEAD: usize = 64 * 1024;
const MAX_HEADERS: usize = 100;
/// most bytes of a chunk size line, with its extensions
const CHUNK_LINE: usize = 1024;
/// The biggest body when Dart gives no limit: a record length is a u32.
const MAX_BODY: usize = u32::MAX as usize - MAX_HEAD - RES_HEAD;
/// How often idle connections are checked against the idle timeout, at
/// the most.
const SWEEP: Duration = Duration::from_secs(1);
/// How often the helper looks at the stop flag while it waits.
const TICK: Duration = Duration::from_millis(100);

/// Request flag: the answer has no body (a HEAD request).
const HEAD_ONLY: u8 = 1;
/// Request flag: it may be sent again when a kept connection was closed.
const AGAIN: u8 = 2;
/// Request flag: an `https://` request. The record has the server name.
const TLS: u8 = 4;

/// Could not connect. Nothing was sent.
const E_CONNECT: u16 = 1;
const E_TIMEOUT: u16 = 2;
/// The connection closed or failed before the answer was whole.
const E_CLOSED: u16 = 3;
/// The answer is not correct HTTP.
const E_BAD: u16 = 4;
/// The body is bigger than the limit.
const E_BIG: u16 = 5;
/// TLS failed, eg. the certificate of the server is not trusted.
const E_TLS: u16 = 6;

struct Req {
    id: u64,
    flags: u8,
    /// it was sent a second time: never a third time
    retried: bool,
    deadline: Option<Instant>,
    bytes: Vec<u8>,
}

enum ChunkState {
    Size,
    Data(usize),
    DataEnd,
    Trailer,
}

/// Reads a chunked body in steps, as the bytes come.
struct Chunked {
    /// the next byte to read in the buffer
    pos: usize,
    out: Vec<u8>,
    state: ChunkState,
}

impl Chunked {
    /// Reads on in `b`. Some(end): the body is whole and ends before
    /// offset `end`. None: more bytes must come.
    fn step(&mut self, b: &[u8], max: usize) -> Result<Option<usize>, u16> {
        loop {
            match self.state {
                ChunkState::Size => {
                    let rest = &b[self.pos..];
                    let Some(nl) = rest.iter().position(|&c| c == b'\n') else {
                        return if rest.len() > CHUNK_LINE {
                            Err(E_BAD)
                        } else {
                            Ok(None)
                        };
                    };
                    if nl > CHUNK_LINE {
                        return Err(E_BAD);
                    }
                    let line = &rest[..nl];
                    let hex = line.split(|&c| c == b';').next().unwrap_or(&[]);
                    let hex = hex.trim_ascii();
                    if hex.is_empty() || hex.len() > 16 || !hex.iter().all(u8::is_ascii_hexdigit) {
                        return Err(E_BAD);
                    }
                    let hex = std::str::from_utf8(hex).map_err(|_| E_BAD)?;
                    let n = u64::from_str_radix(hex, 16).map_err(|_| E_BAD)?;
                    if n > (max - self.out.len().min(max)) as u64 {
                        return Err(E_BIG);
                    }
                    self.pos += nl + 1;
                    self.state = if n == 0 {
                        ChunkState::Trailer
                    } else {
                        ChunkState::Data(n as usize)
                    };
                }
                ChunkState::Data(left) => {
                    let take = left.min(b.len() - self.pos);
                    self.out.extend_from_slice(&b[self.pos..self.pos + take]);
                    self.pos += take;
                    if take < left {
                        self.state = ChunkState::Data(left - take);
                        return Ok(None);
                    }
                    self.state = ChunkState::DataEnd;
                }
                ChunkState::DataEnd => {
                    if b.len() - self.pos < 2 {
                        return Ok(None);
                    }
                    if &b[self.pos..self.pos + 2] != b"\r\n" {
                        return Err(E_BAD);
                    }
                    self.pos += 2;
                    self.state = ChunkState::Size;
                }
                ChunkState::Trailer => {
                    let rest = &b[self.pos..];
                    let Some(nl) = rest.iter().position(|&c| c == b'\n') else {
                        return if rest.len() > MAX_HEAD {
                            Err(E_BAD)
                        } else {
                            Ok(None)
                        };
                    };
                    if nl > MAX_HEAD {
                        return Err(E_BAD);
                    }
                    let empty = nl == 0 || (nl == 1 && rest[0] == b'\r');
                    self.pos += nl + 1;
                    if empty {
                        return Ok(Some(self.pos));
                    }
                }
            }
        }
    }
}

enum Body {
    Len(usize),
    Chunked(Chunked),
    /// no length: the body ends when the connection closes
    Eof,
}

/// The status line and headers of an answer.
struct Head {
    status: u16,
    /// offset of the first body byte in the read buffer
    start: usize,
    /// the header lines for Dart
    lines: Vec<u8>,
    keep: bool,
    body: Body,
}

fn has_token(value: &[u8], token: &str) -> bool {
    value
        .split(|&c| c == b',')
        .any(|t| t.trim_ascii().eq_ignore_ascii_case(token.as_bytes()))
}

/// Reads the head of an answer. None: it is not whole yet. Answers with a
/// 1xx status (eg. 100 Continue) come before the real one and are dropped.
fn parse_head(buf: &[u8], head_only: bool, max: usize) -> Result<Option<Head>, u16> {
    let mut skip = 0;
    loop {
        let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut res = httparse::Response::new(&mut headers);
        let n = match res.parse(&buf[skip..]) {
            Ok(httparse::Status::Complete(n)) => n,
            Ok(httparse::Status::Partial) => return Ok(None),
            Err(_) => return Err(E_BAD),
        };
        let status = res.code.unwrap_or(0);
        // 101: we never ask for another protocol
        if status < 100 || status == 101 {
            return Err(E_BAD);
        }
        if status < 200 {
            skip += n;
            continue;
        }
        let mut keep = res.version == Some(1);
        let mut close = false;
        let mut length: Option<usize> = None;
        let mut encoded = false;
        let mut chunked = false;
        let mut lines = Vec::with_capacity(n);
        for h in res.headers.iter() {
            lines.extend(h.name.bytes().map(|c| c.to_ascii_lowercase()));
            lines.push(b':');
            lines.extend_from_slice(h.value);
            lines.push(b'\n');
            if h.name.eq_ignore_ascii_case("content-length") {
                let v = h.value.trim_ascii();
                if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
                    return Err(E_BAD);
                }
                let v: usize = std::str::from_utf8(v)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or(E_BIG)?;
                if length.is_some_and(|old| old != v) {
                    return Err(E_BAD);
                }
                length = Some(v);
            } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
                encoded = true;
                // chunked must be the last coding
                chunked = h
                    .value
                    .rsplit(|&c| c == b',')
                    .next()
                    .is_some_and(|t| t.trim_ascii().eq_ignore_ascii_case(b"chunked"));
            } else if h.name.eq_ignore_ascii_case("connection") {
                if has_token(h.value, "close") {
                    close = true;
                } else if has_token(h.value, "keep-alive") {
                    keep = true;
                }
            }
        }
        // one `close` wins over each `keep-alive`
        keep = keep && !close;
        let start = skip + n;
        let body = if head_only || status == 204 || status == 304 {
            Body::Len(0)
        } else if encoded {
            // a length next to an encoding is not to be trusted (RFC 9112
            // 6.3): the connection closes after this answer
            keep = keep && length.is_none();
            if chunked {
                Body::Chunked(Chunked {
                    pos: start,
                    out: Vec::new(),
                    state: ChunkState::Size,
                })
            } else {
                Body::Eof
            }
        } else if let Some(n) = length {
            if n > max {
                return Err(E_BIG);
            }
            Body::Len(n)
        } else {
            Body::Eof
        };
        if matches!(body, Body::Eof) {
            keep = false;
        }
        return Ok(Some(Head {
            status,
            start,
            lines,
            keep,
            body,
        }));
    }
}

/// Where a connection goes: the address, and for TLS the name of the
/// server. Requests with the same key share connections.
#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    addr: SocketAddr,
    name: Option<Arc<str>>,
}

/// Makes room for the next read in a read buffer.
fn room(buf: &mut Vec<u8>, filled: usize) {
    if buf.len() - filled < READ_ROOM {
        let size = (buf.len() * 2).max(filled + READ_ROOM);
        buf.resize(size, 0);
    }
}

struct Conn {
    stream: TcpStream,
    key: Key,
    /// TLS state of an `https://` connection
    tls: Option<Box<ClientConnection>>,
    /// the request in flight
    req: Option<Req>,
    sent: usize,
    /// The read buffer. Bytes up to `filled` are from the socket.
    buf: Vec<u8>,
    filled: usize,
    head: Option<Head>,
    /// a write or a read worked: the connection is open
    connected: bool,
    /// it answered a request before
    reused: bool,
    /// when its last answer was whole (for an idle connection)
    idle_since: Instant,
    /// The last `io` stopped at [READ_MAX]: the socket may have more
    /// bytes, and no new event comes for them.
    more: bool,
}

impl Conn {
    /// Writes what is left of the request and reads what the socket has.
    /// Returns the error when the connection failed, true when the server
    /// closed it, and true when a TLS connection ended with no TLS close.
    fn io(&mut self) -> (Option<u16>, bool, bool) {
        self.more = false;
        match self.tls.take() {
            Some(mut tls) => {
                let result = self.tls_io(&mut tls);
                self.tls = Some(tls);
                result
            }
            None => {
                let (fail, eof) = self.plain_io();
                (fail, eof, false)
            }
        }
    }

    fn plain_io(&mut self) -> (Option<u16>, bool) {
        let bytes: &[u8] = self.req.as_ref().map_or(&[], |r| &r.bytes);
        let mut fail = false;
        let mut eof = false;
        while self.sent < bytes.len() {
            match self.stream.write(&bytes[self.sent..]) {
                Ok(0) => {
                    fail = true;
                    break;
                }
                Ok(n) => {
                    self.sent += n;
                    self.connected = true;
                }
                Err(e) => match e.kind() {
                    ErrorKind::Interrupted => {}
                    ErrorKind::WouldBlock | ErrorKind::NotConnected => break,
                    _ => {
                        fail = true;
                        break;
                    }
                },
            }
        }
        // Read also after a write that failed: the server may have answered
        // and closed before it read the whole request (eg. 413).
        let start = self.filled;
        loop {
            if self.filled - start >= READ_MAX {
                self.more = true;
                break;
            }
            room(&mut self.buf, self.filled);
            match self.stream.read(&mut self.buf[self.filled..]) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                Ok(n) => {
                    self.filled += n;
                    self.connected = true;
                }
                Err(e) => match e.kind() {
                    ErrorKind::Interrupted => {}
                    ErrorKind::WouldBlock | ErrorKind::NotConnected => break,
                    _ => {
                        fail = true;
                        break;
                    }
                },
            }
        }
        let error = if self.connected { E_CLOSED } else { E_CONNECT };
        (fail.then_some(error), eof)
    }

    /// Like `plain_io`, through TLS: the request goes into `tls` and comes
    /// out as TLS records for the socket, and the other way for the answer.
    /// The handshake runs in the same steps.
    fn tls_io(&mut self, tls: &mut ClientConnection) -> (Option<u16>, bool, bool) {
        let bytes: &[u8] = self.req.as_ref().map_or(&[], |r| &r.bytes);
        let closed = |connected: bool| Some(if connected { E_CLOSED } else { E_CONNECT });
        let mut fail: Option<u16> = None;
        let mut eof = false;
        let mut cut = false;
        let start = self.filled;
        loop {
            let mut progress = false;
            if fail.is_none() && self.sent < bytes.len() {
                // takes what fits in the TLS buffer, also during the handshake
                match tls.writer().write(&bytes[self.sent..]) {
                    Ok(n) => {
                        self.sent += n;
                        progress |= n > 0;
                    }
                    Err(_) => fail = Some(E_TLS),
                }
            }
            while fail.is_none() && tls.wants_write() {
                match tls.write_tls(&mut self.stream) {
                    Ok(0) => fail = closed(self.connected),
                    Ok(_) => {
                        self.connected = true;
                        progress = true;
                    }
                    Err(e) => match e.kind() {
                        ErrorKind::Interrupted => {}
                        ErrorKind::WouldBlock | ErrorKind::NotConnected => break,
                        _ => fail = closed(self.connected),
                    },
                }
            }
            // also after a write that failed: the server may have answered
            // and closed before it read the whole request (eg. 413)
            if fail != Some(E_TLS) && !eof {
                match tls.read_tls(&mut self.stream) {
                    Ok(0) => eof = true,
                    Ok(_) => {
                        self.connected = true;
                        progress = true;
                        if tls.process_new_packets().is_err() {
                            // tell the server why (a TLS alert), if it fits
                            let _ = tls.write_tls(&mut self.stream);
                            fail = Some(E_TLS);
                        }
                    }
                    Err(e) => match e.kind() {
                        ErrorKind::Interrupted => progress = true,
                        ErrorKind::WouldBlock | ErrorKind::NotConnected => {}
                        _ => fail = fail.or(closed(self.connected)),
                    },
                }
            }
            // also after a failure: the answer may be whole before it
            loop {
                room(&mut self.buf, self.filled);
                match tls.reader().read(&mut self.buf[self.filled..]) {
                    // the server closed the TLS connection
                    Ok(0) => {
                        eof = true;
                        break;
                    }
                    Ok(n) => {
                        self.filled += n;
                        progress = true;
                    }
                    Err(e) => match e.kind() {
                        ErrorKind::Interrupted => {}
                        ErrorKind::WouldBlock => break,
                        // the socket closed with no TLS close
                        ErrorKind::UnexpectedEof => {
                            eof = true;
                            cut = true;
                            break;
                        }
                        _ => {
                            fail = fail.or(Some(E_TLS));
                            break;
                        }
                    },
                }
            }
            if fail.is_some() || !progress || (eof && !tls.wants_write()) {
                return (fail, eof, cut);
            }
            if self.filled - start >= READ_MAX {
                self.more = true;
                return (fail, eof, cut);
            }
        }
    }
}

/// One answer for Dart.
struct Done {
    id: u64,
    status: u16,
    error: u16,
    lines: Vec<u8>,
    body: Vec<u8>,
}

/// The connections to one address (and one TLS name).
#[derive(Default)]
struct Host {
    idle: Vec<usize>,
    /// open connections, busy and idle
    count: usize,
    /// requests that wait for a free connection
    queue: VecDeque<Req>,
}

struct Core {
    poll: Poll,
    events: Events,
    /// `events` has events that no call looked at yet (Windows helper)
    stored: bool,
    tokens: Vec<usize>,
    /// connections whose read stopped at [READ_MAX]
    again: Vec<usize>,
    conns: Slab<Conn>,
    hosts: HashMap<Key, Host>,
    ready: VecDeque<Done>,
    /// requests with no answer in `ready` yet
    open: usize,
    /// the first request timeout. It may be the timeout of a request that
    /// is done: then the check finds nothing.
    due: Option<Instant>,
    now: Instant,
    max_per_host: usize,
    max_body: usize,
    /// an idle connection closes after this time. None: never.
    idle_timeout: Option<Duration>,
    /// when `run` next looks for idle connections to close
    next_sweep: Instant,
    tls: Arc<ClientConfig>,
    /// The 2 buffers of Dart for `dox_client_run`: the requests and the
    /// answers. They live here, so they are freed with the client, also
    /// when Dart never closes it. See `dox_client_buffer`.
    bufs: [Vec<u8>; 2],
}

impl Core {
    fn new(
        max_per_host: usize,
        max_body: usize,
        idle_timeout: Option<Duration>,
        tls: Arc<ClientConfig>,
    ) -> std::io::Result<Core> {
        Ok(Core {
            poll: Poll::new()?,
            events: Events::with_capacity(256),
            stored: false,
            tokens: Vec::new(),
            again: Vec::new(),
            conns: Slab::new(),
            hosts: HashMap::new(),
            ready: VecDeque::new(),
            open: 0,
            due: None,
            now: Instant::now(),
            max_per_host,
            max_body: if max_body == 0 {
                MAX_BODY
            } else {
                max_body.min(MAX_BODY)
            },
            idle_timeout,
            next_sweep: Instant::now(),
            tls,
            bufs: [Vec::new(), Vec::new()],
        })
    }

    fn done(&mut self, id: u64, status: u16, lines: Vec<u8>, body: Vec<u8>) {
        self.ready.push_back(Done {
            id,
            status,
            error: 0,
            lines,
            body,
        });
        self.open -= 1;
    }

    fn failed(&mut self, id: u64, error: u16) {
        self.ready.push_back(Done {
            id,
            status: 0,
            error,
            lines: Vec::new(),
            body: Vec::new(),
        });
        self.open -= 1;
    }

    /// Closes connection `idx` and gives its place to a waiting request.
    fn close(&mut self, idx: usize) -> Option<Conn> {
        let conn = self.conns.try_remove(idx)?;
        if let Some(host) = self.hosts.get_mut(&conn.key) {
            host.idle.retain(|&i| i != idx);
            host.count -= 1;
        }
        Some(conn)
    }

    /// The request of connection `idx` failed. Sends it again when the
    /// connection was a kept one with no answer byte, else Dart gets
    /// `error`.
    fn fail(&mut self, idx: usize, error: u16) {
        let Some(conn) = self.close(idx) else { return };
        let key = conn.key;
        if let Some(mut req) = conn.req {
            // `head`: the read buffer of a chunked answer is empty again
            // after its head, so `filled` alone does not say "no answer byte"
            let again = conn.reused
                && conn.filled == 0
                && conn.head.is_none()
                && req.flags & AGAIN != 0
                && !req.retried
                && error == E_CLOSED;
            if again {
                // first in the queue: it is older than each request there
                req.retried = true;
                self.hosts
                    .entry(key.clone())
                    .or_default()
                    .queue
                    .push_front(req);
            } else {
                self.failed(req.id, error);
            }
        }
        self.pump(&key);
    }

    /// Drops the entry of `key` when it has no connection and no request.
    fn forget(&mut self, key: &Key) {
        if self
            .hosts
            .get(key)
            .is_some_and(|h| h.count == 0 && h.queue.is_empty())
        {
            self.hosts.remove(key);
        }
    }

    fn connect(&mut self, key: Key, req: Req) {
        let tls = match &key.name {
            Some(name) => {
                let made = ServerName::try_from(name.to_string())
                    .ok()
                    .and_then(|name| ClientConnection::new(self.tls.clone(), name).ok());
                let Some(tls) = made else {
                    self.forget(&key);
                    return self.failed(req.id, E_TLS);
                };
                Some(Box::new(tls))
            }
            None => None,
        };
        let Ok(mut stream) = TcpStream::connect(key.addr) else {
            self.forget(&key);
            return self.failed(req.id, E_CONNECT);
        };
        let _ = stream.set_nodelay(true);
        let entry = self.conns.vacant_entry();
        let idx = entry.key();
        if self
            .poll
            .registry()
            .register(
                &mut stream,
                Token(idx),
                Interest::READABLE | Interest::WRITABLE,
            )
            .is_err()
        {
            self.forget(&key);
            return self.failed(req.id, E_CONNECT);
        }
        entry.insert(Conn {
            stream,
            key: key.clone(),
            tls,
            req: Some(req),
            sent: 0,
            buf: Vec::new(),
            filled: 0,
            head: None,
            connected: false,
            reused: false,
            idle_since: self.now,
            more: false,
        });
        self.hosts.entry(key).or_default().count += 1;
    }

    /// Sends a request on an idle connection for `key` or on a new one, or
    /// lets it wait when `max_per_host` connections are open.
    fn start(&mut self, key: Key, req: Req) {
        while let Some(idx) = self.hosts.get_mut(&key).and_then(|h| h.idle.pop()) {
            let Some(c) = self.conns.get_mut(idx) else {
                continue;
            };
            // Read first. The server may have closed the connection, or
            // sent bytes nobody asked for: they must not become the answer
            // to this request.
            let (fail, eof, _) = c.io();
            if fail.is_some() || eof || c.filled > 0 {
                self.close(idx);
                continue;
            }
            c.req = Some(req);
            c.sent = 0;
            return self.drive(idx);
        }
        let host = self.hosts.entry(key.clone()).or_default();
        if self.max_per_host == 0 || host.count < self.max_per_host {
            self.connect(key, req);
        } else {
            host.queue.push_back(req);
        }
    }

    /// Starts the requests that wait for a connection for `key`, while
    /// there is a free one. Drops the entry of a key with nothing.
    fn pump(&mut self, key: &Key) {
        loop {
            let Some(host) = self.hosts.get_mut(key) else {
                return;
            };
            if host.queue.is_empty() {
                if host.count == 0 {
                    self.hosts.remove(key);
                }
                return;
            }
            if host.idle.is_empty() && self.max_per_host != 0 && host.count >= self.max_per_host {
                return;
            }
            let Some(req) = host.queue.pop_front() else {
                return;
            };
            self.start(key.clone(), req);
        }
    }

    /// Runs `step` for connection `idx`. When its read stopped at
    /// [READ_MAX], the connection gets one more step later in this call or
    /// in the next one.
    fn drive(&mut self, idx: usize) {
        self.step(idx);
        if self.conns.get(idx).is_some_and(|c| c.more) {
            self.again.push(idx);
        }
    }

    /// Writes what is left of the request, reads what the socket has and
    /// finishes the request when its answer is whole.
    fn step(&mut self, idx: usize) {
        let max = self.max_body;
        let Some(c) = self.conns.get_mut(idx) else {
            return;
        };
        let (fail, eof, cut) = c.io();
        let Some(req) = &c.req else {
            // an idle connection: the server closed it, or sent bytes
            // nobody asked for
            if fail.is_some() || eof || c.filled > 0 {
                let key = c.key.clone();
                self.close(idx);
                self.pump(&key);
            }
            return;
        };
        // A connection that failed is at its end too. The bytes that came
        // before may be a whole answer: then the request is done.
        let eof = eof || fail.is_some();
        let cut = cut || fail.is_some();
        let ended = fail.unwrap_or(if c.connected { E_CLOSED } else { E_CONNECT });
        if c.head.is_none() {
            match parse_head(&c.buf[..c.filled], req.flags & HEAD_ONLY != 0, max) {
                Ok(Some(head)) => c.head = Some(head),
                Ok(None) if c.filled > MAX_HEAD => return self.fail(idx, E_BAD),
                Ok(None) if eof => return self.fail(idx, ended),
                Ok(None) => return,
                Err(error) => return self.fail(idx, error),
            }
        }
        let Some(head) = c.head.as_mut() else { return };
        // the body and the offset after the answer, when it is whole
        let whole: Result<Option<(Vec<u8>, usize)>, u16> = match &mut head.body {
            Body::Len(n) => {
                let end = head.start + *n;
                if c.filled >= end {
                    Ok(Some((c.buf[head.start..end].to_vec(), end)))
                } else if eof {
                    Err(ended)
                } else {
                    Ok(None)
                }
            }
            Body::Chunked(ch) => match ch.step(&c.buf[..c.filled], max) {
                Ok(Some(end)) => Ok(Some((std::mem::take(&mut ch.out), end))),
                Ok(None) if eof => Err(ended),
                Ok(None) => {
                    // the bytes before `pos` are in `out`: use the room again
                    c.buf.copy_within(ch.pos..c.filled, 0);
                    c.filled -= ch.pos;
                    ch.pos = 0;
                    Ok(None)
                }
                Err(error) => Err(error),
            },
            Body::Eof => {
                if c.filled - head.start > max {
                    Err(E_BIG)
                } else if cut {
                    // the end of the body is the end of the connection: an
                    // end with no TLS close, or a failure, may be a cut body
                    Err(ended)
                } else if eof {
                    Ok(Some((c.buf[head.start..c.filled].to_vec(), c.filled)))
                } else {
                    Ok(None)
                }
            }
        };
        let (body, end) = match whole {
            Ok(Some(v)) => v,
            Ok(None) => return,
            Err(error) => return self.fail(idx, error),
        };
        let id = req.id;
        let status = head.status;
        let lines = std::mem::take(&mut head.lines);
        let key = c.key.clone();
        // The server may answer before it has the whole request (eg. 401 or
        // 413 for a big body). It then still waits for the other bytes, and
        // would read the next request as those bytes.
        let sent_all = c.sent == req.bytes.len() && c.tls.as_ref().is_none_or(|t| !t.wants_write());
        // bytes after the answer are not ours: do not use the connection
        // again
        if head.keep && !eof && end == c.filled && sent_all {
            c.req = None;
            c.head = None;
            c.sent = 0;
            c.filled = 0;
            c.reused = true;
            c.idle_since = self.now;
            if c.buf.len() > KEEP_BUF {
                c.buf = Vec::new();
            }
            self.hosts.entry(key.clone()).or_default().idle.push(idx);
        } else {
            self.close(idx);
        }
        self.done(id, status, lines, body);
        self.pump(&key);
    }

    /// Looks at each socket that has an event.
    fn turn(&mut self) {
        self.stored = false;
        self.tokens.clear();
        self.tokens.extend(self.events.iter().map(|e| e.token().0));
        for i in 0..self.tokens.len() {
            self.drive(self.tokens[i]);
        }
    }

    /// Closes the connections that are idle for `idle_timeout` or longer.
    fn sweep(&mut self) {
        let Some(limit) = self.idle_timeout else {
            return;
        };
        let now = self.now;
        self.next_sweep = now + limit.min(SWEEP);
        let old: Vec<usize> = self
            .conns
            .iter()
            .filter(|(_, c)| c.req.is_none() && now.duration_since(c.idle_since) >= limit)
            .map(|(idx, _)| idx)
            .collect();
        for idx in old {
            if let Some(conn) = self.close(idx) {
                self.forget(&conn.key);
            }
        }
    }

    /// Fails the requests that are over their timeout.
    fn expire(&mut self) {
        let now = self.now;
        let over = |r: &Req| r.deadline.is_some_and(|d| d <= now);
        // The queues first: a connection that closes below gives its place
        // to a waiting request, and that request must not be over its
        // timeout too.
        let mut late: Vec<u64> = Vec::new();
        for host in self.hosts.values_mut() {
            host.queue.retain(|r| {
                let keep = !over(r);
                if !keep {
                    late.push(r.id);
                }
                keep
            });
        }
        for id in late {
            self.failed(id, E_TIMEOUT);
        }
        let late: Vec<usize> = self
            .conns
            .iter()
            .filter(|(_, c)| c.req.as_ref().is_some_and(over))
            .map(|(idx, _)| idx)
            .collect();
        for idx in late {
            if let Some(conn) = self.close(idx) {
                if let Some(req) = conn.req {
                    self.failed(req.id, E_TIMEOUT);
                }
                self.pump(&conn.key);
            }
        }
        let busy = self.conns.iter().filter_map(|(_, c)| c.req.as_ref());
        let waiting = self.hosts.values().flat_map(|h| h.queue.iter());
        self.due = busy.chain(waiting).filter_map(|r| r.deadline).min();
    }

    /// See `dox_client_run`. Err(size): the next record needs `size` bytes.
    fn run(&mut self, out: &[u8], inb: &mut [u8]) -> Result<usize, usize> {
        self.now = Instant::now();
        let mut p = 0;
        while p + REQ_HEAD <= out.len() {
            let len = rd32(out, p);
            if len < REQ_HEAD || p + len > out.len() {
                break;
            }
            let rec = &out[p..p + len];
            p += len;
            let id = u64::from_le_bytes(rec[4..12].try_into().unwrap());
            let port = u16::from_le_bytes(rec[14..16].try_into().unwrap());
            let addr: SocketAddr = if rec[12] == 6 {
                let ip: [u8; 16] = rec[16..32].try_into().unwrap();
                (Ipv6Addr::from(ip), port).into()
            } else {
                (Ipv4Addr::new(rec[16], rec[17], rec[18], rec[19]), port).into()
            };
            let ms = rd32(rec, 32) as u64;
            let name_len = u16::from_le_bytes(rec[36..38].try_into().unwrap()) as usize;
            if REQ_HEAD + name_len > len {
                break;
            }
            let name = &rec[REQ_HEAD..REQ_HEAD + name_len];
            let flags = rec[13];
            let deadline = (ms > 0).then(|| self.now + Duration::from_millis(ms));
            self.open += 1;
            let name = if flags & TLS == 0 {
                None
            } else if let Ok(name) = std::str::from_utf8(name) {
                Some(Arc::from(name))
            } else {
                self.failed(id, E_TLS);
                continue;
            };
            if let Some(d) = deadline {
                self.due = Some(self.due.map_or(d, |old| old.min(d)));
            }
            self.start(
                Key { addr, name },
                Req {
                    id,
                    flags,
                    retried: false,
                    deadline,
                    bytes: rec[REQ_HEAD + name_len..].to_vec(),
                },
            );
        }
        if self.stored {
            self.turn();
        }
        if !self.conns.is_empty() {
            let _ = self.poll.poll(&mut self.events, Some(Duration::ZERO));
            self.turn();
        }
        // the connections with more bytes to read, up to [RUN_MAX] in all
        let mut steps = RUN_MAX / READ_MAX;
        while steps > 0 && !self.again.is_empty() {
            let list = std::mem::take(&mut self.again);
            for idx in list {
                if steps > 0 {
                    steps -= 1;
                    self.drive(idx);
                } else {
                    self.again.push(idx);
                }
            }
        }
        if self.due.is_some_and(|d| d <= self.now) {
            self.expire();
        }
        if self.now >= self.next_sweep {
            self.sweep();
        }
        let mut n = 0;
        while let Some(d) = self.ready.front() {
            let size = RES_HEAD + d.lines.len() + d.body.len();
            if n + size > inb.len() {
                if n == 0 {
                    return Err(size);
                }
                break;
            }
            let rec = &mut inb[n..n + size];
            rec[0..4].copy_from_slice(&(size as u32).to_le_bytes());
            rec[4..12].copy_from_slice(&d.id.to_le_bytes());
            rec[12..14].copy_from_slice(&d.status.to_le_bytes());
            rec[14..16].copy_from_slice(&d.error.to_le_bytes());
            rec[16..20].copy_from_slice(&(d.lines.len() as u32).to_le_bytes());
            let body = RES_HEAD + d.lines.len();
            rec[RES_HEAD..body].copy_from_slice(&d.lines);
            rec[body..].copy_from_slice(&d.body);
            n += size;
            self.ready.pop_front();
        }
        Ok(n)
    }

    /// Closes all connections. Open requests get no answer.
    fn stop(&mut self) {
        self.conns.clear();
        self.again.clear();
        self.hosts.clear();
        self.ready.clear();
        self.open = 0;
        self.due = None;
    }
}

/// One client. Dart holds an `Arc` of it as handle.
struct Client {
    core: Mutex<Core>,
    /// the poll file (epoll or kqueue), watched by the idle helper
    #[cfg(unix)]
    fd: i32,
    /// Dart waits: the helper watches, and tells Dart when work comes
    armed: AtomicBool,
    /// the first timeout of the core, in nanoseconds after `base`.
    /// `u64::MAX`: none.
    due: AtomicU64,
    base: Instant,
    /// wakes the helper: Dart waits for an answer
    kick: Mutex<bool>,
    cv: Condvar,
    stopped: AtomicBool,
    /// No request is open, but connections are: Dart does not call, so
    /// the helper closes the connections that are idle for too long.
    quiet: AtomicBool,
    /// how often the helper looks for them
    sweep_every: Duration,
    dart_port: i64,
}

impl Client {
    /// Closes the idle connections that are over the idle timeout, while
    /// Dart does not call.
    fn sweep(&self) {
        let mut core = self.core.lock().unwrap();
        core.now = Instant::now();
        core.sweep();
        if core.conns.is_empty() {
            self.quiet.store(false, Ordering::Release);
        }
    }

    fn set_due(&self, at: Option<Instant>) {
        let ns = at.map_or(u64::MAX, |t| {
            t.saturating_duration_since(self.base)
                .as_nanos()
                .min(u128::from(u64::MAX - 1)) as u64
        });
        self.due.store(ns, Ordering::Release);
    }

    /// How long until the first timeout. None: no timeout.
    fn due_in(&self) -> Option<Duration> {
        let ns = self.due.load(Ordering::Acquire);
        (ns != u64::MAX).then(|| {
            (self.base + Duration::from_nanos(ns)).saturating_duration_since(Instant::now())
        })
    }

    /// true when the poll file has an event within `wait`. Does not read
    /// the event and does not take the core: `dox_client_run` does that.
    #[cfg(unix)]
    fn ready_within(&self, wait: Duration) -> bool {
        let mut p = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // round up, so a timeout a bit in the future does not spin
        let ms = wait.as_micros().div_ceil(1000).min(i32::MAX as u128) as i32;
        let r = unsafe { libc::poll(&mut p, 1, ms) };
        r > 0 && p.revents & libc::POLLIN != 0
    }

    /// Windows has no poll file to watch: take the core for at most 1 ms
    /// at a time, so Dart waits at most that long. The events stay in the
    /// core for the next `dox_client_run`.
    #[cfg(not(unix))]
    fn ready_within(&self, wait: Duration) -> bool {
        let mut core = self.core.lock().unwrap();
        if !core.stored {
            let core = &mut *core;
            let wait = wait.min(Duration::from_millis(1));
            let _ = core.poll.poll(&mut core.events, Some(wait));
            core.stored = !core.events.is_empty();
        }
        core.stored
    }

    /// The idle helper: watches the poll file while Dart waits for an
    /// answer.
    fn helper(&self) {
        loop {
            {
                let mut k = self.kick.lock().unwrap();
                while !*k && !self.stopped.load(Ordering::Acquire) {
                    if !self.quiet.load(Ordering::Acquire) {
                        k = self.cv.wait(k).unwrap();
                        continue;
                    }
                    let (guard, time) = self.cv.wait_timeout(k, self.sweep_every).unwrap();
                    k = guard;
                    if time.timed_out() && !*k {
                        // never hold `kick` and wait for the core: Dart
                        // takes the core first, then `kick`
                        drop(k);
                        self.sweep();
                        k = self.kick.lock().unwrap();
                    }
                }
                *k = false;
            }
            while self.armed.load(Ordering::Acquire) && !self.stopped.load(Ordering::Acquire) {
                let wait = self.due_in().map_or(TICK, |d| d.min(TICK));
                let ready = self.ready_within(wait);
                if self.stopped.load(Ordering::Acquire) {
                    return;
                }
                if ready || self.due_in() == Some(Duration::ZERO) {
                    // only the side that clears `armed` tells Dart
                    if self
                        .armed
                        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                        && !post(self.dart_port, &[1])
                    {
                        // the isolate is gone
                        self.stopped.store(true, Ordering::Release);
                        return self.core.lock().unwrap().stop();
                    }
                    break;
                }
            }
            if self.stopped.load(Ordering::Acquire) {
                return;
            }
        }
    }
}

// ---- FFI ---------------------------------------------------------------

/// The TLS setup of a client: the root certificates of Mozilla plus the
/// certificates in `extra` (PEM). None: `extra` is not correct PEM, or has
/// a certificate that can not be a root.
fn tls_config(extra: &[u8]) -> Option<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut added = 0;
    for cert in CertificateDer::pem_slice_iter(extra) {
        roots.add(cert.ok()?).ok()?;
        added += 1;
    }
    if added == 0 && !extra.is_empty() {
        return None;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .ok()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Some(Arc::new(config))
}

/// Makes a client. Its idle helper posts `[1]` to `send_port` when a
/// socket has data for a waiting request. With `max_per_host` > 0 it holds
/// at most that many connections open to one address. With `max_body` > 0,
/// a request whose answer body is bigger fails. With `idle_ms` > 0, a
/// connection with no request for that long closes. `ca` (`ca_len` bytes,
/// PEM) has more root certificates to trust for `https://`. Returns a handle
/// (> 0), or < 0: -1 not initialized, -2 no poller or thread, -3 `ca` is
/// not correct.
///
/// # Safety
/// `ca` must point to `ca_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_client_new(
    send_port: i64,
    max_per_host: i64,
    max_body: i64,
    idle_ms: i64,
    ca: *const u8,
    ca_len: i64,
) -> i64 {
    if POST.get().is_none() {
        return -1;
    }
    let ca: &[u8] = if ca_len > 0 {
        unsafe { std::slice::from_raw_parts(ca, ca_len as usize) }
    } else {
        &[]
    };
    let Some(tls) = tls_config(ca) else {
        return -3;
    };
    let idle = (idle_ms > 0).then(|| Duration::from_millis(idle_ms as u64));
    let Ok(core) = Core::new(
        max_per_host.max(0) as usize,
        max_body.max(0) as usize,
        idle,
        tls,
    ) else {
        return -2;
    };
    #[cfg(unix)]
    let fd = {
        use std::os::fd::AsRawFd;
        core.poll.as_raw_fd()
    };
    let client = Arc::new(Client {
        core: Mutex::new(core),
        #[cfg(unix)]
        fd,
        armed: AtomicBool::new(false),
        due: AtomicU64::new(u64::MAX),
        base: Instant::now(),
        kick: Mutex::new(false),
        cv: Condvar::new(),
        stopped: AtomicBool::new(false),
        quiet: AtomicBool::new(false),
        sweep_every: idle.map_or(SWEEP, |d| d.min(SWEEP)),
        dart_port: send_port,
    });
    let for_helper = client.clone();
    if std::thread::Builder::new()
        .name("dox-http-client".into())
        .spawn(move || for_helper.helper())
        .is_err()
    {
        return -2;
    }
    Arc::into_raw(client) as i64
}

/// Sends the requests in `out`, then writes the finished answers to `inb`.
/// Never waits. Returns the bytes written to `inb`. 0 with open requests:
/// the idle helper now watches the sockets and posts `[1]` to the Dart
/// port when there is work. More than 0: call again after a turn of the
/// event loop while requests are open. < 0: one record needs a bigger
/// `inb`, at least -n bytes (call again with a bigger one).
///
/// # Safety
/// `handle` must come from `dox_client_new` and not be closed. `out` must
/// point to `out_len` readable bytes, `inb` to `in_cap` writable bytes.
/// They can be the buffers of `dox_client_buffer`: this call does not
/// change those. Only one call per handle at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_client_run(
    handle: i64,
    out: *const u8,
    out_len: i64,
    inb: *mut u8,
    in_cap: i64,
) -> i64 {
    let s = unsafe { &*(handle as *const Client) };
    let mut core = s.core.lock().unwrap();
    s.armed.store(false, Ordering::Release);
    let out: &[u8] = if out_len > 0 {
        unsafe { std::slice::from_raw_parts(out, out_len as usize) }
    } else {
        &[]
    };
    let inb = unsafe { std::slice::from_raw_parts_mut(inb, in_cap.max(0) as usize) };
    let got = core.run(out, inb);
    // No request is open: Dart may not call for a long time. The helper
    // then closes the connections that stay idle for too long.
    let mut wake = false;
    if core.open == 0 && core.idle_timeout.is_some() && !core.conns.is_empty() {
        wake = !s.quiet.swap(true, Ordering::AcqRel);
    }
    if wake {
        // the helper starts its checks of the idle connections
        *s.kick.lock().unwrap() = true;
        s.cv.notify_one();
    }
    match got {
        Ok(0) if !core.again.is_empty() => {
            // a connection has more bytes to read and no event comes for
            // them: come back after a turn of the event loop
            drop(core);
            post(s.dart_port, &[1]);
            0
        }
        Ok(0) if core.open > 0 => {
            s.set_due(core.due);
            s.armed.store(true, Ordering::Release);
            drop(core);
            *s.kick.lock().unwrap() = true;
            s.cv.notify_one();
            0
        }
        Ok(n) => n as i64,
        Err(size) => -(size as i64),
    }
}

/// Gives buffer `which` (0: the requests for `dox_client_run`, 1: its
/// answers) a size of `size` bytes and returns it. A buffer that grows
/// keeps its bytes. The buffer is freed with the client. Null: `which` is
/// not 0 or 1.
///
/// # Safety
/// `handle` must come from `dox_client_new` and not be closed. The last
/// pointer of the same buffer is not valid after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_client_buffer(handle: i64, which: i32, size: i64) -> *mut u8 {
    let s = unsafe { &*(handle as *const Client) };
    let mut core = s.core.lock().unwrap();
    let Some(buf) = core.bufs.get_mut(which as usize) else {
        return std::ptr::null_mut();
    };
    let size = size.max(1) as usize;
    if size < buf.len() {
        // a smaller buffer: give the memory back
        *buf = vec![0u8; size];
    } else {
        buf.resize(size, 0);
    }
    buf.as_mut_ptr()
}

/// Closes a client: its connections close and the handle is freed. Open
/// requests get no answer. Also the finalizer of the Dart object.
///
/// # Safety
/// `handle` must come from `dox_client_new`. Do not use it after this
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_client_close(handle: *mut c_void) {
    let s = unsafe { Arc::from_raw(handle as *const Client) };
    s.stopped.store(true, Ordering::Release);
    s.armed.store(false, Ordering::Release);
    *s.kick.lock().unwrap() = true;
    s.cv.notify_one();
    // the helper ends by itself and drops the last `Arc`
    s.core.lock().unwrap().stop();
}

/// The fuzz target `client_response`: random bytes as the answer of a
/// server. The head parser must not panic. A chunked body must give the
/// same result in one piece and byte by byte.
#[cfg(fuzzing)]
pub(crate) fn fuzz(data: &[u8]) {
    const MAX: usize = 1 << 20;
    let _ = parse_head(data, true, MAX);
    let Ok(Some(head)) = parse_head(data, false, MAX) else {
        return;
    };
    assert!(head.start <= data.len());
    let Body::Chunked(mut whole) = head.body else {
        return;
    };
    let all = whole.step(data, MAX);
    let mut parts = Chunked {
        pos: head.start,
        out: Vec::new(),
        state: ChunkState::Size,
    };
    let mut split = Ok(None);
    for n in head.start..=data.len() {
        split = parts.step(&data[..n], MAX);
        if !matches!(split, Ok(None)) {
            break;
        }
    }
    assert_eq!(all, split);
    if all.is_ok() {
        assert_eq!(whole.out, parts.out);
        assert!(whole.out.len() <= MAX);
    }
}

#[cfg(test)]
mod tests;
