//! Single thread mode (ABI 7, a prototype): the socket work runs on the
//! thread of the Dart isolate, like Bun. There is no hand off between a
//! Rust thread and the Dart thread under load.
//!
//! Dart calls `dox_run` (a non-leaf FFI call) in a loop. One call writes
//! the answers Dart gives (response records, see lib.rs), then accepts,
//! reads and parses new requests into Dart's buffer (request records, see
//! lib.rs). It returns the bytes it wrote there. When nothing came in
//! `budget_us`, it returns 0 and hands the sockets to the idle helper.
//!
//! Idle helper: one small thread per server. It sleeps while Dart runs.
//! When `dox_run` returns 0 it waits on the sockets (epoll or kqueue via
//! mio). When a socket has data, it posts one message to the Dart port and
//! goes back to sleep. Dart then calls `dox_run` again. Only one side owns
//! the sockets at a time: the `core` mutex. When Dart calls `dox_run` while
//! the helper waits, Dart sets `want` and wakes the helper with the mio
//! `Waker`, so the helper gives the mutex back at once.
//!
//! A Dart isolate may move to another OS thread between two event loop
//! turns. So all state here is `Send` and lives behind the mutex. Nothing
//! uses thread locals.
//!
//! The helper also checks every [PROBE] that the isolate is alive (an empty
//! post). When the post fails, the isolate is gone and the server closes.
//!
//! Message ids: (connection index << 32) | sequence number. The sequence
//! number comes from one counter per server, so a late answer for a closed
//! connection never goes to a new connection in the same slot.
// ponytail: prototype. Skipped: WebSocket, streamed answers (kinds 1 to
// 8 are dropped), chunked request bodies (answered 501), Expect:
// 100-continue, big bodies as external typed data, the 408 body timeout,
// lingering close and graceful shutdown.
use std::borrow::Cow;
use std::collections::VecDeque;
use std::ffi::c_void;
use std::io::{ErrorKind, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token, Waker};
use slab::Slab;

use crate::{Key, MAX_HEAD, POST, RES_HEAD, bind, gzip, peer_bytes, post, rd16, rd32, unbind};

const LISTENER: Token = Token(0);
const WAKER: Token = Token(usize::MAX);
/// Smallest free room in a read buffer before a read.
const READ_ROOM: usize = 16 * 1024;
/// How often the idle helper checks that the isolate is alive.
const PROBE: Duration = Duration::from_millis(100);
/// Request record head: 28 bytes, then 16 bytes remote ip.
const REQ_HEAD: usize = 44;

/// One request that waits for its answer, in the order of the connection.
struct Slot {
    seq: u32,
    /// the rendered answer, when it came before the answers in front of it
    out: Option<Vec<u8>>,
    /// close the connection after this answer
    close: bool,
    head: bool,
    http10: bool,
}

struct Conn {
    sock: TcpStream,
    peer: [u8; 18],
    /// read bytes: `rpos..rlen` is not parsed yet. All of it is initialized.
    rbuf: Vec<u8>,
    rpos: usize,
    rlen: usize,
    /// bytes not written yet
    wbuf: Vec<u8>,
    slots: VecDeque<Slot>,
    /// in the ready queue
    queued: bool,
    /// in the touched list of `respond`
    dirty: bool,
    /// a request that closes the connection was parsed: parse no more
    closing: bool,
    /// the answer that closes the connection is in `wbuf`
    shut: bool,
    /// the client closed its side
    eof: bool,
}

/// What parsing the next request gave.
enum Step {
    /// the request is not complete yet
    More,
    /// a bad request: answer with this status and close
    Fail(u16),
    /// the record does not fit in the room left: needs this many bytes
    NoRoom(usize),
    /// a record was written: (bytes used of the read buffer, its slot)
    Done(usize, Slot),
}

impl Conn {
    fn new(sock: TcpStream, peer: [u8; 18]) -> Self {
        Conn {
            sock,
            peer,
            rbuf: vec![0; READ_ROOM],
            rpos: 0,
            rlen: 0,
            wbuf: Vec::new(),
            slots: VecDeque::new(),
            queued: false,
            dirty: false,
            closing: false,
            shut: false,
            eof: false,
        }
    }

    /// Reads what the socket has. False on a socket error.
    fn read(&mut self, max: usize) -> bool {
        loop {
            if self.rpos == self.rlen {
                self.rpos = 0;
                self.rlen = 0;
            }
            if self.rbuf.len() - self.rlen < READ_ROOM {
                if self.rpos > 0 {
                    self.rbuf.copy_within(self.rpos..self.rlen, 0);
                    self.rlen -= self.rpos;
                    self.rpos = 0;
                }
                if self.rbuf.len() - self.rlen < READ_ROOM {
                    if self.rlen > max {
                        return true; // parse first. A too big request fails there.
                    }
                    let size = (self.rbuf.len() * 2).max(self.rlen + READ_ROOM);
                    self.rbuf.resize(size, 0);
                }
            }
            match self.sock.read(&mut self.rbuf[self.rlen..]) {
                Ok(0) => {
                    self.eof = true;
                    return true;
                }
                Ok(k) => {
                    self.rlen += k;
                    // a short read: the socket is empty now. New bytes give
                    // a new edge, so no extra read that ends in WouldBlock.
                    if self.rlen < self.rbuf.len() {
                        return true;
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return true,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => return false,
            }
        }
    }

    /// Writes `wbuf`. False on a socket error.
    fn flush(&mut self) -> bool {
        let mut at = 0;
        while at < self.wbuf.len() {
            match (&self.sock).write(&self.wbuf[at..]) {
                Ok(0) => return false,
                Ok(k) => at += k,
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => return false,
            }
        }
        self.wbuf.drain(..at);
        true
    }

    /// True when the connection can close now.
    fn finished(&self) -> bool {
        self.wbuf.is_empty() && (self.shut || (self.eof && self.slots.is_empty() && !self.queued))
    }

    /// Moves the answers that are next in order to `wbuf`.
    fn pump(&mut self) {
        while let Some(front) = self.slots.front_mut() {
            let Some(out) = front.out.take() else { break };
            self.wbuf.extend_from_slice(&out);
            let close = front.close;
            self.slots.pop_front();
            if close {
                self.shut = true;
                self.slots.clear();
                break;
            }
        }
    }

    /// Puts the answer to request `seq` in its place.
    fn answer(&mut self, seq: u32, status: u16, lines: &[u8], body: &[u8], date: &[u8]) {
        let Some(first) = self.slots.front() else { return };
        let i = seq.wrapping_sub(first.seq) as usize;
        let Some(slot) = self.slots.get(i) else { return };
        if slot.seq != seq || slot.out.is_some() {
            return; // a stale or a second answer
        }
        if i == 0 {
            // the common case: render straight into the write buffer
            render(&mut self.wbuf, slot, status, lines, body, date);
            let close = slot.close;
            self.slots.pop_front();
            if close {
                self.shut = true;
                self.slots.clear();
                return;
            }
            self.pump();
        } else {
            let mut out = Vec::new();
            render(&mut out, slot, status, lines, body, date);
            self.slots[i].out = Some(out);
        }
    }

    /// Answers a bad request from Rust and closes after it.
    fn fail(&mut self, status: u16, date: &[u8]) {
        let slot = Slot { seq: 0, out: None, close: true, head: false, http10: false };
        let mut out = Vec::new();
        render(&mut out, &slot, status, b"", b"", date);
        self.slots.push_back(Slot { out: Some(out), ..slot });
        self.closing = true;
        self.pump();
    }

    /// Parses the complete requests of the read buffer into `inb` from
    /// `*n`. Err(size): the next record needs `size` bytes, more than the
    /// room left.
    fn parse(
        &mut self,
        idx: usize,
        inb: &mut [u8],
        n: &mut usize,
        seq: &mut u32,
        max_body: usize,
        date: &[u8],
    ) -> Result<(), usize> {
        while !self.closing && self.rpos < self.rlen {
            let id = ((idx as u64) << 32) | u64::from(*seq);
            let step = parse_one(&self.rbuf[self.rpos..self.rlen], id, &self.peer, inb, *n, max_body);
            match step {
                Step::More => break,
                Step::Fail(status) => self.fail(status, date),
                Step::NoRoom(size) => return Err(size),
                Step::Done(used, mut slot) => {
                    self.rpos += used;
                    *n += record_len(&inb[*n..]);
                    slot.seq = *seq;
                    *seq = seq.wrapping_add(1);
                    self.closing = slot.close;
                    self.slots.push_back(slot);
                }
            }
        }
        if self.eof && !self.closing && self.rpos < self.rlen && self.slots.is_empty() {
            // the client closed in the middle of a request
            self.rpos = self.rlen;
        }
        Ok(())
    }
}

/// The length of the record at the start of `b`.
fn record_len(b: &[u8]) -> usize {
    REQ_HEAD + rd16(b, 12) as usize + rd16(b, 14) as usize + rd32(b, 16) + rd32(b, 20)
}

fn eq_name(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// Parses one request from `data` and writes its record to `inb[at..]`.
fn parse_one(data: &[u8], id: u64, peer: &[u8; 18], inb: &mut [u8], at: usize, max_body: usize) -> Step {
    let mut hs = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut hs);
    let head = match req.parse(data) {
        Ok(httparse::Status::Complete(h)) => h,
        Ok(httparse::Status::Partial) if data.len() > MAX_HEAD => return Step::Fail(431),
        Ok(httparse::Status::Partial) => return Step::More,
        Err(httparse::Error::TooManyHeaders) => return Step::Fail(431),
        Err(_) => return Step::Fail(400),
    };
    if head > MAX_HEAD {
        return Step::Fail(431);
    }
    let http10 = req.version == Some(0);
    let mut close = http10;
    let mut clen = 0usize;
    let mut hlen = 0usize;
    for h in req.headers.iter() {
        hlen += 4 + h.name.len() + h.value.len();
        if eq_name(h.name, "content-length") {
            match std::str::from_utf8(h.value).ok().and_then(|v| v.trim().parse().ok()) {
                Some(v) => clen = v,
                None => return Step::Fail(400),
            }
        } else if eq_name(h.name, "transfer-encoding") {
            return Step::Fail(501); // ponytail: chunked bodies, add with the full version
        } else if eq_name(h.name, "connection") {
            if h.value.windows(5).any(|w| w.eq_ignore_ascii_case(b"close")) {
                close = true;
            } else if h.value.windows(10).any(|w| w.eq_ignore_ascii_case(b"keep-alive")) {
                close = false;
            }
        }
    }
    if clen > max_body {
        return Step::Fail(413);
    }
    if data.len() < head + clen {
        return Step::More;
    }
    let method = req.method.unwrap_or("").as_bytes();
    let target = req.path.unwrap_or("").as_bytes();
    let size = REQ_HEAD + method.len() + target.len() + hlen + clen;
    if inb.len() - at < size {
        return Step::NoRoom(size);
    }
    let b = &mut inb[at..at + size];
    b[0..8].copy_from_slice(&id.to_le_bytes());
    b[8..12].copy_from_slice(&u32::from(http10).to_le_bytes());
    b[12..14].copy_from_slice(&(method.len() as u16).to_le_bytes());
    b[14..16].copy_from_slice(&(target.len() as u16).to_le_bytes());
    b[16..20].copy_from_slice(&(hlen as u32).to_le_bytes());
    b[20..24].copy_from_slice(&(clen as u32).to_le_bytes());
    b[24..26].copy_from_slice(&peer[16..]);
    b[26..28].copy_from_slice(&[0, 0]);
    b[28..44].copy_from_slice(&peer[..16]);
    let mut p = REQ_HEAD;
    b[p..p + method.len()].copy_from_slice(method);
    p += method.len();
    b[p..p + target.len()].copy_from_slice(target);
    p += target.len();
    for h in req.headers.iter() {
        let (name, value) = (h.name.as_bytes(), h.value);
        b[p..p + 2].copy_from_slice(&(name.len() as u16).to_le_bytes());
        b[p + 2..p + 4].copy_from_slice(&(value.len() as u16).to_le_bytes());
        p += 4;
        for (d, s) in b[p..p + name.len()].iter_mut().zip(name) {
            *d = s.to_ascii_lowercase();
        }
        p += name.len();
        b[p..p + value.len()].copy_from_slice(value);
        p += value.len();
    }
    b[p..p + clen].copy_from_slice(&data[head..head + clen]);
    let head_req = method == b"HEAD";
    Step::Done(head + clen, Slot { seq: 0, out: None, close, head: head_req, http10 })
}

/// Writes one HTTP answer. `lines` are header lines from Dart (no
/// content-length, Rust sets it).
fn render(buf: &mut Vec<u8>, slot: &Slot, status: u16, lines: &[u8], body: &[u8], date: &[u8]) {
    let status = if (100..=999).contains(&status) { status } else { 500 };
    buf.extend_from_slice(if slot.http10 { b"HTTP/1.0 " } else { b"HTTP/1.1 " });
    buf.extend_from_slice(&[
        b'0' + (status / 100) as u8,
        b'0' + (status / 10 % 10) as u8,
        b'0' + (status % 10) as u8,
        b' ',
    ]);
    let reason = hyper::StatusCode::from_u16(status)
        .ok()
        .and_then(|s| s.canonical_reason())
        .unwrap_or("");
    buf.extend_from_slice(reason.as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(lines);
    let no_body = status < 200 || status == 204 || status == 304;
    if !no_body {
        let _ = write!(buf, "content-length: {}\r\n", body.len());
    }
    buf.extend_from_slice(date);
    if slot.close {
        buf.extend_from_slice(b"connection: close\r\n");
    } else if slot.http10 {
        buf.extend_from_slice(b"connection: keep-alive\r\n");
    }
    buf.extend_from_slice(b"\r\n");
    if !no_body && !slot.head {
        buf.extend_from_slice(body);
    }
}

/// All socket state of one server. Only the owner of the mutex touches it.
struct Core {
    poll: Poll,
    events: Events,
    listener: Option<TcpListener>,
    conns: Slab<Conn>,
    /// connections that may have bytes to read or requests to parse
    ready: VecDeque<usize>,
    /// the listener may have connections to accept
    accept: bool,
    /// the idle helper watches the sockets
    armed: bool,
    seq: u32,
    max_body: usize,
    /// (second, `date: ...\r\n`)
    date: (u64, Vec<u8>),
    /// lists reused per call
    dead: Vec<usize>,
    touched: Vec<usize>,
}

impl Core {
    fn update_date(&mut self) {
        let now = SystemTime::now();
        let sec = now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
        if sec != self.date.0 || self.date.1.is_empty() {
            self.date = (sec, format!("date: {}\r\n", httpdate::fmt_http_date(now)).into_bytes());
        }
    }

    fn kill(&mut self, idx: usize) {
        if let Some(mut c) = self.conns.try_remove(idx) {
            let _ = self.poll.registry().deregister(&mut c.sock);
        }
    }

    fn kill_dead(&mut self) {
        while let Some(idx) = self.dead.pop() {
            self.kill(idx);
        }
    }

    /// Waits up to `timeout` for socket events, then handles them: writes
    /// what waits for room, queues readable connections. Returns
    /// (any event, the waker fired).
    fn wait(&mut self, timeout: Option<Duration>) -> (bool, bool) {
        if self.poll.poll(&mut self.events, timeout).is_err() {
            return (false, false); // eg. interrupted
        }
        let (mut any, mut woke) = (false, false);
        for ev in self.events.iter() {
            any = true;
            match ev.token() {
                LISTENER => self.accept = true,
                WAKER => woke = true,
                Token(t) => {
                    let idx = t - 1;
                    let Some(c) = self.conns.get_mut(idx) else { continue };
                    if ev.is_writable() && !c.wbuf.is_empty() && !c.flush() {
                        self.dead.push(idx);
                        continue;
                    }
                    if (ev.is_readable() || ev.is_read_closed() || ev.is_error()) && !c.queued {
                        c.queued = true;
                        self.ready.push_back(idx);
                    }
                    if c.finished() {
                        self.dead.push(idx);
                    }
                }
            }
        }
        self.kill_dead();
        (any, woke)
    }

    fn accept_all(&mut self) {
        let Some(listener) = &self.listener else { return };
        loop {
            match listener.accept() {
                Ok((mut sock, addr)) => {
                    let _ = sock.set_nodelay(true);
                    let entry = self.conns.vacant_entry();
                    let idx = entry.key();
                    let interest = Interest::READABLE | Interest::WRITABLE;
                    if self.poll.registry().register(&mut sock, Token(idx + 1), interest).is_err() {
                        continue;
                    }
                    let mut conn = Conn::new(sock, peer_bytes(addr));
                    conn.queued = true; // the request is often there already
                    entry.insert(conn);
                    self.ready.push_back(idx);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                // ponytail: eg. too many open files. The connection stays in
                // the backlog until the next accept edge.
                Err(_) => break,
            }
        }
        self.accept = false;
    }

    /// Writes the answers in `out` (response records).
    fn respond(&mut self, out: &[u8]) {
        let mut p = 0;
        while p + RES_HEAD <= out.len() {
            let id = u64::from_le_bytes(out[p..p + 8].try_into().unwrap());
            let status = rd16(out, p + 8);
            let kind = rd16(out, p + 10);
            let hlen = rd32(out, p + 12);
            let blen = rd32(out, p + 16);
            let h_at = p + RES_HEAD;
            if h_at + hlen + blen > out.len() {
                break; // broken record: drop the rest
            }
            let body_at = h_at + hlen;
            p = body_at + blen;
            if kind & 0xff != 0 {
                continue; // ponytail: WebSocket and streamed answers
            }
            let idx = (id >> 32) as usize;
            let Some(c) = self.conns.get_mut(idx) else { continue };
            let body = &out[body_at..p];
            let body: Cow<[u8]> = if kind & 0x100 != 0 { Cow::Owned(gzip(body)) } else { Cow::Borrowed(body) };
            c.answer(id as u32, status, &out[h_at..body_at], &body, &self.date.1);
            if !c.dirty {
                c.dirty = true;
                self.touched.push(idx);
            }
        }
        while let Some(idx) = self.touched.pop() {
            let c = &mut self.conns[idx];
            c.dirty = false;
            if !c.flush() || c.finished() {
                self.dead.push(idx);
            }
        }
        self.kill_dead();
    }

    /// Accepts, reads and parses into `inb`. Returns the bytes written, or
    /// Err(size) when one record is bigger than all of `inb`.
    fn fill(&mut self, inb: &mut [u8]) -> Result<usize, usize> {
        if self.accept {
            self.accept_all();
        }
        let mut n = 0;
        let max = MAX_HEAD + self.max_body;
        while let Some(&idx) = self.ready.front() {
            let Some(c) = self.conns.get_mut(idx) else {
                self.ready.pop_front();
                continue;
            };
            if !c.read(max) {
                self.ready.pop_front();
                self.kill(idx);
                continue;
            }
            let res = c.parse(idx, inb, &mut n, &mut self.seq, self.max_body, &self.date.1);
            if let Err(size) = res {
                // stays in the queue for the next call
                return if n == 0 { Err(size) } else { Ok(n) };
            }
            c.queued = false;
            self.ready.pop_front();
            if !c.wbuf.is_empty() && !c.flush() {
                self.kill(idx);
                continue;
            }
            if self.conns[idx].finished() {
                self.kill(idx);
            }
        }
        Ok(n)
    }
}

/// One server in single thread mode. Dart holds an `Arc` of it as handle.
struct Native {
    core: Mutex<Core>,
    waker: Waker,
    /// Dart waits for the core: the helper gives it back
    want: AtomicBool,
    /// wakes the helper: Dart went idle
    kick: Mutex<bool>,
    cv: Condvar,
    stopped: AtomicBool,
    key: Key,
    dart_port: i64,
    helper: Mutex<Option<JoinHandle<()>>>,
}

impl Native {
    fn arm(&self) {
        *self.kick.lock().unwrap() = true;
        self.cv.notify_one();
    }

    /// The idle helper: watches the sockets while Dart is idle.
    fn helper(&self) {
        loop {
            {
                let mut k = self.kick.lock().unwrap();
                while !*k && !self.stopped.load(Ordering::Acquire) {
                    k = self.cv.wait(k).unwrap();
                }
                *k = false;
            }
            if self.stopped.load(Ordering::Acquire) {
                return;
            }
            let mut core = self.core.lock().unwrap();
            loop {
                if !core.armed || self.stopped.load(Ordering::Acquire) {
                    break; // Dart owns the sockets again
                }
                if !core.ready.is_empty() || core.accept {
                    core.armed = false;
                    drop(core);
                    post(self.dart_port, &[1]);
                    break;
                }
                let (any, woke) = core.wait(Some(PROBE));
                if woke && self.want.load(Ordering::Acquire) {
                    break;
                }
                if !any && !post(self.dart_port, &[]) {
                    // the isolate is gone and did not stop the server
                    self.stopped.store(true, Ordering::Release);
                    core.listener = None;
                    core.conns.clear();
                    unbind(self.key);
                    return;
                }
            }
        }
    }
}

// ---- FFI ---------------------------------------------------------------

/// Binds `port` for single thread mode. Same arguments as `dox_start`
/// (the timeouts are not used yet). Returns a handle (> 0), or < 0: -1
/// not initialized, -2 no poller, -(1000 + errno) when the bind fails.
#[unsafe(no_mangle)]
pub extern "C" fn dox_bind(
    port: i32,
    send_port: i64,
    max_body: i64,
    _max_multipart: i64,
    _body_timeout_ms: i64,
    family: i32,
    addr_hi: u64,
    addr_lo: u64,
) -> i64 {
    if POST.get().is_none() {
        return -1;
    }
    let (listener, key) = match bind((family, addr_hi, addr_lo, port as u16)) {
        Ok(l) => l,
        Err(e) => return -(1000 + i64::from(e.raw_os_error().unwrap_or(0))),
    };
    let Ok(poll) = Poll::new() else { return -2 };
    let Ok(waker) = Waker::new(poll.registry(), WAKER) else { return -2 };
    let mut listener = TcpListener::from_std(listener);
    if poll.registry().register(&mut listener, LISTENER, Interest::READABLE).is_err() {
        return -2;
    }
    let core = Core {
        poll,
        events: Events::with_capacity(1024),
        listener: Some(listener),
        conns: Slab::with_capacity(1024),
        ready: VecDeque::new(),
        accept: true,
        armed: true,
        seq: 0,
        max_body: max_body.max(0) as usize,
        date: (0, Vec::new()),
        dead: Vec::new(),
        touched: Vec::new(),
    };
    let native = Arc::new(Native {
        core: Mutex::new(core),
        waker,
        want: AtomicBool::new(false),
        kick: Mutex::new(true),
        cv: Condvar::new(),
        stopped: AtomicBool::new(false),
        key,
        dart_port: send_port,
        helper: Mutex::new(None),
    });
    let for_helper = native.clone();
    let Ok(t) = std::thread::Builder::new()
        .name("dox-http-idle".into())
        .spawn(move || for_helper.helper())
    else {
        return -2;
    };
    *native.helper.lock().unwrap() = Some(t);
    Arc::into_raw(native) as i64
}

/// The port of a `dox_bind` server.
///
/// # Safety
/// `handle` must come from `dox_bind` and not be unbound.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_bound_port(handle: i64) -> i32 {
    let s = unsafe { &*(handle as *const Native) };
    i32::from(s.key.3)
}

/// Writes the answers in `out`, then reads new requests into `inb`.
/// Waits up to `budget_us` when there are none. Returns the bytes written
/// to `inb`. 0: nothing came. The idle helper now watches the sockets and
/// posts to the Dart port when there is something. < 0: one request needs
/// a bigger `inb`, at least -n bytes (call again with a bigger one).
///
/// # Safety
/// `handle` must come from `dox_bind` and not be unbound. `out` must point
/// to `out_len` readable bytes, `inb` to `in_cap` writable bytes. Only one
/// call per handle at a time (one isolate).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_run(
    handle: i64,
    out: *const u8,
    out_len: i64,
    inb: *mut u8,
    in_cap: i64,
    budget_us: i64,
) -> i64 {
    let s = unsafe { &*(handle as *const Native) };
    let mut core = match s.core.try_lock() {
        Ok(c) => c,
        Err(_) => {
            // the helper waits on the sockets: wake it, it gives way
            s.want.store(true, Ordering::Release);
            let _ = s.waker.wake();
            s.core.lock().unwrap()
        }
    };
    s.want.store(false, Ordering::Release);
    core.armed = false;
    core.update_date();
    if out_len > 0 {
        core.respond(unsafe { std::slice::from_raw_parts(out, out_len as usize) });
    }
    let inb = unsafe { std::slice::from_raw_parts_mut(inb, in_cap.max(0) as usize) };
    core.wait(Some(Duration::ZERO));
    let mut got = core.fill(inb);
    if got == Ok(0) && budget_us > 0 {
        core.wait(Some(Duration::from_micros(budget_us as u64)));
        got = core.fill(inb);
    }
    match got {
        Ok(0) => {
            core.armed = true;
            drop(core);
            s.arm();
            0
        }
        Ok(n) => n as i64,
        Err(size) => -(size as i64),
    }
}

/// Stops a `dox_bind` server, closes its sockets and frees the handle.
///
/// # Safety
/// `handle` must come from `dox_bind`. Do not use it after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_unbind(handle: i64) {
    let s = unsafe { Arc::from_raw(handle as *const Native) };
    let was = s.stopped.swap(true, Ordering::AcqRel);
    s.want.store(true, Ordering::Release);
    let _ = s.waker.wake();
    s.arm();
    let helper = s.helper.lock().unwrap().take();
    if let Some(t) = helper {
        let _ = t.join();
    }
    if !was {
        unbind(s.key);
    }
    let mut core = s.core.lock().unwrap();
    core.listener = None;
    core.conns.clear();
}

/// Native memory for the Dart buffers (non-leaf calls can not use Dart
/// heap bytes).
#[unsafe(no_mangle)]
pub extern "C" fn dox_alloc(size: i64) -> *mut c_void {
    let b: Box<[u8]> = vec![0u8; size.max(1) as usize].into_boxed_slice();
    Box::into_raw(b) as *mut u8 as *mut c_void
}

/// Frees memory from `dox_alloc`.
///
/// # Safety
/// `ptr` and `size` must come from one `dox_alloc` call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_free(ptr: *mut c_void, size: i64) {
    let slice = std::ptr::slice_from_raw_parts_mut(ptr as *mut u8, size.max(1) as usize);
    drop(unsafe { Box::from_raw(slice) });
}
