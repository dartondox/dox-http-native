//! The server loop. It runs on the thread of the Dart isolate.
//!
//! Dart calls `dox_run` (a non-leaf FFI call) in a loop. One call:
//! 1. writes the answers Dart gives (response records, see lib.rs),
//! 2. runs the timers that are due (408, lingering close, WebSocket waits),
//! 3. accepts, reads and parses new requests into Dart's buffer (request
//!    records), plus the WebSocket and stream messages for Dart.
//!
//! It returns the bytes it wrote there. Dart may pass a budget: then the
//! call waits up to that long for new requests when there are none. Dart
//! passes a budget only when it has no open request and its last call
//! found nothing. Else Dart may have other work (timers, database replies,
//! other ports), and a wait here would hold it up.
//!
//! When a call finds nothing, it returns 0 and hands the sockets to the
//! idle helper: one small thread per server. It sleeps while Dart runs.
//! Then it waits on the sockets (epoll or kqueue via mio) and runs the
//! timers. When there is something for Dart, it posts one byte to the Dart
//! port and goes back to sleep. Dart then calls `dox_run` again. Only one
//! side owns the sockets at a time: the `core` mutex. When Dart calls
//! `dox_run` while the helper waits, Dart sets `want` and wakes the helper
//! with the mio `Waker`, so the helper gives the mutex back at once.
//!
//! A Dart isolate may move to another OS thread between two event loop
//! turns. So all state here is `Send` and lives behind the mutex. Nothing
//! uses thread locals.
//!
//! The helper also checks every [PROBE] that the isolate is alive (an empty
//! post), also while Dart owns the sockets. When the post fails, the
//! isolate is gone and the server closes: the port is free.
//!
//! Request ids: (connection index << 32) | sequence number. The sequence
//! number comes from one counter per server, so a late answer for a closed
//! connection never goes to a new connection in the same slot.
//!
//! Per connection:
//! - Answers go out in the order of the requests, also when Dart answers
//!   them in another order. At most [MAX_INFLIGHT] requests may wait for an
//!   answer. Then Rust stops reading that connection until answers go out.
//! - A body that is not whole in the first read is read apart (a length or
//!   chunked). Too big: 413. With `Expect: 100-continue` the 413 comes at
//!   once, else the body is read and dropped first (up to [DRAIN_MAX]), so
//!   the client sees the 413 and not a reset. No body bytes for the body
//!   timeout: 408. A request head must be whole in [HEAD_WAIT], and an idle
//!   connection closes after it.
//! - When Rust closes a connection after an answer, it first closes its
//!   own side, then reads and drops what is left for up to [LINGER]
//!   ("lingering close"). Else the OS could send a reset and the client
//!   could lose the last answer (eg. a 413).
//! - gzip (kind bit 0x100) runs on one helper thread per server, so the
//!   isolate does not wait for it.
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::ffi::c_void;
use std::io::{ErrorKind, Read, Write};
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token, Waker};
use slab::Slab;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::{CloseFrame, Role, WebSocketConfig, WebSocketContext};
use tungstenite::{Error as WsError, Message, Utf8Bytes};

use crate::{
    CLOSE_WAIT, DRAIN_MAX, EXTERNAL_MIN, Key, MAX_HEAD, MAX_RESERVE, MIN_QUEUE, POST, REQ_HEAD,
    RES_HEAD, SEND_WAIT, STREAM_GONE, STREAM_SENT, WS_BINARY, WS_CLOSED, WS_TEXT, WS_UNACKED, bind,
    gzip, peer_bytes, post, rd16, rd32, unbind,
};

const LISTENER: Token = Token(0);
const WAKER: Token = Token(usize::MAX);
/// Smallest free room in a read buffer before a read.
const READ_ROOM: usize = 16 * 1024;
/// How often the helper checks that the isolate is alive.
const PROBE: Duration = Duration::from_millis(100);
/// Most requests of one connection that wait for an answer. Then Rust
/// stops reading that connection until answers go out.
const MAX_INFLIGHT: usize = 64;
/// A request head must come whole in this time, and a connection with no
/// request closes after it.
const HEAD_WAIT: Duration = Duration::from_secs(30);
/// Lingering close: how long, and how many bytes, Rust drops.
const LINGER: Duration = Duration::from_secs(1);
const LINGER_MAX: usize = 1 << 20;
/// Wait before the next accept after an error (eg. too many open files).
const ACCEPT_RETRY: Duration = Duration::from_millis(10);
/// Bytes not written yet before Rust gives tungstenite the next message.
const WS_HOLD: usize = 64 * 1024;
/// Most bytes one `ws_read` call takes from a socket. Then the socket
/// waits for the next turn, so a client that sends many small frames (eg.
/// pings) can not hold the isolate.
const WS_READ: usize = 256 * 1024;
/// Over this many bytes not written yet (eg. pongs to a client that does
/// not read), Rust stops reading the frames of that WebSocket. It reads on
/// once less than [WS_HOLD] waits.
const WS_OUT_MAX: usize = 4 * WS_HOLD;
/// Over this many answer bytes not written yet, Rust stops reading new
/// requests of that connection (pipelining by a client that does not read
/// the answers). It reads on once they are written.
const MAX_PENDING: usize = 1 << 20;
/// A write buffer bigger than this is freed once it is empty.
const BIG_BUF: usize = 1 << 20;
/// Most headers of one request. More gets 431.
const MAX_HEADERS: usize = 100;

/// One request that waits for its answer, in the order of the connection.
#[derive(Default)]
struct Slot {
    seq: u32,
    /// answer bytes, when they came before the answers in front of it
    out: Vec<u8>,
    /// the answer is whole: the slot can go once `out` is written
    done: bool,
    /// an answer came (maybe not whole yet: a stream or a gzip job)
    answered: bool,
    /// close the connection after this answer
    close: bool,
    head: bool,
    http10: bool,
    /// `Sec-WebSocket-Key` of a request that can upgrade
    key: Option<Box<[u8]>>,
    /// Dart accepted the upgrade: the connection is a WebSocket after it
    upgrade: bool,
    /// a streamed body: (stream id, how it ends)
    stream: Option<(u64, Tail)>,
    /// stream body bytes in `out`, told to Dart once they are written
    unsent: u64,
}

/// How a streamed body is framed and how it ends.
#[derive(Clone, Copy, PartialEq)]
enum Tail {
    /// in chunks, then `0\r\n\r\n`
    Chunked,
    /// the connection closes after it
    Close,
    /// Dart sent `content-length`: this many bytes are still due
    Left(u64),
}

/// A request body that is read apart from its head.
struct BodyRead {
    /// the record head, method, target and headers. Id and body length
    /// are set when the body is whole.
    rec: Vec<u8>,
    data: Vec<u8>,
    /// length mode: bytes still to come. Chunked: see `chunk`.
    left: usize,
    chunk: Option<Chunk>,
    max: usize,
    /// all body bytes, also the dropped ones
    seen: usize,
    /// over `max`: drop the bytes and answer 413 at the end
    too_big: bool,
    /// the body is whole, the record waits for room in Dart's buffer
    whole: bool,
    /// when the last body bytes came
    last: Instant,
    slot: Slot,
}

/// Where a chunked body is.
#[derive(Clone, Copy, PartialEq)]
enum Chunk {
    Size,
    Data(usize),
    DataEnd,
    Trailer,
}

/// A WebSocket of a connection.
struct Ws {
    ctx: WebSocketContext,
    id: u64,
    max: usize,
    /// messages from Dart that tungstenite does not have yet
    queue: VecDeque<Message>,
    bytes: usize,
    cap: usize,
    /// the 101 answer is written: frames flow
    live: bool,
    /// the close code and reason for Dart, once known
    code: Option<(u16, Vec<u8>)>,
    /// the closed record went to Dart
    told: bool,
    /// we sent a close frame at this time
    closing: Option<Instant>,
    /// Dart is behind: no frames are read
    paused: bool,
}

struct Conn {
    sock: TcpStream,
    peer: [u8; 18],
    /// read bytes: `rpos..rlen` is not parsed yet. All of it is initialized.
    rbuf: Vec<u8>,
    rpos: usize,
    rlen: usize,
    /// `wpos..` is not written yet
    wbuf: Vec<u8>,
    wpos: usize,
    /// bytes written to the socket so far
    written: u64,
    /// stream bytes to tell Dart once `written` gets to the first value:
    /// (end, stream id, bytes)
    marks: VecDeque<(u64, u64, u64)>,
    slots: VecDeque<Slot>,
    body: Option<Box<BodyRead>>,
    ws: Option<Box<Ws>>,
    /// in the ready queue
    queued: bool,
    /// in the touched list of `respond`
    dirty: bool,
    /// parse no more requests (a close, an error or a shutdown)
    closing: bool,
    /// a request that can upgrade waits for its answer: parse no more now
    upgrading: bool,
    /// reading stopped (too many open requests, or Dart is behind on a
    /// WebSocket). Rust queues the connection again when it can go on.
    paused: bool,
    /// the 101 answer went out: switch to the WebSocket
    switch: bool,
    /// close once `wbuf` is written
    shut: bool,
    /// the client closed its side
    eof: bool,
    /// lingering close: (started, bytes dropped)
    linger: Option<(Instant, usize)>,
    /// when the last request came or the last answer went out (or the
    /// accept): the idle time counts from here
    since: Instant,
    /// the time of the one timer entry this connection relies on
    timer: Option<Instant>,
    /// writes that wait: when they last made progress. No progress for
    /// [SEND_WAIT] closes the connection (HTTP and WebSocket).
    stuck: Option<Instant>,
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
    /// the head is read, the body comes apart: (bytes used, its state,
    /// send `100 Continue`)
    Body(usize, Box<BodyRead>, bool),
}

/// What happens to a body when new bytes came.
enum Feed {
    More,
    Fail(u16),
    Whole,
}

/// A due timer of a connection.
#[derive(PartialEq)]
enum Due {
    Linger,
    Body,
    Head,
    Ws,
    Send,
}

/// Body limits of one server.
#[derive(Clone, Copy)]
struct Limits {
    /// max bytes of a body that is not a multipart form
    body: usize,
    /// max bytes of a multipart form (file uploads)
    multipart: usize,
    /// max idle time with no body bytes. Longer gets 408.
    timeout: Duration,
}

/// How to write the head of an answer.
#[derive(Clone, Copy)]
struct Way {
    http10: bool,
    head: bool,
    close: bool,
    /// Dart's header lines already hold `connection: close`
    said: bool,
}

impl Way {
    fn of(s: &Slot) -> Self {
        Way {
            http10: s.http10,
            head: s.head,
            close: s.close,
            said: false,
        }
    }
}

/// The body framing of an answer.
#[derive(Clone, Copy, PartialEq)]
enum Len {
    Exact(usize),
    Chunked,
    /// the body ends when the connection closes (HTTP/1.0 streams), or
    /// Dart sent `content-length` itself
    Open,
    /// no body and no length (101)
    None,
}

/// Writes the head of an answer. `lines` are header lines from Dart.
fn render_head(
    buf: &mut Vec<u8>,
    status: u16,
    lines: &[u8],
    extra: &[u8],
    date: &[u8],
    w: Way,
    len: Len,
) {
    let status = if (100..=999).contains(&status) {
        status
    } else {
        500
    };
    buf.extend_from_slice(if w.http10 { b"HTTP/1.0 " } else { b"HTTP/1.1 " });
    buf.extend_from_slice(&[
        b'0' + (status / 100) as u8,
        b'0' + (status / 10 % 10) as u8,
        b'0' + (status % 10) as u8,
        b' ',
    ]);
    let reason = tungstenite::http::StatusCode::from_u16(status)
        .ok()
        .and_then(|s| s.canonical_reason())
        .unwrap_or("");
    buf.extend_from_slice(reason.as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(lines);
    buf.extend_from_slice(extra);
    let no_body = status < 200 || status == 204 || status == 304;
    match len {
        Len::Exact(n) if !no_body => {
            let _ = write!(buf, "content-length: {n}\r\n");
        }
        Len::Chunked if !no_body => buf.extend_from_slice(b"transfer-encoding: chunked\r\n"),
        _ => {}
    }
    buf.extend_from_slice(date);
    if w.close && !w.said {
        buf.extend_from_slice(b"connection: close\r\n");
    } else if w.http10 && !w.close {
        buf.extend_from_slice(b"connection: keep-alive\r\n");
    }
    buf.extend_from_slice(b"\r\n");
}

/// Writes a whole answer with this body.
fn render(buf: &mut Vec<u8>, status: u16, lines: &[u8], body: &[u8], date: &[u8], w: Way) {
    render_head(buf, status, lines, b"", date, w, Len::Exact(body.len()));
    let no_body = status < 200 || status == 204 || status == 304;
    if !no_body && !w.head {
        buf.extend_from_slice(body);
    }
}

/// One part of a streamed body, chunk framed when `chunked`.
fn frame_part(buf: &mut Vec<u8>, part: &[u8], chunked: bool) {
    if part.is_empty() {
        return;
    }
    if chunked {
        let _ = write!(buf, "{:x}\r\n", part.len());
    }
    buf.extend_from_slice(part);
    if chunked {
        buf.extend_from_slice(b"\r\n");
    }
}

/// Moves `data` to Dart without a copy: the 24 byte body of an external
/// record (address, token, length). `dox_free_body(token)` frees it.
fn external(data: Vec<u8>) -> [u8; 24] {
    let len = data.len() as u64;
    let boxed = Box::new(data);
    let addr = boxed.as_ptr() as u64;
    let token = Box::into_raw(boxed) as u64;
    let mut out = [0u8; 24];
    out[..8].copy_from_slice(&addr.to_le_bytes());
    out[8..16].copy_from_slice(&token.to_le_bytes());
    out[16..].copy_from_slice(&len.to_le_bytes());
    out
}

/// Frees the external body of a record Dart never got.
fn free_external(rec: &[u8]) {
    let flags = rd32(rec, 8);
    if flags & 4 != 0 {
        let at = REQ_HEAD + rd16(rec, 12) as usize + rd16(rec, 14) as usize + rd32(rec, 16);
        let token = u64::from_le_bytes(rec[at + 8..at + 16].try_into().unwrap());
        drop(unsafe { Box::from_raw(token as *mut Vec<u8>) });
    }
}

/// The length of the record at the start of `b`.
fn record_len(b: &[u8]) -> usize {
    REQ_HEAD + rd16(b, 12) as usize + rd16(b, 14) as usize + rd32(b, 16) + rd32(b, 20)
}

/// Writes a record head (no method, target, headers or body).
#[allow(clippy::too_many_arguments)]
fn write_head(
    b: &mut [u8],
    id: u64,
    flags: u32,
    mlen: usize,
    tlen: usize,
    hlen: usize,
    blen: usize,
    port: [u8; 2],
    ip: &[u8],
) {
    b[0..8].copy_from_slice(&id.to_le_bytes());
    b[8..12].copy_from_slice(&flags.to_le_bytes());
    b[12..14].copy_from_slice(&(mlen as u16).to_le_bytes());
    b[14..16].copy_from_slice(&(tlen as u16).to_le_bytes());
    b[16..20].copy_from_slice(&(hlen as u32).to_le_bytes());
    b[20..24].copy_from_slice(&(blen as u32).to_le_bytes());
    b[24..26].copy_from_slice(&port);
    b[26..28].copy_from_slice(&[0, 0]);
    b[28..44].copy_from_slice(ip);
}

/// Messages for Dart that are not requests (WebSocket and stream), in
/// record format. They go to Dart at the end of the next `dox_run`.
#[derive(Default)]
struct Msgs {
    buf: Vec<u8>,
    /// client message bytes given to Dart and not acknowledged yet
    ws_unacked: usize,
}

impl Msgs {
    fn push(&mut self, id: u64, kind: u32, code: u16, data: &[u8]) {
        let ext = data.len() >= EXTERNAL_MIN;
        let body: &[u8] = if ext { &external(data.to_vec()) } else { data };
        let at = self.buf.len();
        self.buf.resize(at + REQ_HEAD, 0);
        let flags = (kind << 8) | (u32::from(ext) << 2);
        write_head(
            &mut self.buf[at..],
            id,
            flags,
            0,
            0,
            0,
            body.len(),
            code.to_le_bytes(),
            &[0; 16],
        );
        self.buf.extend_from_slice(body);
    }

    /// A WebSocket message or close for Dart.
    fn ws(&mut self, id: u64, kind: u32, code: u16, data: &[u8]) {
        if kind != WS_CLOSED {
            self.ws_unacked += data.len();
        }
        self.push(id, kind, code, data);
    }
}

impl Drop for Msgs {
    fn drop(&mut self) {
        let mut p = 0;
        while p + REQ_HEAD <= self.buf.len() {
            free_external(&self.buf[p..]);
            p += record_len(&self.buf[p..]);
        }
    }
}

/// Reads from the socket, writes to the write buffer: tungstenite never
/// waits on a write, Rust writes the buffer itself.
struct WsIo<'a> {
    sock: &'a TcpStream,
    out: &'a mut Vec<u8>,
    /// bytes it may still read in this turn
    left: usize,
}

impl Read for WsIo<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.left == 0 {
            // the share of this turn is used up: the caller queues the
            // socket again
            return Err(ErrorKind::WouldBlock.into());
        }
        let max = buf.len().min(self.left);
        let mut sock = self.sock;
        let k = sock.read(&mut buf[..max])?;
        self.left -= k;
        Ok(k)
    }
}

impl Write for WsIo<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// (code, reason) of a close frame. No frame is 1005 (no status), like dart:io.
fn close_parts(frame: Option<CloseFrame>) -> (u16, Vec<u8>) {
    frame.map_or((1005, Vec::new()), |f| {
        (u16::from(f.code), f.reason.as_bytes().to_vec())
    })
}

fn close_frame(code: CloseCode) -> Option<CloseFrame> {
    Some(CloseFrame {
        code,
        reason: Utf8Bytes::from_static(""),
    })
}

impl Ws {
    fn new(id: u64, max: usize) -> Self {
        Ws {
            ctx: WebSocketContext::new(Role::Server, None),
            id,
            max,
            queue: VecDeque::new(),
            bytes: 0,
            cap: max.saturating_mul(4).max(MIN_QUEUE),
            live: false,
            code: None,
            told: false,
            closing: None,
            paused: false,
        }
    }

    /// Queues Dart's close with `code` (eg. 1001 on a shutdown).
    fn close(&mut self, code: CloseCode) {
        if self.closing.is_none() && !self.told {
            self.queue.push_back(Message::Close(close_frame(code)));
        }
    }
}

impl Conn {
    fn new(sock: TcpStream, peer: [u8; 18], now: Instant) -> Self {
        Conn {
            sock,
            peer,
            rbuf: vec![0; READ_ROOM],
            rpos: 0,
            rlen: 0,
            wbuf: Vec::new(),
            wpos: 0,
            written: 0,
            marks: VecDeque::new(),
            slots: VecDeque::new(),
            body: None,
            ws: None,
            queued: false,
            dirty: false,
            closing: false,
            upgrading: false,
            paused: false,
            switch: false,
            shut: false,
            eof: false,
            linger: None,
            since: now,
            timer: None,
            stuck: None,
        }
    }

    fn pending(&self) -> usize {
        self.wbuf.len() - self.wpos
    }

    fn live_ws(&self) -> bool {
        self.ws.as_ref().is_some_and(|w| w.live)
    }

    /// true when it may read and parse more requests now
    fn can_parse(&self) -> bool {
        self.slots.len() < MAX_INFLIGHT && !self.upgrading && self.pending() < MAX_PENDING
    }

    /// Reads what the socket has. False on a socket error.
    fn read(&mut self) -> bool {
        loop {
            if self.rpos == self.rlen {
                self.rpos = 0;
                self.rlen = 0;
                if self.rbuf.len() > 4 * READ_ROOM {
                    self.rbuf = vec![0; READ_ROOM]; // do not keep a big buffer
                }
            }
            if self.rbuf.len() - self.rlen < READ_ROOM {
                if self.rpos > 0 {
                    self.rbuf.copy_within(self.rpos..self.rlen, 0);
                    self.rlen -= self.rpos;
                    self.rpos = 0;
                }
                if self.rbuf.len() - self.rlen < READ_ROOM {
                    if self.rlen > MAX_HEAD + READ_ROOM {
                        // many pipelined requests: parse first, then read on
                        self.paused = true;
                        return true;
                    }
                    let size = (self.rbuf.len() * 2).max(self.rlen + READ_ROOM);
                    self.rbuf.resize(size, 0);
                }
            }
            match (&self.sock).read(&mut self.rbuf[self.rlen..]) {
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
        while self.wpos < self.wbuf.len() {
            match (&self.sock).write(&self.wbuf[self.wpos..]) {
                Ok(0) => return false,
                Ok(k) => {
                    self.wpos += k;
                    self.written += k as u64;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => return false,
            }
        }
        if self.wpos == self.wbuf.len() {
            self.wpos = 0;
            if self.wbuf.capacity() > BIG_BUF {
                self.wbuf = Vec::new(); // do not keep a big buffer forever
            } else {
                self.wbuf.clear();
            }
        } else if self.wpos > BIG_BUF && self.wpos * 2 > self.wbuf.len() {
            self.wbuf.drain(..self.wpos);
            self.wpos = 0;
        }
        true
    }

    /// True when the connection can close now.
    fn finished(&self) -> bool {
        self.pending() == 0
            && !self.live_ws()
            && self.eof
            && self.slots.is_empty()
            && self.body.is_none()
            && !self.queued
    }

    /// Adds bytes of the answer of slot `i`. `body` of them are bytes of
    /// a streamed body (told to Dart once written).
    fn put(&mut self, i: usize, data: &[u8], body: u64) {
        if i == 0 {
            self.wbuf.extend_from_slice(data);
            self.mark(body);
        } else {
            let s = &mut self.slots[i];
            s.out.extend_from_slice(data);
            s.unsent += body;
        }
    }

    /// The last `n` bytes of `wbuf` are stream bytes of the front slot.
    fn mark(&mut self, n: u64) {
        if n > 0
            && let Some((sid, _)) = self.slots.front().and_then(|s| s.stream)
        {
            let end = self.written + self.pending() as u64;
            self.marks.push_back((end, sid, n));
        }
    }

    /// Moves the answers that are next in order to `wbuf`.
    fn pump(&mut self) {
        while let Some(front) = self.slots.front_mut() {
            if !front.out.is_empty() {
                let out = std::mem::take(&mut front.out);
                let n = std::mem::take(&mut front.unsent);
                if self.pending() == 0 {
                    self.wbuf = out;
                    self.wpos = 0;
                } else {
                    self.wbuf.extend_from_slice(&out);
                }
                self.mark(n);
            }
            let Some(front) = self.slots.front() else {
                break;
            };
            if !front.done {
                break;
            }
            let s = self.slots.pop_front().unwrap();
            if s.close {
                self.shut = true;
                break;
            }
            if s.upgrade {
                self.switch = true;
                break;
            }
        }
        if self.slots.is_empty() {
            self.upgrading = false;
            if self.closing && self.body.is_none() && self.ws.is_none() {
                self.shut = true;
            }
        }
    }

    /// Answers a bad request from Rust and closes after it.
    fn fail(&mut self, status: u16, date: &[u8]) {
        self.body = None;
        let mut out = Vec::new();
        let w = Way {
            http10: false,
            head: false,
            close: true,
            said: false,
        };
        render(&mut out, status, b"", b"", date, w);
        self.slots.push_back(Slot {
            out,
            done: true,
            answered: true,
            close: true,
            ..Slot::default()
        });
        self.closing = true;
        self.pump();
    }

    /// Takes the body bytes of the read buffer.
    fn feed(&mut self) -> Feed {
        let Some(b) = self.body.as_mut() else {
            return Feed::More;
        };
        if b.whole {
            return Feed::Whole;
        }
        loop {
            let avail = &self.rbuf[self.rpos..self.rlen];
            match b.chunk {
                None | Some(Chunk::Data(_)) => {
                    let want = match b.chunk {
                        Some(Chunk::Data(n)) => n,
                        _ => b.left,
                    };
                    let k = want.min(avail.len());
                    if k > 0 {
                        b.seen += k;
                        if b.seen > b.max {
                            if b.seen > b.max.saturating_add(DRAIN_MAX) {
                                return Feed::Fail(413);
                            }
                            b.too_big = true;
                        }
                        if !b.too_big {
                            b.data.extend_from_slice(&avail[..k]);
                        }
                        self.rpos += k;
                    }
                    match b.chunk {
                        Some(Chunk::Data(n)) if n == k => b.chunk = Some(Chunk::DataEnd),
                        Some(Chunk::Data(n)) => {
                            b.chunk = Some(Chunk::Data(n - k));
                            return Feed::More;
                        }
                        _ => {
                            b.left -= k;
                            if b.left > 0 {
                                return Feed::More;
                            }
                            break;
                        }
                    }
                }
                Some(Chunk::Size) => match httparse::parse_chunk_size(avail) {
                    Ok(httparse::Status::Complete((used, size))) => {
                        self.rpos += used;
                        b.chunk = Some(if size == 0 {
                            Chunk::Trailer
                        } else {
                            match usize::try_from(size) {
                                Ok(n) => Chunk::Data(n),
                                Err(_) => return Feed::Fail(413),
                            }
                        });
                    }
                    Ok(httparse::Status::Partial) if avail.len() < 1024 => return Feed::More,
                    _ => return Feed::Fail(400),
                },
                Some(Chunk::DataEnd) => {
                    if avail.len() < 2 {
                        return Feed::More;
                    }
                    if &avail[..2] != b"\r\n" {
                        return Feed::Fail(400);
                    }
                    self.rpos += 2;
                    b.chunk = Some(Chunk::Size);
                }
                Some(Chunk::Trailer) => {
                    // no trailer: an empty line. Else lines up to an empty one.
                    if avail.len() >= 2 && &avail[..2] == b"\r\n" {
                        self.rpos += 2;
                        break;
                    }
                    match avail.windows(4).position(|w| w == b"\r\n\r\n") {
                        Some(at) => {
                            self.rpos += at + 4;
                            break;
                        }
                        None if avail.len() > MAX_HEAD => return Feed::Fail(431),
                        None => return Feed::More,
                    }
                }
            }
        }
        if b.too_big {
            return Feed::Fail(413);
        }
        b.whole = true;
        Feed::Whole
    }

    /// Parses the complete requests of the read buffer into `inb` from
    /// `*n`. Err(size): the next record needs `size` bytes, more than the
    /// room left.
    #[allow(clippy::too_many_arguments)]
    fn parse(
        &mut self,
        idx: usize,
        inb: &mut [u8],
        n: &mut usize,
        seq: &mut u32,
        lim: &Limits,
        date: &[u8],
        now: Instant,
    ) -> Result<(), usize> {
        loop {
            if self.body.is_some() {
                let before = self.rpos;
                match self.feed() {
                    Feed::More => {
                        if self.rpos > before {
                            self.body.as_mut().unwrap().last = now;
                        }
                        break;
                    }
                    Feed::Fail(status) => {
                        self.fail(status, date);
                        break;
                    }
                    Feed::Whole => {
                        let b = self.body.as_mut().unwrap();
                        let ext = b.data.len() >= EXTERNAL_MIN;
                        let size = b.rec.len() + if ext { 24 } else { b.data.len() };
                        if inb.len() - *n < size {
                            return Err(size);
                        }
                        let mut b = self.body.take().unwrap();
                        let id = ((idx as u64) << 32) | u64::from(*seq);
                        let out = &mut inb[*n..*n + size];
                        let rl = b.rec.len();
                        out[..rl].copy_from_slice(&b.rec);
                        out[0..8].copy_from_slice(&id.to_le_bytes());
                        if ext {
                            let flags = rd32(out, 8) as u32 | 4;
                            out[8..12].copy_from_slice(&flags.to_le_bytes());
                            out[20..24].copy_from_slice(&24u32.to_le_bytes());
                            out[rl..].copy_from_slice(&external(std::mem::take(&mut b.data)));
                        } else {
                            out[20..24].copy_from_slice(&(b.data.len() as u32).to_le_bytes());
                            out[rl..].copy_from_slice(&b.data);
                        }
                        *n += size;
                        let mut slot = std::mem::take(&mut b.slot);
                        slot.seq = *seq;
                        *seq = seq.wrapping_add(1);
                        self.closing |= slot.close;
                        self.slots.push_back(slot);
                        self.since = now;
                    }
                }
                continue;
            }
            if self.closing || self.rpos >= self.rlen {
                break;
            }
            if !self.can_parse() {
                self.paused = true;
                break;
            }
            let id = ((idx as u64) << 32) | u64::from(*seq);
            let step = parse_one(
                &self.rbuf[self.rpos..self.rlen],
                id,
                &self.peer,
                inb,
                *n,
                lim,
                now,
            );
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
                    self.upgrading = slot.key.is_some();
                    self.slots.push_back(slot);
                    self.since = now;
                }
                Step::Body(used, body, go_on) => {
                    self.rpos += used;
                    self.body = Some(body);
                    if go_on && self.slots.is_empty() {
                        // ponytail: only when no earlier answer is due, so
                        // it never goes in the middle of one. Else the
                        // client sends the body after its own wait.
                        self.wbuf
                            .extend_from_slice(b"HTTP/1.1 100 Continue\r\n\r\n");
                    }
                }
            }
        }
        if self.eof
            && !self.closing
            && self.body.is_none()
            && self.rpos < self.rlen
            && self.slots.is_empty()
        {
            // the client closed in the middle of a request
            self.rpos = self.rlen;
        }
        Ok(())
    }
}

/// A length: ASCII digits only, with spaces or tabs around. Not `+5`, not
/// `5, 5`, not other white space.
fn parse_len(v: &[u8]) -> Option<u64> {
    let v = v.trim_ascii();
    if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
        return None;
    }
    v.iter().try_fold(0u64, |n, &d| {
        n.checked_mul(10)?.checked_add(u64::from(d - b'0'))
    })
}

/// The `content-length` of Dart's header lines: None when there is none,
/// Some(None) when it is not a number.
fn length_line(lines: &[u8]) -> Option<Option<u64>> {
    lines.split(|&c| c == b'\n').find_map(|line| {
        let at = line.iter().position(|&c| c == b':')?;
        line[..at]
            .trim_ascii()
            .eq_ignore_ascii_case(b"content-length")
            .then(|| parse_len(&line[at + 1..]))
    })
}

fn has_token(value: &[u8], token: &str) -> bool {
    value
        .split(|&c| c == b',')
        .any(|t| t.trim_ascii().eq_ignore_ascii_case(token.as_bytes()))
}

/// Parses one request from `data` and writes its record to `inb[at..]`.
fn parse_one(
    data: &[u8],
    id: u64,
    peer: &[u8; 18],
    inb: &mut [u8],
    at: usize,
    lim: &Limits,
    now: Instant,
) -> Step {
    let mut hs = [const { MaybeUninit::<httparse::Header>::uninit() }; MAX_HEADERS];
    let mut req = httparse::Request::new(&mut []);
    let head = match req.parse_with_uninit_headers(data, &mut hs) {
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
    let mut clen: Option<usize> = None;
    let mut chunked = false;
    let mut hlen = 0usize;
    let mut expect = false;
    let mut form = false;
    let (mut conn_up, mut up_ws, mut v13) = (false, false, false);
    let mut key: Option<&[u8]> = None;
    for h in req.headers.iter() {
        hlen += 4 + h.name.len() + h.value.len();
        let name = h.name.as_bytes();
        // the first byte picks the few names that matter, so most
        // headers cost one compare
        match name.first().map(u8::to_ascii_lowercase) {
            Some(b'c') => {
                if name.eq_ignore_ascii_case(b"content-length") {
                    match std::str::from_utf8(h.value)
                        .ok()
                        .and_then(|v| v.trim().parse().ok())
                    {
                        Some(v) if clen.is_none_or(|c| c == v) => clen = Some(v),
                        _ => return Step::Fail(400),
                    }
                } else if name.eq_ignore_ascii_case(b"connection") {
                    if has_token(h.value, "close") {
                        close = true;
                    } else if has_token(h.value, "keep-alive") {
                        close = false;
                    }
                    conn_up |= has_token(h.value, "upgrade");
                } else if name.eq_ignore_ascii_case(b"content-type") {
                    let mime = h.value.split(|&c| c == b';').next().unwrap_or(b"");
                    form = mime
                        .windows(9)
                        .any(|w| w.eq_ignore_ascii_case(b"form-data"));
                }
            }
            Some(b't') if name.eq_ignore_ascii_case(b"transfer-encoding") => {
                // only chunked is known. Anything else can not be framed.
                if !h.value.trim_ascii().eq_ignore_ascii_case(b"chunked") {
                    return Step::Fail(501);
                }
                chunked = true;
            }
            Some(b'e') if name.eq_ignore_ascii_case(b"expect") => {
                expect = h.value.trim_ascii().eq_ignore_ascii_case(b"100-continue");
            }
            Some(b'u') if name.eq_ignore_ascii_case(b"upgrade") => {
                up_ws = has_token(h.value, "websocket")
            }
            Some(b's') if name.eq_ignore_ascii_case(b"sec-websocket-version") => {
                v13 = h.value.trim_ascii() == b"13"
            }
            Some(b's') if name.eq_ignore_ascii_case(b"sec-websocket-key") => {
                key = Some(h.value.trim_ascii())
            }
            _ => {}
        }
    }
    if chunked && clen.is_some() {
        return Step::Fail(400); // two lengths: a request smuggling try
    }
    let method = req.method.unwrap_or("").as_bytes();
    let target = req.path.unwrap_or("").as_bytes();
    let clen = clen.unwrap_or(0);
    let has_body = chunked || clen > 0;
    let upgrade =
        method == b"GET" && !http10 && !has_body && conn_up && up_ws && v13 && key.is_some();
    let flags = u32::from(http10) | (u32::from(upgrade) << 1);
    let slot = Slot {
        close,
        head: method == b"HEAD",
        http10,
        key: if upgrade { key.map(Box::from) } else { None },
        ..Slot::default()
    };
    let max = if form { lim.multipart } else { lim.body };
    if clen > max && (expect || clen > max.saturating_add(DRAIN_MAX)) {
        // too big, and the client waits for our go before it sends the
        // body: say no at once
        return Step::Fail(413);
    }
    let whole = !chunked && data.len() >= head + clen;
    if whole && clen < EXTERNAL_MIN && clen <= max {
        // the common case: the whole request is here, write its record
        let size = REQ_HEAD + method.len() + target.len() + hlen + clen;
        if inb.len() - at < size {
            return Step::NoRoom(size);
        }
        let b = &mut inb[at..at + size];
        write_head(
            b,
            id,
            flags,
            method.len(),
            target.len(),
            hlen,
            clen,
            [peer[16], peer[17]],
            &peer[..16],
        );
        let p = write_names(&mut b[REQ_HEAD..], method, target, req.headers);
        b[REQ_HEAD + p..].copy_from_slice(&data[head..head + clen]);
        return Step::Done(head + clen, slot);
    }
    // the body comes apart: keep the record head until it is whole
    let size = REQ_HEAD + method.len() + target.len() + hlen;
    let mut rec = vec![0u8; size];
    write_head(
        &mut rec,
        0,
        flags,
        method.len(),
        target.len(),
        hlen,
        0,
        [peer[16], peer[17]],
        &peer[..16],
    );
    write_names(&mut rec[REQ_HEAD..], method, target, req.headers);
    let body = BodyRead {
        rec,
        data: Vec::with_capacity(if chunked {
            0
        } else {
            clen.min(MAX_RESERVE).min(max)
        }),
        left: clen,
        chunk: chunked.then_some(Chunk::Size),
        max,
        seen: 0,
        too_big: false,
        whole: false,
        last: now,
        slot,
    };
    let more = data.len() < head + clen || chunked;
    Step::Body(head, Box::new(body), expect && more)
}

/// Writes method, target and headers. Returns the bytes written.
fn write_names(b: &mut [u8], method: &[u8], target: &[u8], headers: &[httparse::Header]) -> usize {
    let mut p = 0;
    b[p..p + method.len()].copy_from_slice(method);
    p += method.len();
    b[p..p + target.len()].copy_from_slice(target);
    p += target.len();
    for h in headers {
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
    p
}

/// A gzip job: the answer of one request, zipped and rendered off the
/// isolate's thread.
struct Job {
    idx: usize,
    seq: u32,
    status: u16,
    lines: Vec<u8>,
    body: Vec<u8>,
    date: Vec<u8>,
    way: Way,
}

/// A done gzip job: the rendered answer.
type Zipped = (usize, u32, Vec<u8>);

/// All socket state of one server. Only the owner of the mutex touches it.
struct Core {
    poll: Poll,
    events: Events,
    listener: Option<TcpListener>,
    key: Key,
    /// the listener is given back (see `unbind`)
    unbound: bool,
    conns: Slab<Conn>,
    /// connections that may have bytes to read or requests to parse
    ready: VecDeque<usize>,
    /// the listener may have connections to accept
    accept: bool,
    /// accept again at this time (after an accept error)
    accept_at: Option<Instant>,
    /// the idle helper watches the sockets
    armed: bool,
    seq: u32,
    limits: Limits,
    /// (second, `date: ...\r\n`)
    date: (u64, Vec<u8>),
    now: Instant,
    /// (time, connection): one live entry per connection, see `Conn::timer`
    timers: BinaryHeap<Reverse<(Instant, usize)>>,
    msgs: Msgs,
    /// open streamed bodies: stream id to (connection, request seq)
    streams: HashMap<u64, (usize, u32)>,
    /// WebSockets: ws id to connection
    wss: HashMap<u64, usize>,
    /// WebSockets that stopped reading until Dart acknowledges
    ws_paused: Vec<usize>,
    /// a graceful shutdown runs
    draining: bool,
    /// Dart got the "all closed" byte
    done_sent: bool,
    /// the server stops: no lingering close
    stopped: bool,
    waker: Arc<Waker>,
    zip: Option<mpsc::Sender<Job>>,
    zipped: Arc<Mutex<Vec<Zipped>>>,
    /// lists reused per call
    dead: Vec<usize>,
    touched: Vec<usize>,
}

impl Core {
    fn new(listener: std::net::TcpListener, key: Key, limits: Limits) -> std::io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKER)?);
        let mut listener = TcpListener::from_std(listener);
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)?;
        Ok(Core {
            poll,
            events: Events::with_capacity(1024),
            listener: Some(listener),
            key,
            unbound: false,
            conns: Slab::with_capacity(1024),
            ready: VecDeque::new(),
            accept: true,
            accept_at: None,
            armed: true,
            seq: 0,
            limits,
            date: (0, Vec::new()),
            now: Instant::now(),
            timers: BinaryHeap::new(),
            msgs: Msgs::default(),
            streams: HashMap::new(),
            wss: HashMap::new(),
            ws_paused: Vec::new(),
            draining: false,
            done_sent: false,
            stopped: false,
            waker,
            zip: None,
            zipped: Arc::new(Mutex::new(Vec::new())),
            dead: Vec::new(),
            touched: Vec::new(),
        })
    }

    fn update_clock(&mut self) {
        self.now = Instant::now();
        let now = SystemTime::now();
        let sec = now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
        if sec != self.date.0 || self.date.1.is_empty() {
            self.date = (
                sec,
                format!("date: {}\r\n", httpdate::fmt_http_date(now)).into_bytes(),
            );
        }
    }

    /// Gives the listening socket back: no new connections.
    fn close_listener(&mut self) {
        if let Some(mut l) = self.listener.take() {
            let _ = self.poll.registry().deregister(&mut l);
        }
        self.accept = false;
        self.accept_at = None;
        if !self.unbound {
            self.unbound = true;
            unbind(self.key);
        }
    }

    fn queue(&mut self, idx: usize) {
        if let Some(c) = self.conns.get_mut(idx)
            && !c.queued
        {
            c.queued = true;
            self.ready.push_back(idx);
        }
    }

    fn touch(&mut self, idx: usize) {
        if let Some(c) = self.conns.get_mut(idx)
            && !c.dirty
        {
            c.dirty = true;
            self.touched.push(idx);
        }
    }

    /// Closes a connection now. Tells Dart about its streams and WebSocket.
    fn kill(&mut self, idx: usize) {
        let Some(mut c) = self.conns.try_remove(idx) else {
            return;
        };
        let _ = self.poll.registry().deregister(&mut c.sock);
        for s in &c.slots {
            if let Some((sid, _)) = s.stream
                && self.streams.remove(&sid).is_some()
            {
                self.msgs.push(sid, STREAM_GONE, 0, b"");
            }
        }
        if let Some(ws) = c.ws {
            self.wss.remove(&ws.id);
            if !ws.told {
                let (code, reason) = ws.code.unwrap_or((1006, Vec::new()));
                self.msgs.ws(ws.id, WS_CLOSED, code, &reason);
            }
        }
    }

    fn kill_dead(&mut self) {
        while let Some(idx) = self.dead.pop() {
            self.kill(idx);
        }
    }

    /// Makes sure the timer of `idx` fires by its next due time.
    fn arm_timer(&mut self, idx: usize) {
        let timeout = self.limits.timeout;
        let Some(c) = self.conns.get_mut(idx) else {
            return;
        };
        let at = c.due(timeout).map_or(self.now + HEAD_WAIT, |d| d.0);
        if c.timer.is_none_or(|t| at < t) {
            c.timer = Some(at);
            self.timers.push(Reverse((at, idx)));
        }
    }

    /// Runs the timers that are due.
    fn run_timers(&mut self) {
        let now = self.now;
        if self.accept_at.is_some_and(|t| t <= now) {
            self.accept_at = None;
            self.accept = true;
        }
        while let Some(&Reverse((t, idx))) = self.timers.peek() {
            if t > now {
                break;
            }
            self.timers.pop();
            let timeout = self.limits.timeout;
            let Some(c) = self.conns.get_mut(idx) else {
                continue;
            };
            if c.timer != Some(t) {
                continue; // an old entry
            }
            c.timer = None;
            match c.due(timeout) {
                Some((at, what)) if at <= now => match what {
                    Due::Body => {
                        c.fail(408, &self.date.1);
                        self.service(idx);
                    }
                    _ => self.kill(idx),
                },
                _ => {}
            }
            self.arm_timer(idx);
        }
    }

    /// The time of the next timer, if any.
    fn next_timer(&self) -> Option<Instant> {
        let t = self.timers.peek().map(|r| r.0.0);
        match (t, self.accept_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
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
        // taken out, so the loop can change the connections
        let events = std::mem::replace(&mut self.events, Events::with_capacity(0));
        for ev in events.iter() {
            any = true;
            match ev.token() {
                LISTENER => self.accept = true,
                WAKER => woke = true,
                Token(t) => {
                    let idx = t - 1;
                    let (writable, readable) = (
                        ev.is_writable(),
                        ev.is_readable() || ev.is_read_closed() || ev.is_error(),
                    );
                    let Some(c) = self.conns.get(idx) else {
                        continue;
                    };
                    let paused = c.paused || c.ws.as_ref().is_some_and(|w| w.paused);
                    if readable && !paused {
                        self.queue(idx);
                    }
                    if writable && self.conns[idx].pending() > 0 {
                        self.service(idx);
                    }
                }
            }
        }
        self.events = events;
        self.kill_dead();
        (any, woke)
    }

    fn accept_all(&mut self) {
        self.accept = false;
        loop {
            let Some(listener) = &self.listener else {
                return;
            };
            match listener.accept() {
                Ok((mut sock, addr)) => {
                    let _ = sock.set_nodelay(true);
                    let entry = self.conns.vacant_entry();
                    let idx = entry.key();
                    let interest = Interest::READABLE | Interest::WRITABLE;
                    if self
                        .poll
                        .registry()
                        .register(&mut sock, Token(idx + 1), interest)
                        .is_err()
                    {
                        continue;
                    }
                    let mut conn = Conn::new(sock, peer_bytes(addr), self.now);
                    conn.queued = true; // the request is often there already
                    entry.insert(conn);
                    self.ready.push_back(idx);
                    self.arm_timer(idx);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => {
                    // eg. too many open files. Try again soon, do not spin.
                    self.accept_at = Some(self.now + ACCEPT_RETRY);
                    return;
                }
            }
        }
    }

    /// After a change to a connection: switches to the WebSocket, hands
    /// queued messages to it, writes, starts the lingering close or
    /// closes it, and queues it again when it can read on.
    fn service(&mut self, idx: usize) {
        let now = self.now;
        let draining = self.draining;
        let Some(c) = self.conns.get_mut(idx) else {
            return;
        };
        if c.switch {
            c.switch = false;
            let ws = c.ws.as_mut().expect("accepted");
            let part = c.rbuf[c.rpos..c.rlen].to_vec();
            c.rpos = 0;
            c.rlen = 0;
            c.rbuf = Vec::new();
            let config = WebSocketConfig::default()
                .max_message_size(Some(ws.max))
                .max_frame_size(Some(ws.max))
                .read_buffer_size(READ_ROOM)
                .write_buffer_size(0);
            ws.ctx = WebSocketContext::from_partially_read(part, Role::Server, Some(config));
            ws.live = true;
            if draining {
                ws.close(CloseCode::Away);
            }
            c.paused = false;
            c.queued = true;
            self.ready.push_back(idx);
        }
        let c = &mut self.conns[idx];
        let mut close_started = false;
        if c.live_ws() {
            let Conn {
                ws,
                sock,
                wbuf,
                wpos,
                ..
            } = c;
            let ws = ws.as_mut().unwrap();
            while wbuf.len() - *wpos < WS_HOLD
                && ws.closing.is_none()
                && let Some(m) = ws.queue.pop_front()
            {
                ws.bytes = ws.bytes.saturating_sub(m.len());
                let mut io = WsIo {
                    sock,
                    out: &mut *wbuf,
                    left: 0,
                };
                if let Message::Close(frame) = m {
                    let _ = ws.ctx.close(&mut io, frame);
                    ws.closing = Some(now);
                    ws.queue.clear();
                    ws.bytes = 0;
                    close_started = true;
                } else {
                    let _ = ws.ctx.write(&mut io, m);
                }
            }
        }
        let before = c.written;
        if c.pending() > 0 && !c.flush() {
            self.kill(idx);
            return;
        }
        // a write starts to wait: make sure its timer runs
        let wait_starts = c.stuck.is_none() && c.pending() > 0;
        while let Some(&(end, sid, n)) = c.marks.front() {
            if end > c.written {
                break;
            }
            c.marks.pop_front();
            self.msgs.push(sid, STREAM_SENT, 0, &n.to_le_bytes());
        }
        if c.pending() == 0 {
            c.stuck = None;
            if c.written > before && c.slots.is_empty() {
                // the last answer is out: the idle time starts now
                c.since = now;
            }
        } else if c.written > before || c.stuck.is_none() {
            c.stuck = Some(now);
        }
        if c.pending() == 0 && c.live_ws() && !c.ws.as_ref().unwrap().queue.is_empty() {
            // tungstenite takes the next messages
            return self.service(idx);
        }
        let mut go_on = false;
        if c.paused && c.can_parse() && !c.closing && c.linger.is_none() && !c.live_ws() {
            c.paused = false;
            go_on = true;
        }
        let pending = c.pending();
        if let Some(ws) = c.ws.as_mut()
            && ws.paused
            && pending < WS_HOLD
            && self.msgs.ws_unacked <= WS_UNACKED
        {
            // the client read what waited: read its frames again
            ws.paused = false;
            go_on = true;
        }
        let mut linger = false;
        if c.pending() == 0 && c.shut && c.linger.is_none() {
            if self.stopped || c.ws.is_some() {
                self.kill(idx);
                return;
            }
            let _ = c.sock.shutdown(Shutdown::Write);
            c.linger = Some((now, 0));
            c.paused = false;
            linger = true;
            go_on = true;
        } else if c.finished() {
            self.kill(idx);
            return;
        }
        if go_on {
            self.queue(idx);
        }
        if linger || close_started || wait_starts {
            self.arm_timer(idx);
        }
    }

    /// Reads and drops what a closing client still sends.
    fn linger_read(&mut self, idx: usize) {
        let c = &mut self.conns[idx];
        let mut buf = [0u8; 16 * 1024];
        loop {
            match (&c.sock).read(&mut buf) {
                Ok(0) => return self.kill(idx),
                Ok(k) => {
                    let (_, dropped) = c.linger.as_mut().unwrap();
                    *dropped += k;
                    if *dropped > LINGER_MAX {
                        return self.kill(idx);
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => return self.kill(idx),
            }
        }
    }

    /// Reads WebSocket frames: messages go to Dart.
    fn ws_read(&mut self, idx: usize) {
        let c = &mut self.conns[idx];
        let Conn {
            ws,
            sock,
            wbuf,
            wpos,
            ..
        } = c;
        let ws = ws.as_mut().unwrap();
        let mut io = WsIo {
            sock,
            out: wbuf,
            left: WS_READ,
        };
        let mut again = false;
        let fail = loop {
            if self.msgs.ws_unacked > WS_UNACKED {
                // Dart is behind: read on after its ack
                ws.paused = true;
                self.ws_paused.push(idx);
                break None;
            }
            if io.out.len() - *wpos > WS_OUT_MAX {
                // the client does not read (eg. the pongs of its pings):
                // read on once they are written, see `service`
                ws.paused = true;
                break None;
            }
            match ws.ctx.read(&mut io) {
                Ok(Message::Text(t)) => self.msgs.ws(ws.id, WS_TEXT, 0, t.as_bytes()),
                Ok(Message::Binary(b)) => self.msgs.ws(ws.id, WS_BINARY, 0, &b),
                // the close of the peer (or its answer to ours).
                // tungstenite answers it.
                Ok(Message::Close(f)) => {
                    if ws.code.is_none() {
                        ws.code = Some(close_parts(f));
                    }
                }
                // tungstenite answers a ping itself
                Ok(_) => {}
                Err(WsError::Io(e)) if e.kind() == ErrorKind::WouldBlock => {
                    // the share of this turn is used up: more may wait
                    again = io.left == 0;
                    break None;
                }
                Err(WsError::Io(e)) if e.kind() == ErrorKind::Interrupted => {}
                // the close handshake is over
                Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => break Some(None),
                Err(WsError::Capacity(_)) => break Some(Some(CloseCode::Size)),
                Err(WsError::Utf8(_)) => break Some(Some(CloseCode::Invalid)),
                Err(WsError::Protocol(
                    tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
                )) => {
                    return self.kill(idx);
                }
                Err(WsError::Protocol(_)) => break Some(Some(CloseCode::Protocol)),
                Err(_) => return self.kill(idx),
            }
        };
        match fail {
            None => {}
            // over: close the TCP connection once the last frame is out
            Some(None) => c.shut = true,
            // the client broke the protocol: send `code` and close
            Some(Some(code)) => {
                let ws = c.ws.as_mut().unwrap();
                ws.code = Some((u16::from(code), Vec::new()));
                let mut io = WsIo {
                    sock: &c.sock,
                    out: &mut c.wbuf,
                    left: 0,
                };
                let _ = ws.ctx.close(&mut io, close_frame(code));
                c.shut = true;
            }
        }
        self.service(idx);
        if again {
            // read the rest in a later turn (see `fill`)
            self.queue(idx);
        }
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
            let lines = &out[h_at..body_at];
            let body = &out[body_at..p];
            match kind & 0xff {
                0 | 1 | 6 => self.answer(id, status, kind, lines, body),
                2..=4 => self.ws_send(id, kind, status, body),
                5 => {
                    let was = self.msgs.ws_unacked;
                    self.msgs.ws_unacked = was.saturating_sub(id as usize);
                    if was > WS_UNACKED && self.msgs.ws_unacked <= WS_UNACKED {
                        for idx in std::mem::take(&mut self.ws_paused) {
                            if let Some(ws) = self.conns.get_mut(idx).and_then(|c| c.ws.as_mut()) {
                                ws.paused = false;
                                self.queue(idx);
                            }
                        }
                    }
                }
                7 => self.part(id, body, false, true),
                8 => self.part(id, b"", true, status == 0),
                _ => {}
            }
        }
        while let Some(idx) = self.touched.pop() {
            if let Some(c) = self.conns.get_mut(idx) {
                c.dirty = false;
                c.pump();
                self.service(idx);
            }
        }
        self.kill_dead();
    }

    /// An HTTP answer for a request: kind 0 (whole), 1 (WebSocket accept)
    /// or 6 (streamed).
    fn answer(&mut self, id: u64, status: u16, kind: u16, lines: &[u8], body: &[u8]) {
        let idx = (id >> 32) as usize;
        let seq = id as u32;
        let date = &self.date.1;
        let found = self.conns.get_mut(idx).and_then(|c| {
            let first = c.slots.front()?.seq;
            let i = seq.wrapping_sub(first) as usize;
            let s = c.slots.get(i)?;
            (s.seq == seq && !s.answered).then_some(i)
        });
        let Some(i) = found else {
            // the client is gone (or a second answer)
            match kind & 0xff {
                1 if body.len() >= 8 => {
                    let ws = u64::from_le_bytes(body[..8].try_into().unwrap());
                    self.msgs.ws(ws, WS_CLOSED, 1006, b"");
                }
                6 if body.len() >= 8 => {
                    let sid = u64::from_le_bytes(body[..8].try_into().unwrap());
                    self.msgs.push(sid, STREAM_GONE, 0, b"");
                }
                _ => {}
            }
            return;
        };
        let c = &mut self.conns[idx];
        // the last answer of a closing connection (eg. a shutdown) says so
        let last = c.closing && i + 1 == c.slots.len() && c.body.is_none();
        let s = &mut c.slots[i];
        s.answered = true;
        s.close |= last && s.key.is_none();
        if kind & 0x200 != 0 {
            s.close = true;
            c.closing = true;
        }
        let mut way = Way::of(s);
        way.said = kind & 0x200 != 0;
        match kind & 0xff {
            0 if kind & 0x100 != 0 && !way.head && !body.is_empty() => {
                // gzip on the helper thread. The slot waits for it.
                let job = Job {
                    idx,
                    seq,
                    status,
                    lines: lines.to_vec(),
                    body: body.to_vec(),
                    date: date.clone(),
                    way,
                };
                self.zip(job);
                return;
            }
            0 => {
                s.done = true;
                if i == 0 {
                    render(&mut c.wbuf, status, lines, body, date, way);
                } else {
                    render(&mut s.out, status, lines, body, date, way);
                }
            }
            1 => {
                let (Some(key), true) = (s.key.take(), body.len() >= 16) else {
                    // Dart accepted a request that can not upgrade
                    s.done = true;
                    let mut out = Vec::new();
                    render(&mut out, 400, b"", b"", date, way);
                    c.put(i, &out, 0);
                    if body.len() >= 8 {
                        let ws = u64::from_le_bytes(body[..8].try_into().unwrap());
                        self.msgs.ws(ws, WS_CLOSED, 1006, b"");
                    }
                    self.touch(idx);
                    return;
                };
                s.done = true;
                s.upgrade = true;
                let ws_id = u64::from_le_bytes(body[..8].try_into().unwrap());
                let max = u64::from_le_bytes(body[8..16].try_into().unwrap());
                let max = max.min(u64::from(u32::MAX)) as usize;
                let accept = tungstenite::handshake::derive_accept_key(&key);
                let extra = format!(
                    "upgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: {accept}\r\n"
                );
                let mut out = Vec::new();
                render_head(&mut out, 101, lines, extra.as_bytes(), date, way, Len::None);
                c.put(i, &out, 0);
                c.ws = Some(Box::new(Ws::new(ws_id, max)));
                self.wss.insert(ws_id, idx);
            }
            _ => {
                if body.len() < 8 {
                    s.done = true;
                    return;
                }
                let sid = u64::from_le_bytes(body[..8].try_into().unwrap());
                let first = &body[8..];
                // Dart sends `content-length` itself when it knows it. Rust
                // counts the bytes: a wrong count closes the connection, so
                // the rest can not look like the next answer.
                let (len, tail) = match length_line(lines) {
                    _ if way.head => (Len::Open, Tail::Close),
                    Some(Some(n)) => (Len::Open, Tail::Left(n)),
                    sized => {
                        if sized.is_some() || way.http10 {
                            // a length Rust can not read, or HTTP/1.0: the
                            // end of the body is the close
                            way.close = true;
                            s.close = true;
                            (Len::Open, Tail::Close)
                        } else {
                            (Len::Chunked, Tail::Chunked)
                        }
                    }
                };
                let mut out = Vec::new();
                render_head(&mut out, status, lines, b"", date, way, len);
                if way.head {
                    // no body: tell Dart to stop
                    s.done = true;
                    c.put(i, &out, 0);
                    self.msgs.push(sid, STREAM_GONE, 0, b"");
                } else {
                    s.stream = Some((sid, tail));
                    c.put(i, &out, 0);
                    self.streams.insert(sid, (idx, seq));
                    self.touch(idx);
                    self.part(sid, first, false, true);
                }
            }
        }
        self.touch(idx);
    }

    /// A part (or the end) of the streamed body `sid`.
    fn part(&mut self, sid: u64, data: &[u8], end: bool, ok: bool) {
        let Some(&(idx, seq)) = self.streams.get(&sid) else {
            return;
        };
        let Some(c) = self.conns.get_mut(idx) else {
            return;
        };
        let Some(i) = c.slots.iter().position(|s| s.seq == seq) else {
            return;
        };
        let Some((_, tail)) = c.slots[i].stream else {
            return;
        };
        let (mut data, mut end, mut ok) = (data, end, ok);
        if let Tail::Left(left) = tail {
            let k = data.len() as u64;
            if k > left {
                // more bytes than `content-length`: send what it said, then
                // close. Dart stops.
                data = &data[..left as usize];
                (end, ok) = (true, false);
                self.msgs.push(sid, STREAM_GONE, 0, b"");
            } else if end && k < left {
                ok = false; // too few: the client sees the body is not whole
            }
            c.slots[i].stream = Some((sid, Tail::Left(left.saturating_sub(k))));
        }
        let chunked = tail == Tail::Chunked;
        let mut out = Vec::new();
        frame_part(&mut out, data, chunked);
        if end {
            self.streams.remove(&sid);
            let s = &mut c.slots[i];
            s.done = true;
            if !ok {
                // cut the connection: the client sees the body is not whole
                s.close = true;
            } else if chunked {
                out.extend_from_slice(b"0\r\n\r\n");
            }
        }
        c.put(i, &out, data.len() as u64);
        self.touch(idx);
    }

    /// A message (kind 2 text, 3 binary) or the close (kind 4) from Dart
    /// for the WebSocket `id`.
    fn ws_send(&mut self, id: u64, kind: u16, status: u16, body: &[u8]) {
        let Some(&idx) = self.wss.get(&id) else {
            return;
        };
        let Some(ws) = self.conns.get_mut(idx).and_then(|c| c.ws.as_mut()) else {
            return;
        };
        if ws.told || ws.closing.is_some() {
            return;
        }
        let m = match kind & 0xff {
            2 => match Utf8Bytes::try_from(body.to_vec()) {
                Ok(text) => Message::Text(text),
                Err(_) => return, // Dart always sends UTF-8
            },
            3 => Message::Binary(Bytes::copy_from_slice(body)),
            _ => {
                let frame = (status != 0).then(|| CloseFrame {
                    code: CloseCode::from(status),
                    reason: Utf8Bytes::try_from(body.to_vec())
                        .unwrap_or_else(|_| Utf8Bytes::from_static("")),
                });
                Message::Close(frame)
            }
        };
        if !matches!(m, Message::Close(_)) && ws.bytes + m.len() > ws.cap {
            // the client reads too slowly: close with 1008 and drop the queue
            ws.queue.clear();
            ws.bytes = 0;
            ws.code = Some((1008, Vec::new()));
            ws.told = true;
            self.msgs.ws(id, WS_CLOSED, 1008, b"");
            let c = &mut self.conns[idx];
            if c.live_ws() && c.pending() > 0 {
                // a send waits for a client that does not read: drop it
                self.dead.push(idx);
                return;
            }
            c.ws.as_mut()
                .unwrap()
                .queue
                .push_back(Message::Close(close_frame(CloseCode::Policy)));
            c.shut = true;
        } else {
            ws.bytes += m.len();
            ws.queue.push_back(m);
        }
        self.touch(idx);
    }

    /// Gives a gzip job to the helper thread of this server. Starts it on
    /// the first job.
    fn zip(&mut self, job: Job) {
        if self.zip.is_none() {
            let (tx, rx) = mpsc::channel::<Job>();
            let (results, waker) = (self.zipped.clone(), self.waker.clone());
            let started = std::thread::Builder::new()
                .name("dox-http-gzip".into())
                .spawn(move || {
                    // ends when the server drops the sender
                    for job in rx {
                        let zipped = gzip(&job.body);
                        let mut out = Vec::with_capacity(zipped.len() + 256);
                        render(
                            &mut out, job.status, &job.lines, &zipped, &job.date, job.way,
                        );
                        results.lock().unwrap().push((job.idx, job.seq, out));
                        let _ = waker.wake();
                    }
                });
            if started.is_err() {
                // no thread: zip here
                let mut out = Vec::new();
                render(
                    &mut out,
                    job.status,
                    &job.lines,
                    &gzip(&job.body),
                    &job.date,
                    job.way,
                );
                self.zipped.lock().unwrap().push((job.idx, job.seq, out));
                return;
            }
            self.zip = Some(tx);
        }
        let _ = self.zip.as_ref().unwrap().send(job);
    }

    /// Puts the done gzip jobs in their places.
    fn take_zipped(&mut self) {
        let done = std::mem::take(&mut *self.zipped.lock().unwrap());
        for (idx, seq, out) in done {
            let Some(c) = self.conns.get_mut(idx) else {
                continue;
            };
            let Some(i) = c.slots.iter().position(|s| s.seq == seq) else {
                continue;
            };
            c.slots[i].done = true;
            c.put(i, &out, 0);
            self.touch(idx);
        }
        while let Some(idx) = self.touched.pop() {
            if let Some(c) = self.conns.get_mut(idx) {
                c.dirty = false;
                c.pump();
                self.service(idx);
            }
        }
        self.kill_dead();
    }

    /// Accepts, reads and parses into `inb`, then adds the messages for
    /// Dart. Returns the bytes written, or Err(size) when one record is
    /// bigger than all of `inb`.
    fn fill(&mut self, inb: &mut [u8]) -> Result<usize, usize> {
        if self.accept {
            self.accept_all();
        }
        let mut n = 0;
        // each connection gets one turn per call. One queued again in this
        // call (eg. a WebSocket with more frames) waits for the next call,
        // so Dart gets the requests of the others first.
        let mut turns = self.ready.len();
        while turns > 0
            && let Some(&idx) = self.ready.front()
        {
            turns -= 1;
            let Some(c) = self.conns.get_mut(idx) else {
                self.ready.pop_front();
                continue;
            };
            if c.linger.is_some() {
                c.queued = false;
                self.ready.pop_front();
                self.linger_read(idx);
                continue;
            }
            if c.live_ws() {
                c.queued = false;
                self.ready.pop_front();
                self.ws_read(idx);
                continue;
            }
            if c.body.is_none() && (c.closing || !c.can_parse()) {
                c.queued = false;
                self.ready.pop_front();
                if !c.closing {
                    c.paused = true; // read on once answers went out
                    continue;
                }
                // a closing connection reads no more requests: drop them
                if !c.read() {
                    self.kill(idx);
                    continue;
                }
                c.rpos = c.rlen;
                if c.finished() {
                    self.kill(idx);
                }
                continue;
            }
            if !c.read() {
                self.ready.pop_front();
                self.kill(idx);
                continue;
            }
            let res = c.parse(
                idx,
                inb,
                &mut n,
                &mut self.seq,
                &self.limits,
                &self.date.1,
                self.now,
            );
            if let Err(size) = res {
                // stays in the queue for the next call
                if n == 0 {
                    return Err(size);
                }
                break;
            }
            let c = &mut self.conns[idx];
            c.queued = false;
            self.ready.pop_front();
            if c.body.as_ref().is_some_and(|b| b.last == self.now) {
                self.arm_timer(idx);
            }
            self.service(idx);
        }
        self.kill_dead();
        // the WebSocket and stream messages, whole records only
        let room = inb.len() - n;
        let mut take = 0;
        while take + REQ_HEAD <= self.msgs.buf.len() {
            let len = record_len(&self.msgs.buf[take..]);
            if take + len > room {
                break;
            }
            take += len;
        }
        if take == 0 && n == 0 && !self.msgs.buf.is_empty() {
            return Err(record_len(&self.msgs.buf));
        }
        inb[n..n + take].copy_from_slice(&self.msgs.buf[..take]);
        self.msgs.buf.drain(..take);
        Ok(n + take)
    }

    /// Starts a graceful shutdown: no new connections, each connection
    /// closes after its current answer, WebSockets get 1001.
    fn start_drain(&mut self) {
        self.draining = true;
        self.close_listener();
        let all: Vec<usize> = self.conns.iter().map(|(i, _)| i).collect();
        for idx in all {
            let c = &mut self.conns[idx];
            c.closing = true;
            if let Some(ws) = c.ws.as_mut() {
                if ws.live {
                    ws.close(CloseCode::Away);
                }
            } else if c.slots.is_empty() && c.body.is_none() {
                c.shut = true;
            }
            self.service(idx);
        }
        self.kill_dead();
    }

    /// true once after a graceful shutdown is over: all connections are
    /// closed (or only drop the last bytes of a lingering close) and Dart
    /// has all messages.
    fn done_due(&mut self) -> bool {
        if self.draining
            && !self.done_sent
            && self.msgs.buf.is_empty()
            && self.conns.iter().all(|(_, c)| c.linger.is_some())
        {
            self.done_sent = true;
            return true;
        }
        false
    }

    /// true when Dart has something to do
    fn has_work(&self) -> bool {
        !self.ready.is_empty() || self.accept || !self.msgs.buf.is_empty()
    }

    /// One turn for Dart: see `dox_run`.
    fn run(&mut self, out: &[u8], inb: &mut [u8], budget_us: i64) -> Result<usize, usize> {
        self.update_clock();
        self.take_zipped();
        if !out.is_empty() {
            self.respond(out);
        }
        self.run_timers();
        self.wait(Some(Duration::ZERO));
        let mut got = self.fill(inb);
        if got == Ok(0) && budget_us > 0 {
            let mut wait = Duration::from_micros(budget_us as u64);
            if !self.ready.is_empty() {
                wait = Duration::ZERO; // a connection waits for its next turn
            }
            if let Some(t) = self.next_timer() {
                wait = wait.min(t.saturating_duration_since(self.now));
            }
            self.wait(Some(wait));
            self.update_clock();
            self.run_timers();
            got = self.fill(inb);
        }
        got
    }
}

impl Conn {
    /// The next due time of this connection, and what is due then.
    fn due(&self, body_timeout: Duration) -> Option<(Instant, Due)> {
        if let Some((t, _)) = self.linger {
            return Some((t + LINGER, Due::Linger));
        }
        if let Some(t) = self.ws.as_ref().and_then(|w| w.closing) {
            return Some((t + CLOSE_WAIT, Due::Ws));
        }
        if let Some(b) = &self.body
            && !b.whole
        {
            return Some((b.last + body_timeout, Due::Body));
        }
        // a write makes no progress: the client does not read
        if let Some(t) = self.stuck {
            return Some((t + SEND_WAIT, Due::Send));
        }
        if self.ws.is_some() || self.body.is_some() {
            return None;
        }
        if self.slots.is_empty() && self.pending() == 0 {
            return Some((self.since + HEAD_WAIT, Due::Head));
        }
        None
    }
}

/// One server. Dart holds an `Arc` of it as handle.
struct Native {
    core: Mutex<Core>,
    waker: Arc<Waker>,
    /// Dart waits for the core: the helper gives it back
    want: AtomicBool,
    /// wakes the helper: Dart went idle
    kick: Mutex<bool>,
    cv: Condvar,
    stopped: AtomicBool,
    dart_port: i64,
    port: u16,
    helper: Mutex<Option<JoinHandle<()>>>,
}

impl Native {
    fn arm(&self) {
        *self.kick.lock().unwrap() = true;
        self.cv.notify_one();
    }

    /// Takes the core from the idle helper (it gives way at once).
    fn lock(&self) -> MutexGuard<'_, Core> {
        let core = match self.core.try_lock() {
            Ok(c) => c,
            Err(_) => {
                self.want.store(true, Ordering::Release);
                let _ = self.waker.wake();
                self.core.lock().unwrap()
            }
        };
        self.want.store(false, Ordering::Release);
        core
    }

    /// The isolate is gone and did not stop the server (eg. an uncaught
    /// error). Close the sockets: the port is free, and new clients get
    /// "connection refused" and not a wait.
    fn gone(&self, mut core: MutexGuard<'_, Core>) {
        self.stopped.store(true, Ordering::Release);
        core.stopped = true;
        core.close_listener();
        core.conns.clear();
        core.zip = None; // ends the gzip thread
    }

    /// The idle helper: watches the sockets while Dart is idle. Also checks
    /// every [PROBE] that the isolate is alive, idle or not.
    fn helper(&self) {
        let mut probed = Instant::now();
        loop {
            {
                let mut k = self.kick.lock().unwrap();
                while !*k && !self.stopped.load(Ordering::Acquire) {
                    k = self.cv.wait_timeout(k, PROBE).unwrap().0;
                    if !*k && probed.elapsed() >= PROBE {
                        // Dart owns the sockets. A dead isolate never
                        // calls `dox_run` again, so check it here too.
                        probed = Instant::now();
                        if !post(self.dart_port, &[]) {
                            drop(k);
                            return self.gone(self.core.lock().unwrap());
                        }
                    }
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
                core.update_clock();
                core.take_zipped();
                core.run_timers();
                if core.done_due() {
                    post(self.dart_port, &[2]);
                }
                if core.has_work() {
                    core.armed = false;
                    drop(core);
                    post(self.dart_port, &[1]);
                    break;
                }
                let mut wait = PROBE;
                if let Some(t) = core.next_timer() {
                    wait = wait.min(t.saturating_duration_since(core.now));
                }
                let (_, woke) = core.wait(Some(wait));
                if woke && self.want.load(Ordering::Acquire) {
                    break;
                }
                if probed.elapsed() >= PROBE {
                    probed = Instant::now();
                    if !post(self.dart_port, &[]) {
                        return self.gone(core);
                    }
                }
            }
        }
    }
}

// ---- FFI ---------------------------------------------------------------

/// Binds `port` and starts the idle helper, which posts to `send_port`.
/// `family`, `addr_hi` and `addr_lo` are the host, see [Key]. Family 0 is
/// all interfaces. Bodies over `max_body` bytes (`max_multipart` for a
/// multipart form) get 413. A body with no new bytes for
/// `body_timeout_ms` gets 408. Returns a handle (> 0), or < 0: -1 not
/// initialized, -2 no poller or thread, -(1000 + errno) when the bind
/// fails.
#[unsafe(no_mangle)]
pub extern "C" fn dox_bind(
    port: i32,
    send_port: i64,
    max_body: i64,
    max_multipart: i64,
    body_timeout_ms: i64,
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
    let limits = Limits {
        body: max_body.max(0) as usize,
        multipart: max_multipart.max(0) as usize,
        timeout: Duration::from_millis(body_timeout_ms.max(0) as u64),
    };
    let Ok(core) = Core::new(listener, key, limits) else {
        unbind(key);
        return -2;
    };
    let native = Arc::new(Native {
        waker: core.waker.clone(),
        core: Mutex::new(core),
        want: AtomicBool::new(false),
        kick: Mutex::new(true),
        cv: Condvar::new(),
        stopped: AtomicBool::new(false),
        dart_port: send_port,
        port: key.3,
        helper: Mutex::new(None),
    });
    let for_helper = native.clone();
    let Ok(t) = std::thread::Builder::new()
        .name("dox-http-idle".into())
        .spawn(move || for_helper.helper())
    else {
        native.core.lock().unwrap().close_listener();
        return -2;
    };
    *native.helper.lock().unwrap() = Some(t);
    Arc::into_raw(native) as i64
}

/// The port of a server (useful for port 0).
///
/// # Safety
/// `handle` must come from `dox_bind` and not be unbound.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_bound_port(handle: i64) -> i32 {
    let s = unsafe { &*(handle as *const Native) };
    i32::from(s.port)
}

/// Writes the answers in `out`, then reads new requests into `inb`.
/// Waits up to `budget_us` when there are none. Returns the bytes written
/// to `inb`. 0: nothing came. The idle helper now watches the sockets and
/// posts `[1]` to the Dart port when there is something. < 0: one record
/// needs a bigger `inb`, at least -n bytes (call again with a bigger one).
///
/// After `dox_shutdown`, Rust posts `[2]` to the port once all
/// connections are closed and Dart has all messages.
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
    let mut core = s.lock();
    core.armed = false;
    let out: &[u8] = if out_len > 0 {
        unsafe { std::slice::from_raw_parts(out, out_len as usize) }
    } else {
        &[]
    };
    let inb = unsafe { std::slice::from_raw_parts_mut(inb, in_cap.max(0) as usize) };
    let got = core.run(out, inb, budget_us);
    if core.done_due() {
        post(s.dart_port, &[2]);
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

/// Starts a graceful shutdown: the server stops accepting connections at
/// once. Each open connection closes after its current answer, each
/// WebSocket with 1001. When all are closed, Dart gets `[2]` on its port.
/// Then call `dox_unbind`.
///
/// # Safety
/// `handle` must come from `dox_bind` and not be unbound.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_shutdown(handle: i64) {
    let s = unsafe { &*(handle as *const Native) };
    let mut core = s.lock();
    core.update_clock();
    core.start_drain();
    if core.armed {
        // the helper watches on, and posts `[2]` or the messages
        drop(core);
        s.arm();
    }
}

/// Stops a server, closes its sockets and frees the handle.
///
/// # Safety
/// `handle` must come from `dox_bind`. Do not use it after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_unbind(handle: i64) {
    let s = unsafe { Arc::from_raw(handle as *const Native) };
    s.stopped.store(true, Ordering::Release);
    s.want.store(true, Ordering::Release);
    let _ = s.waker.wake();
    s.arm();
    let helper = s.helper.lock().unwrap().take();
    if let Some(t) = helper {
        let _ = t.join();
    }
    let mut core = s.core.lock().unwrap();
    core.stopped = true;
    core.close_listener();
    core.conns.clear();
    core.zip = None; // ends the gzip thread
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

/// The finalizer of an external body (see lib.rs): frees it.
///
/// # Safety
/// `token` must be the token of an external record, freed only once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_free_body(token: *mut c_void) {
    drop(unsafe { Box::from_raw(token as *mut Vec<u8>) });
}

#[cfg(test)]
mod tests;
