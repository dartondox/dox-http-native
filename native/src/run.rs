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
//! When a call finds nothing, it returns 0 and the idle helper takes over:
//! one small thread per server. It sleeps while Dart runs. Then it waits
//! until the poll file (epoll or kqueue) has an event, or until the next
//! timer is due. It never takes the `core` mutex for that: it only checks
//! that the poll file is readable, it does not read the events. So Dart
//! never waits for the helper. When there is something, the helper posts
//! one byte to the Dart port and goes back to sleep. Dart then calls
//! `dox_run` again, which reads the events and does the work. The flag
//! `armed` makes sure only one side tells Dart: the one that clears it.
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
//!   answer, and at most [MAX_PENDING] answer bytes may wait to be written.
//!   Then Rust stops reading that connection until answers go out.
//! - A write that makes no progress for [SEND_WAIT] closes the connection:
//!   the client does not read.
//! - Each connection gets one turn per `dox_run` call. A WebSocket reads at
//!   most [WS_READ] bytes in one turn, so one busy client can not hold the
//!   isolate.
//! - A body that is not whole in the first read is read apart (a length or
//!   chunked). Too big: 413. With `Expect: 100-continue` the 413 comes at
//!   once, else the body is read and dropped first (up to [DRAIN_MAX]), so
//!   the client sees the 413 and not a reset. No body bytes for the body
//!   timeout: 408. A request head must be whole in the idle timeout
//!   (Dart's `idleTimeout`, 30 seconds by default), and an idle
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

use crate::h2::{H2, PREFACE};
use crate::{
    BODY_END, CLOSE_WAIT, DRAIN_MAX, EXTERNAL_MIN, Key, MAX_HEAD, MAX_RESERVE, MIN_QUEUE, POST,
    REQ_HEAD, REQUEST_GONE, RES_HEAD, SEND_WAIT, STREAM_GONE, STREAM_SENT, WS_BINARY, WS_CLOSED,
    WS_TEXT, WS_UNACKED, bind, gzip, peer_bytes, post, rd16, rd32, unbind,
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
/// Lingering close: how long, and how many bytes, Rust drops.
const LINGER: Duration = Duration::from_secs(1);
const LINGER_MAX: usize = 1 << 20;
/// No traffic for this long and the OS checks that the client is still
/// there (TCP keep-alive, see `keepalive`).
const KEEPALIVE: Duration = Duration::from_secs(60);
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
/// Over this many bytes written, a write buffer drops what is written.
const BIG_BUF: usize = 1 << 20;
/// A write buffer bigger than this is freed once it is empty. Small
/// answers keep their buffer (no new one per answer), but a big answer
/// does not leave a big buffer on a keep-alive connection: with many
/// connections that adds up.
const KEEP_WBUF: usize = 64 * 1024;
/// The longest body or idle timeout (a year). Longer is cut to this.
const MAX_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 3600);
/// Most headers of one request. More gets 431.
pub(crate) const MAX_HEADERS: usize = 100;
/// most bytes of a chunk size line, with its extensions
const CHUNK_LINE: usize = 1024;

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

impl BodyRead {
    /// Takes the body bytes of `buf[*pos..]` and moves `*pos` past them.
    /// `buf` is the read buffer, so parts of it may come in many calls.
    fn feed(&mut self, buf: &[u8], pos: &mut usize) -> Feed {
        let b = self;
        if b.whole {
            return Feed::Whole;
        }
        loop {
            let avail = &buf[*pos..];
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
                        *pos += k;
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
                    // a longer line is refused also when it came whole, so
                    // the answer does not hang on how the bytes were split
                    Ok(httparse::Status::Complete((used, _))) if used > CHUNK_LINE => {
                        return Feed::Fail(400);
                    }
                    Ok(httparse::Status::Complete((used, size))) => {
                        *pos += used;
                        b.chunk = Some(if size == 0 {
                            Chunk::Trailer
                        } else {
                            match usize::try_from(size) {
                                Ok(n) => Chunk::Data(n),
                                Err(_) => return Feed::Fail(413),
                            }
                        });
                    }
                    Ok(httparse::Status::Partial) if avail.len() < CHUNK_LINE => return Feed::More,
                    _ => return Feed::Fail(400),
                },
                Some(Chunk::DataEnd) => {
                    if avail.len() < 2 {
                        return Feed::More;
                    }
                    if &avail[..2] != b"\r\n" {
                        return Feed::Fail(400);
                    }
                    *pos += 2;
                    b.chunk = Some(Chunk::Size);
                }
                Some(Chunk::Trailer) => {
                    // no trailer: an empty line. Else lines up to an empty one.
                    if avail.len() >= 2 && &avail[..2] == b"\r\n" {
                        *pos += 2;
                        break;
                    }
                    match avail.windows(4).position(|w| w == b"\r\n\r\n") {
                        // too long also when it came whole (see above)
                        Some(at) if at + 4 > MAX_HEAD => return Feed::Fail(431),
                        Some(at) => {
                            *pos += at + 4;
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
    /// `pingInterval` of Dart: send a ping when the client was quiet this
    /// long, close when it stays quiet one more interval. None: off.
    ping: Option<Duration>,
    /// when the client last sent bytes (or the ping timer started)
    heard: Instant,
    /// a ping went out at this time and the client was quiet since
    pinged: Option<Instant>,
}

struct Conn {
    sock: TcpStream,
    peer: [u8; 18],
    /// read bytes: `rpos..rlen` is not parsed yet. All of it is initialized.
    /// Empty while nothing waits: then a read uses the core's shared buffer
    /// (see `lend`).
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
    /// HTTP/2: all bytes go here (see h2/mod.rs)
    h2: Option<Box<H2>>,
    /// HTTP/2 is on and no bytes were parsed yet: the first ones pick
    /// HTTP/1.1 or HTTP/2
    sniff: bool,
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
    /// the poll saw the client close its side. A short read must then go on
    /// until the 0: on Linux the close comes in the same event as the last
    /// bytes, and no new event follows.
    hup: bool,
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
#[derive(Debug, PartialEq)]
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
    /// HTTP/2: a request body got no bytes for the body timeout
    H2Body,
    Head,
    Ws,
    Send,
    /// the client closed its side, answers wait and nothing was written
    /// for the idle time: it is gone
    Gone,
    /// a WebSocket ping is due
    Ping,
    /// the client did not answer the ping in time
    Pong,
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
    /// keep-alive: a connection with no request closes after this. A
    /// request head must also come whole in this time.
    idle: Duration,
    /// most open connections (0: no limit). At the limit Rust stops
    /// accepting until one closes. New clients wait in the listen queue.
    conns: usize,
    /// a connection that starts with the HTTP/2 preface speaks HTTP/2
    http2: bool,
    /// HTTP/2: most open streams of one connection
    max_streams: u32,
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

/// A status whose answer never has a body: 1xx, 204 and 304. A status
/// out of range is sent as 500, which has one.
fn no_body(status: u16) -> bool {
    (100..200).contains(&status) || status == 204 || status == 304
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

/// The size of a record whose body was read apart from its head (see
/// [put_body]).
fn body_size(rec: usize, body: usize) -> usize {
    rec + if body >= EXTERNAL_MIN { 24 } else { body }
}

/// Writes a record whose body was read apart: `rec` (head, method,
/// target, headers) with the id and the body. A big body goes to Dart
/// without a copy (see [external]). `out` is [body_size] bytes.
fn put_body(out: &mut [u8], rec: &[u8], body: Vec<u8>, id: u64) {
    let rl = rec.len();
    out[..rl].copy_from_slice(rec);
    out[0..8].copy_from_slice(&id.to_le_bytes());
    if body.len() >= EXTERNAL_MIN {
        let flags = rd32(out, 8) as u32 | 4;
        out[8..12].copy_from_slice(&flags.to_le_bytes());
        out[20..24].copy_from_slice(&24u32.to_le_bytes());
        out[rl..].copy_from_slice(&external(body));
    } else {
        out[20..24].copy_from_slice(&(body.len() as u32).to_le_bytes());
        out[rl..].copy_from_slice(&body);
    }
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
pub(crate) fn write_head(
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
    fn new(id: u64, max: usize, now: Instant) -> Self {
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
            ping: None,
            heard: now,
            pinged: None,
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
            // made by the first read: an idle connection holds no buffer
            rbuf: Vec::new(),
            rpos: 0,
            rlen: 0,
            wbuf: Vec::new(),
            wpos: 0,
            written: 0,
            marks: VecDeque::new(),
            slots: VecDeque::new(),
            body: None,
            ws: None,
            h2: None,
            sniff: false,
            queued: false,
            dirty: false,
            closing: false,
            upgrading: false,
            paused: false,
            switch: false,
            shut: false,
            eof: false,
            hup: false,
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

    /// true while a request waits for its answer (or its answer is not
    /// all sent, on HTTP/2)
    fn busy(&self) -> bool {
        !self.slots.is_empty() || self.h2.as_ref().is_some_and(|h| h.open() > 0)
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
                    if self.rlen < self.rbuf.len() && !self.hup {
                        return true;
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return true,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => return false,
            }
        }
    }

    /// Before a read. When no bytes wait, the read goes into `scratch`, the
    /// one read buffer of the core, not a buffer per connection: an idle
    /// keep-alive connection then holds no read buffer. True when lent.
    fn lend(&mut self, scratch: &mut Vec<u8>) -> bool {
        if self.rpos < self.rlen {
            return false;
        }
        self.rpos = 0;
        self.rlen = 0;
        self.rbuf = std::mem::take(scratch);
        true
    }

    /// After the parse. Gives `scratch` back to the core. Only bytes not
    /// parsed yet (a part of a request) are copied to a buffer of the
    /// connection's own, so a whole request is never copied here. A buffer
    /// of its own with nothing left in it is freed.
    fn give_back(&mut self, scratch: &mut Vec<u8>, lent: bool) {
        if self.rpos == self.rlen {
            self.rpos = 0;
            self.rlen = 0;
            let buf = std::mem::take(&mut self.rbuf);
            if lent {
                *scratch = buf;
            }
        } else if lent {
            let buf = std::mem::take(&mut self.rbuf);
            self.rbuf = buf[self.rpos..self.rlen].to_vec();
            self.rlen -= self.rpos;
            self.rpos = 0;
            *scratch = buf;
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
            if self.wbuf.capacity() > KEEP_WBUF {
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
            && !self.busy()
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

    /// HTTP/2 is on and this is the start of the connection: the client
    /// preface makes it HTTP/2, anything else HTTP/1.1. True while the
    /// bytes so far match the preface but are fewer: wait for more.
    fn sniff(&mut self, idx: usize, lim: &Limits) -> bool {
        let data = &self.rbuf[self.rpos..self.rlen];
        let k = data.len().min(PREFACE.len());
        if data[..k] != PREFACE[..k] {
            self.sniff = false;
            return false;
        }
        if k < PREFACE.len() {
            if self.eof {
                self.rpos = self.rlen; // closed in the middle of it
            }
            return true;
        }
        self.sniff = false;
        self.rpos += PREFACE.len();
        let max = (lim.body, lim.multipart);
        self.h2 = Some(H2::new(
            idx,
            self.peer,
            lim.max_streams,
            max,
            &mut self.wbuf,
        ));
        false
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
        b.feed(&self.rbuf[..self.rlen], &mut self.rpos)
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
        if self.sniff && self.sniff(idx, lim) {
            return Ok(());
        }
        if let Some(h2) = self.h2.as_mut() {
            loop {
                if let Some((rec, body)) = h2.held.take() {
                    let size = body_size(rec.len(), body.len());
                    if inb.len() - *n < size {
                        h2.held = Some((rec, body));
                        return Err(size);
                    }
                    let id = u64::from_le_bytes(rec[..8].try_into().unwrap());
                    put_body(&mut inb[*n..*n + size], &rec, body, id);
                    *n += size;
                    self.since = now;
                }
                if self.wbuf.len() - self.wpos >= MAX_PENDING {
                    self.paused = true; // read on once the answers went out
                    break;
                }
                if !h2.step(
                    &self.rbuf[..self.rlen],
                    &mut self.rpos,
                    &mut self.wbuf,
                    seq,
                    date,
                    now,
                ) {
                    break;
                }
            }
            // the client closed its side and all its frames are read (a
            // partial frame is left at most): a streamed body can not end
            // well any more
            if self.eof
                && h2.held.is_none()
                && crate::h2::H2::partial(&self.rbuf[self.rpos..self.rlen])
            {
                h2.cut_bodies();
            }
            return Ok(());
        }
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
                        let size = body_size(b.rec.len(), b.data.len());
                        if inb.len() - *n < size {
                            return Err(size);
                        }
                        let mut b = self.body.take().unwrap();
                        let id = ((idx as u64) << 32) | u64::from(*seq);
                        let data = std::mem::take(&mut b.data);
                        put_body(&mut inb[*n..*n + size], &b.rec, data, id);
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
pub(crate) fn parse_len(v: &[u8]) -> Option<u64> {
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
pub(crate) fn length_line(lines: &[u8]) -> Option<Option<u64>> {
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
    // `Connection: close` anywhere wins over `keep-alive` (RFC 9112 9.3)
    let (mut close_tok, mut keep_tok) = (false, false);
    let mut clen: Option<usize> = None;
    let mut chunked = false;
    let mut hlen = 0usize;
    let mut expect = false;
    let mut form = false;
    let (mut conn_up, mut up_ws, mut v13) = (false, false, false);
    let mut key: Option<&[u8]> = None;
    // the WebSocket version and key may come only once (RFC 6455 11.3)
    let (mut n_ver, mut n_key) = (0u32, 0u32);
    for h in req.headers.iter() {
        hlen += 4 + h.name.len() + h.value.len();
        let name = h.name.as_bytes();
        // the first byte picks the few names that matter, so most
        // headers cost one compare
        match name.first().map(u8::to_ascii_lowercase) {
            Some(b'c') => {
                if name.eq_ignore_ascii_case(b"content-length") {
                    match parse_len(h.value).and_then(|v| usize::try_from(v).ok()) {
                        Some(v) if clen.is_none_or(|c| c == v) => clen = Some(v),
                        _ => return Step::Fail(400),
                    }
                } else if name.eq_ignore_ascii_case(b"connection") {
                    // once `close` is seen, `keep-alive` does not matter
                    close_tok = close_tok || has_token(h.value, "close");
                    keep_tok = keep_tok || (!close_tok && has_token(h.value, "keep-alive"));
                    conn_up = conn_up || has_token(h.value, "upgrade");
                } else if name.eq_ignore_ascii_case(b"content-type") {
                    // a file upload gets the multipart limit. The last
                    // `content-type` counts.
                    let mime = h.value.split(|&c| c == b';').next().unwrap_or(b"");
                    form = mime
                        .trim_ascii()
                        .eq_ignore_ascii_case(b"multipart/form-data");
                }
            }
            Some(b't') if name.eq_ignore_ascii_case(b"transfer-encoding") => {
                // HTTP/1.0 has no Transfer-Encoding: the framing is faulty
                // (RFC 9112 6.1). A second one would apply chunked twice.
                if http10 || chunked {
                    return Step::Fail(400);
                }
                // only chunked is known. Anything else can not be framed.
                if !h.value.trim_ascii().eq_ignore_ascii_case(b"chunked") {
                    return Step::Fail(501);
                }
                chunked = true;
            }
            Some(b'e') if name.eq_ignore_ascii_case(b"expect") => {
                expect |= h.value.trim_ascii().eq_ignore_ascii_case(b"100-continue");
            }
            Some(b'u') if name.eq_ignore_ascii_case(b"upgrade") => {
                up_ws |= has_token(h.value, "websocket")
            }
            Some(b's') if name.eq_ignore_ascii_case(b"sec-websocket-version") => {
                v13 = h.value.trim_ascii() == b"13";
                n_ver += 1;
            }
            Some(b's') if name.eq_ignore_ascii_case(b"sec-websocket-key") => {
                key = Some(h.value.trim_ascii());
                n_key += 1;
            }
            _ => {}
        }
    }
    if chunked && clen.is_some() {
        return Step::Fail(400); // two lengths: a request smuggling try
    }
    let close = close_tok || (http10 && !keep_tok);
    let method = req.method.unwrap_or("").as_bytes();
    let target = req.path.unwrap_or("").as_bytes();
    let escaped;
    let target = if target.is_ascii() {
        target
    } else {
        escaped = escape_target(target);
        &escaped[..]
    };
    let clen = clen.unwrap_or(0);
    let has_body = chunked || clen > 0;
    let upgrade = method == b"GET"
        && !http10
        && !has_body
        && conn_up
        && up_ws
        && v13
        && n_ver == 1
        && n_key == 1
        && key.is_some_and(ws_key_ok);
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

/// A `Sec-WebSocket-Key` is base64 of 16 bytes: 22 base64 characters,
/// then `==` (RFC 6455 4.2.1). Another key gives no upgrade.
fn ws_key_ok(k: &[u8]) -> bool {
    k.len() == 24
        && k.ends_with(b"==")
        && k[..22]
            .iter()
            .all(|&c| c.is_ascii_alphanumeric() || c == b'+' || c == b'/')
}

/// A target with raw bytes over 0x7f (eg. UTF-8 from a client that does
/// not encode): those bytes as `%XX`. Dart reads the target as ASCII, and
/// `Uri` decodes `%XX` as UTF-8.
pub(crate) fn escape_target(target: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(target.len() * 3);
    for &c in target {
        if c.is_ascii() {
            out.push(c);
        } else {
            let _ = write!(out, "%{c:02X}");
        }
    }
    out
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
    /// HTTP/2: only the zipped body, the H2 code makes the frames
    h2: bool,
}

impl Job {
    /// The answer: zipped and rendered (HTTP/1.1), or only zipped (HTTP/2).
    fn run(&self) -> Vec<u8> {
        let zipped = gzip(&self.body);
        if self.h2 {
            return zipped;
        }
        let mut out = Vec::with_capacity(zipped.len() + 256);
        render(
            &mut out,
            self.status,
            &self.lines,
            &zipped,
            &self.date,
            self.way,
        );
        out
    }
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
    /// the read buffer lent to each connection in turn (see `Conn::lend`).
    /// The loop runs one connection at a time, so one is enough.
    scratch: Vec<u8>,
    /// lists reused per call
    dead: Vec<usize>,
    touched: Vec<usize>,
}

impl Core {
    fn new(listener: std::net::TcpListener, key: Key, limits: Limits) -> std::io::Result<Self> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKER)?);
        // finds a client whose host is gone (no FIN, eg. a phone that lost
        // its network): the OS probes an idle connection, and an answer
        // that never comes ends it with an error. A live client's OS always
        // answers. Set once here: accepted sockets inherit it (see
        // `accept_all`). An error only means no keep-alive.
        let _ = socket2::SockRef::from(&listener).set_tcp_keepalive(&keepalive());
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
            scratch: Vec::new(),
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
            } else if !s.answered {
                let id = ((idx as u64) << 32) | u64::from(s.seq);
                self.msgs.push(id, REQUEST_GONE, 0, b"");
            }
        }
        if let Some(h2) = &mut c.h2 {
            let base = (idx as u64) << 32;
            // the body parts first, then the end of the bodies that were cut
            for (seq, kind, code, data) in h2.to_dart.drain(..) {
                self.msgs.push(base | u64::from(seq), kind, code, &data);
            }
            for seq in h2.streaming() {
                self.msgs.push(base | u64::from(seq), BODY_END, 1, b"");
            }
            for seq in h2.unanswered() {
                let id = ((idx as u64) << 32) | u64::from(seq);
                self.msgs.push(id, REQUEST_GONE, 0, b"");
            }
            for sid in h2.streamed() {
                if self.streams.remove(&sid).is_some() {
                    self.msgs.push(sid, STREAM_GONE, 0, b"");
                }
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
        let limits = self.limits;
        let Some(c) = self.conns.get_mut(idx) else {
            return;
        };
        let at = c.due(&limits).map_or(self.now + limits.idle, |d| d.0);
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
            let limits = self.limits;
            let Some(c) = self.conns.get_mut(idx) else {
                continue;
            };
            if c.timer != Some(t) {
                continue; // an old entry
            }
            c.timer = None;
            match c.due(&limits) {
                Some((at, what)) if at <= now => match what {
                    Due::Body => {
                        c.fail(408, &self.date.1);
                        self.service(idx);
                    }
                    // HTTP/2 idle: GOAWAY NO_ERROR, then close once it is
                    // written (the send stall covers a client that does
                    // not read)
                    Due::Head if c.h2.is_some() && !c.shut => {
                        c.h2.as_mut().unwrap().drain(&mut c.wbuf);
                        c.shut = true;
                        self.service(idx);
                    }
                    Due::H2Body => {
                        let h2 = c.h2.as_mut().unwrap();
                        h2.expire(now, limits.timeout, SEND_WAIT, &self.date.1, &mut c.wbuf);
                        self.service(idx);
                    }
                    Due::Ping => {
                        let ws = c.ws.as_mut().unwrap();
                        ws.pinged = Some(now);
                        ws.queue.push_front(Message::Ping(Bytes::new()));
                        self.service(idx);
                    }
                    Due::Pong => {
                        // The pong may wait in the socket, not read yet:
                        // timers run before reads, and Dart may have been
                        // busy for longer than the interval. Read first.
                        self.ws_read(idx);
                        if let Some(c) = self.conns.get_mut(idx)
                            && let Some((at, Due::Pong)) = c.due(&limits)
                            && at <= now
                        {
                            // like dart:io: the close code is 1001 (going away)
                            let ws = c.ws.as_mut().unwrap();
                            ws.code.get_or_insert((1001, Vec::new()));
                            self.kill(idx);
                        }
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
                    let Some(c) = self.conns.get_mut(idx) else {
                        continue;
                    };
                    c.hup |= ev.is_read_closed();
                    let paused = c.paused || c.ws.as_ref().is_some_and(|w| w.paused);
                    if readable && !paused {
                        self.queue(idx);
                    } else if c.paused && c.hup && c.ws.is_none() {
                        // no read while paused (eg. an upgrade waits): the
                        // close is only seen here, see `Due::Gone`
                        self.arm_timer(idx);
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
            if self.limits.conns > 0 && self.conns.len() >= self.limits.conns {
                // at the limit: try again soon, like after an accept error
                self.accept_at = Some(self.now + ACCEPT_RETRY);
                return;
            }
            match listener.accept() {
                Ok((mut sock, addr)) => {
                    let _ = sock.set_nodelay(true);
                    // Linux copies the keep-alive of the listener (see
                    // `Core::new`) to the accepted socket. macOS copies
                    // all but the idle time. Others (eg. Windows) get all
                    // of it here. Tested in `accepted_sockets_have_keepalive`.
                    #[cfg(target_os = "macos")]
                    let _ = socket2::SockRef::from(&sock)
                        .set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(KEEPALIVE));
                    #[cfg(not(any(
                        target_os = "linux",
                        target_os = "android",
                        target_os = "macos"
                    )))]
                    let _ = socket2::SockRef::from(&sock).set_tcp_keepalive(&keepalive());
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
                    conn.sniff = self.limits.http2;
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
            ws.heard = now;
            if draining {
                ws.close(CloseCode::Away);
            }
            c.paused = false;
            c.queued = true;
            self.ready.push_back(idx);
            if ws.ping.is_some() {
                self.arm_timer(idx);
            }
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
        let data = c.h2.as_ref().map_or(0, |h| h.data_sent);
        loop {
            if let Some(h2) = c.h2.as_mut() {
                h2.send(&mut c.wbuf, c.wpos, c.written, &mut c.marks);
            }
            if c.pending() > 0 && !c.flush() {
                self.kill(idx);
                return;
            }
            // HTTP/2: all written, and more DATA may go now
            if c.pending() > 0 || !c.h2.as_ref().is_some_and(|h| h.can_send()) {
                break;
            }
        }
        if let Some(h2) = c.h2.as_mut() {
            h2.stall_mark(now);
            for (seq, kind, code, data) in h2.to_dart.drain(..) {
                let id = ((idx as u64) << 32) | u64::from(seq);
                self.msgs.push(id, kind, code, &data);
            }
            for seq in h2.gone.drain(..) {
                let id = ((idx as u64) << 32) | u64::from(seq);
                self.msgs.push(id, REQUEST_GONE, 0, b"");
            }
            for sid in h2.gone_streams.drain(..) {
                if self.streams.remove(&sid).is_some() {
                    self.msgs.push(sid, STREAM_GONE, 0, b"");
                }
            }
            c.shut |= h2.over();
            if c.written > before {
                h2.flushed(); // our ACKs are on their way
            }
        }
        // a write starts to wait: make sure its timer runs
        let blocked = c.h2.as_ref().is_some_and(|h| h.blocked());
        let wait_starts = c.stuck.is_none() && (c.pending() > 0 || blocked);
        while let Some(&(end, sid, n)) = c.marks.front() {
            if end > c.written {
                break;
            }
            c.marks.pop_front();
            self.msgs.push(sid, STREAM_SENT, 0, &n.to_le_bytes());
        }
        // the idle time (and the grace of a client that closed its side,
        // see `due`) counts from the last write. HTTP/2: PING and SETTINGS
        // ACKs do not count, only streams that open, close or send.
        let active = match c.h2.as_mut() {
            Some(h2) => std::mem::take(&mut h2.stirred) || (c.written > before && h2.open() > 0),
            None => c.written > before,
        };
        if active {
            c.since = now;
        }
        // HTTP/2: a closed window with data waiting also counts as stuck
        if c.pending() == 0 && !blocked {
            c.stuck = None;
        } else if c.stuck.is_none()
            // a blocked HTTP/2 window: ACKs and PINGs are no progress, only DATA is
            || if blocked {
                c.h2.as_ref().is_some_and(|h| h.data_sent > data)
            } else {
                c.written > before
            }
        {
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
        // any bytes from the client show it is there (see `Ws::ping`)
        let ping = ws.ping.is_some();
        if io.left < WS_READ {
            ws.heard = self.now;
            ws.pinged = None;
        }
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
        if ping {
            // after a pause the ping may be due earlier than the timer
            self.arm_timer(idx);
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
                7 => self.part(id, body, false, true, b""),
                // the header lines are trailers
                8 => self.part(id, b"", true, status == 0, lines),
                9 => self.ws_ping(id, body),
                10 if body.len() >= 8 => {
                    let n = u64::from_le_bytes(body[..8].try_into().unwrap());
                    self.body_ack(id, n);
                }
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
        if let Some(c) = self.conns.get_mut(idx)
            && let Some(h2) = c.h2.as_mut()
        {
            // HTTP/2: no answer order. Kind bit 0x200 does not matter.
            let zip = kind & 0xff == 0
                && kind & 0x100 != 0
                && !body.is_empty()
                && h2.defer(seq, status, lines);
            match kind & 0xff {
                0 if zip => {}
                0 => h2.answer(seq, status, lines, body, date, &mut c.wbuf),
                6 if body.len() >= 8 => {
                    let sid = u64::from_le_bytes(body[..8].try_into().unwrap());
                    if h2.start(seq, status, lines, sid, &body[8..], date, &mut c.wbuf) {
                        self.streams.insert(sid, (idx, seq));
                    } else {
                        self.msgs.push(sid, STREAM_GONE, 0, b""); // Dart stops
                    }
                }
                // a WebSocket can not come: the request never has flag bit 1
                _ => h2.refuse(seq, &mut c.wbuf),
            }
            self.touch(idx);
            if zip {
                let job = Job {
                    idx,
                    seq,
                    status,
                    lines: Vec::new(),
                    body: body.to_vec(),
                    date: Vec::new(),
                    way: Way {
                        http10: false,
                        head: false,
                        close: false,
                        said: false,
                    },
                    h2: true,
                };
                self.zip(job);
            } else if kind & 0xff == 1 {
                self.answer_gone(kind, body);
            }
            return;
        }
        let found = self.conns.get_mut(idx).and_then(|c| {
            let first = c.slots.front()?.seq;
            let i = seq.wrapping_sub(first) as usize;
            let s = c.slots.get(i)?;
            (s.seq == seq && !s.answered).then_some(i)
        });
        let Some(i) = found else {
            // the client is gone (or a second answer)
            self.answer_gone(kind, body);
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
                    h2: false,
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
                c.ws = Some(Box::new(Ws::new(ws_id, max, self.now)));
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
                // HEAD, 1xx, 204 and 304 have no body: only the head goes
                // out, the connection stays usable
                let silent = way.head || no_body(status);
                let (len, tail) = match length_line(lines) {
                    _ if silent => (Len::Open, Tail::Close),
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
                if silent {
                    // no body: tell Dart to stop
                    s.done = true;
                    c.put(i, &out, 0);
                    self.msgs.push(sid, STREAM_GONE, 0, b"");
                } else {
                    s.stream = Some((sid, tail));
                    c.put(i, &out, 0);
                    self.streams.insert(sid, (idx, seq));
                    self.touch(idx);
                    self.part(sid, first, false, true, b"");
                }
            }
        }
        self.touch(idx);
    }

    /// An answer that has no request to go to: tell Dart its WebSocket
    /// (kind 1) or stream (kind 6) is over.
    fn answer_gone(&mut self, kind: u16, body: &[u8]) {
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
    }

    /// HTTP/2: Dart consumed `n` bytes of the streamed body of request `id`
    /// (BODY_ACK).
    fn body_ack(&mut self, id: u64, n: u64) {
        let idx = (id >> 32) as usize;
        if let Some(c) = self.conns.get_mut(idx)
            && let Some(h2) = c.h2.as_mut()
        {
            h2.ack(id as u32, n, &mut c.wbuf);
            self.touch(idx);
        }
    }

    /// A part (or the end) of the streamed body `sid`. The end may have
    /// trailers (HTTP/2 only).
    fn part(&mut self, sid: u64, data: &[u8], end: bool, ok: bool, trailers: &[u8]) {
        let Some(&(idx, seq)) = self.streams.get(&sid) else {
            return;
        };
        let Some(c) = self.conns.get_mut(idx) else {
            return;
        };
        if let Some(h2) = c.h2.as_mut() {
            if end {
                self.streams.remove(&sid);
            }
            h2.part(seq, data, end, ok, trailers, &mut c.wbuf);
            self.touch(idx);
            return;
        }
        // ponytail: trailers are dropped on HTTP/1.1. Chunked trailers need
        // `TE: trailers` from the client; add them when someone asks.
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

    /// Dart set `pingInterval` of the WebSocket `id`: u64 milliseconds in
    /// `body`, 0 for off. The interval starts now.
    fn ws_ping(&mut self, id: u64, body: &[u8]) {
        let Some(&idx) = self.wss.get(&id) else {
            return;
        };
        let Some(ws) = self.conns.get_mut(idx).and_then(|c| c.ws.as_mut()) else {
            return;
        };
        let ms = body
            .get(..8)
            .map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap()));
        ws.ping = (ms > 0).then(|| Duration::from_millis(ms).min(MAX_TIMEOUT));
        ws.heard = self.now;
        ws.pinged = None;
        self.arm_timer(idx);
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
                        let out = job.run();
                        results.lock().unwrap().push((job.idx, job.seq, out));
                        let _ = waker.wake();
                    }
                });
            if started.is_err() {
                // no thread: zip here
                let out = job.run();
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
            if let Some(h2) = c.h2.as_mut() {
                h2.zipped(seq, &out, &self.date.1, &mut c.wbuf);
                self.touch(idx);
                continue;
            }
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
                let lent = c.lend(&mut self.scratch);
                let ok = c.read();
                c.rpos = c.rlen;
                c.give_back(&mut self.scratch, lent);
                if !ok {
                    self.kill(idx);
                    continue;
                }
                if c.finished() {
                    self.kill(idx);
                } else if c.eof {
                    self.arm_timer(idx); // see `Due::Gone`
                }
                continue;
            }
            let lent = c.lend(&mut self.scratch);
            if !c.read() {
                c.give_back(&mut self.scratch, lent);
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
            c.give_back(&mut self.scratch, lent);
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
            if c.eof || c.h2.is_some() || c.body.as_ref().is_some_and(|b| b.last == self.now) {
                self.arm_timer(idx); // `Due::Gone`, `Due::Body` or `Due::H2Body`
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
            if let Some(h2) = c.h2.as_mut() {
                // GOAWAY: the open streams finish, then it closes
                h2.drain(&mut c.wbuf);
                self.service(idx);
                continue;
            }
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
    fn due(&self, limits: &Limits) -> Option<(Instant, Due)> {
        if let Some((t, _)) = self.linger {
            return Some((t + LINGER, Due::Linger));
        }
        if let Some(t) = self.ws.as_ref().and_then(|w| w.closing) {
            return Some((t + CLOSE_WAIT, Due::Ws));
        }
        if let Some(b) = &self.body
            && !b.whole
        {
            return Some((b.last + limits.timeout, Due::Body));
        }
        // HTTP/2: the body timeout of the stream that waits longest, unless
        // the send stall comes first
        let h2_due = self.h2.as_ref().and_then(|h| {
            let body = h.body_since().map(|t| t + limits.timeout);
            let stall = h.stall_since().map(|t| t + SEND_WAIT);
            body.into_iter().chain(stall).min()
        });
        if let Some(t) = h2_due
            && self.stuck.is_none_or(|s| t <= s + SEND_WAIT)
        {
            return Some((t, Due::H2Body));
        }
        // a write makes no progress: the client does not read
        if let Some(t) = self.stuck {
            return Some((t + SEND_WAIT, Due::Send));
        }
        if let Some(w) = &self.ws
            && w.live
            && !w.paused
            && let Some(every) = w.ping
        {
            return Some(match w.pinged {
                Some(t) => (t + every, Due::Pong),
                None => (w.heard + every, Due::Ping),
            });
        }
        if self.ws.is_some() || self.body.is_some() || self.pending() > 0 {
            return None;
        }
        if !self.busy() {
            return Some((self.since + limits.idle, Due::Head));
        }
        // The client closed its side while answers wait. It may still
        // read (a half-close), so it gets the idle time to get an answer.
        // `since` moves with each write, so a stream that still writes
        // stays. A client that is really gone makes the write fail.
        // A paused connection (eg. an upgrade waits, or too many requests)
        // reads nothing, so only the poll saw the close (`hup`).
        if self.eof || (self.hup && self.paused) {
            return Some((self.since + limits.idle, Due::Gone));
        }
        None
    }
}

/// TCP keep-alive of accepted sockets: the first probe after [KEEPALIVE]
/// with no traffic, then one every 10 seconds. After 6 probes with no
/// answer (Windows: its fixed count) the connection is dead.
fn keepalive() -> socket2::TcpKeepalive {
    let ka = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE)
        .with_interval(Duration::from_secs(10));
    #[cfg(not(windows))]
    let ka = ka.with_retries(6);
    ka
}

/// The limits from Dart's values. A timeout is at most [MAX_TIMEOUT], so
/// `Instant + timeout` can not overflow.
fn limits_of(
    max_body: i64,
    max_multipart: i64,
    body_timeout_ms: i64,
    max_conns: i64,
    idle_ms: i64,
    http2: bool,
    max_streams: i64,
) -> Limits {
    Limits {
        body: max_body.max(0) as usize,
        multipart: max_multipart.max(0) as usize,
        timeout: Duration::from_millis(body_timeout_ms.max(0) as u64).min(MAX_TIMEOUT),
        idle: Duration::from_millis(idle_ms.max(0) as u64).min(MAX_TIMEOUT),
        conns: max_conns.max(0) as usize,
        http2,
        max_streams: max_streams.clamp(0, i64::from(u32::MAX)) as u32,
    }
}

/// One server. Dart holds an `Arc` of it as handle.
struct Native {
    core: Mutex<Core>,
    /// wakes a `poll` on the poll file (dox_unbind)
    waker: Arc<Waker>,
    /// the poll file (epoll or kqueue), watched by the idle helper
    #[cfg(unix)]
    fd: i32,
    /// Dart is idle: the helper watches, and tells Dart when work comes
    armed: AtomicBool,
    /// the next timer of the core, in nanoseconds after `base`. `u64::MAX`:
    /// none.
    due: AtomicU64,
    base: Instant,
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

    fn set_due(&self, at: Option<Instant>) {
        let ns = at.map_or(u64::MAX, |t| {
            t.saturating_duration_since(self.base)
                .as_nanos()
                .min(u128::from(u64::MAX - 1)) as u64
        });
        self.due.store(ns, Ordering::Release);
    }

    /// How long until the next timer of the core. None: no timer.
    fn due_in(&self) -> Option<Duration> {
        let ns = self.due.load(Ordering::Acquire);
        (ns != u64::MAX).then(|| {
            (self.base + Duration::from_nanos(ns)).saturating_duration_since(Instant::now())
        })
    }

    /// true when the poll file has an event within `wait`. Does not read
    /// the event and does not take the core: `dox_run` does that.
    #[cfg(unix)]
    fn ready_within(&self, wait: Duration) -> bool {
        let mut p = libc::pollfd {
            fd: self.fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // round up, so a timer a bit in the future does not spin
        let ms = wait.as_micros().div_ceil(1000).min(i32::MAX as u128) as i32;
        let r = unsafe { libc::poll(&mut p, 1, ms) };
        r > 0 && p.revents & libc::POLLIN != 0
    }

    /// Windows has no poll file to watch: take the core for at most 1 ms
    /// at a time, so Dart waits at most that long.
    #[cfg(not(unix))]
    fn ready_within(&self, wait: Duration) -> bool {
        let mut core = self.core.lock().unwrap();
        core.update_clock();
        core.wait(Some(wait.min(Duration::from_millis(1))));
        core.has_work()
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

    /// The idle helper: watches the poll file while Dart is idle. Also
    /// checks every [PROBE] that the isolate is alive, idle or not.
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
            while self.armed.load(Ordering::Acquire) && !self.stopped.load(Ordering::Acquire) {
                let wait = self.due_in().map_or(PROBE, |d| d.min(PROBE));
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
                        return self.gone(self.core.lock().unwrap());
                    }
                    break;
                }
                if probed.elapsed() >= PROBE {
                    probed = Instant::now();
                    if !post(self.dart_port, &[]) {
                        return self.gone(self.core.lock().unwrap());
                    }
                }
            }
            if self.stopped.load(Ordering::Acquire) {
                return;
            }
        }
    }
}

// ---- FFI ---------------------------------------------------------------

/// Binds `port` and starts the idle helper, which posts to `send_port`.
/// `family`, `addr_hi` and `addr_lo` are the host, see [Key]. Family 0 is
/// all interfaces. Bodies over `max_body` bytes (`max_multipart` for a
/// multipart form) get 413. A body with no new bytes for
/// `body_timeout_ms` gets 408. With `max_conns` > 0 it holds at most that
/// many connections open. A connection with no request for `idle_ms`
/// closes. With `http2`, a connection that starts with the HTTP/2
/// preface speaks HTTP/2, with at most `max_streams` open streams.
/// Returns a handle (> 0), or < 0: -1 not
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
    max_conns: i64,
    idle_ms: i64,
    http2: bool,
    max_streams: i64,
) -> i64 {
    if POST.get().is_none() {
        return -1;
    }
    let (listener, key) = match bind((family, addr_hi, addr_lo, port as u16)) {
        Ok(l) => l,
        Err(e) => return -(1000 + i64::from(e.raw_os_error().unwrap_or(0))),
    };
    let limits = limits_of(
        max_body,
        max_multipart,
        body_timeout_ms,
        max_conns,
        idle_ms,
        http2,
        max_streams,
    );
    let Ok(core) = Core::new(listener, key, limits) else {
        unbind(key);
        return -2;
    };
    #[cfg(unix)]
    let fd = {
        use std::os::fd::AsRawFd;
        core.poll.as_raw_fd()
    };
    let native = Arc::new(Native {
        waker: core.waker.clone(),
        core: Mutex::new(core),
        #[cfg(unix)]
        fd,
        // a new server may have connections waiting: Dart reads at once
        armed: AtomicBool::new(true),
        due: AtomicU64::new(u64::MAX),
        base: Instant::now(),
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
    let mut core = s.core.lock().unwrap();
    s.armed.store(false, Ordering::Release);
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
        Ok(0) if core.has_work() => {
            // a connection waits for its next turn: come back after a turn
            // of the event loop
            drop(core);
            post(s.dart_port, &[1]);
            0
        }
        Ok(0) => {
            s.set_due(core.next_timer());
            s.armed.store(true, Ordering::Release);
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
    let mut core = s.core.lock().unwrap();
    core.update_clock();
    core.start_drain();
    if core.done_due() {
        post(s.dart_port, &[2]);
    }
    // the lingering closes have timers: the helper must wake for them
    s.set_due(core.next_timer());
}

/// Stops a server, closes its sockets and frees the handle.
///
/// # Safety
/// `handle` must come from `dox_bind`. Do not use it after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_unbind(handle: i64) {
    let s = unsafe { Arc::from_raw(handle as *const Native) };
    s.stopped.store(true, Ordering::Release);
    let _ = s.waker.wake(); // ends a `poll` of the helper
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

/// Entry points for the fuzz targets in `fuzz/`. Only in `cargo fuzz`
/// builds (`--cfg fuzzing`), not in the library.
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod fuzz {
    use super::*;
    use crate::aead;
    use std::sync::OnceLock;

    /// HPACK: see `h2::hpack::fuzz`.
    pub fn hpack(data: &[u8]) {
        crate::h2::hpack::fuzz(data)
    }

    /// The defaults of `NativeHttpServer.bind` in lib/src/server.dart
    /// (`maxBodySize`, `maxMultipartSize`, `bodyTimeout`, `idleTimeout`,
    /// `maxConcurrentStreams`). Keep them the same when those change.
    fn default_limits() -> Limits {
        limits_of(
            100 * 1024 * 1024,
            100 * 1024 * 1024,
            120_000,
            0,
            30_000,
            false,
            250,
        )
    }

    /// Remote ip and port of the test client. Not zero, so the record
    /// check sees that they are copied.
    const PEER: [u8; 18] = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
    ];

    /// A request record, read the way Dart reads it: `_onBatch` in
    /// lib/src/server.dart and `_parse` in lib/src/headers.dart.
    #[derive(Debug, PartialEq)]
    struct Rec {
        flags: u32,
        method: Vec<u8>,
        target: Vec<u8>,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
        body: Vec<u8>,
    }

    /// Reads the record at the start of `b`. Every length must stay inside
    /// the record, so Dart never reads past its end. Returns the record
    /// and its length.
    fn decode(b: &[u8]) -> (Rec, usize) {
        assert!(b.len() >= REQ_HEAD);
        let flags = rd32(b, 8) as u32;
        let (ml, tl) = (rd16(b, 12) as usize, rd16(b, 14) as usize);
        let (hl, bl) = (rd32(b, 16), rd32(b, 20));
        let len = record_len(b);
        assert!(len <= b.len(), "the record is longer than the bytes");
        assert_eq!(b[24..26], PEER[16..]);
        assert_eq!(b[26..28], [0, 0]);
        assert_eq!(b[28..44], PEER[..16]);
        let mut q = REQ_HEAD;
        let method = b[q..q + ml].to_vec();
        q += ml;
        let target = b[q..q + tl].to_vec();
        q += tl;
        // headers.dart walks u16 name length, u16 value length, name,
        // value. The walk must end exactly at the end of the block.
        let hb = &b[q..q + hl];
        q += hl;
        let mut headers = Vec::new();
        let mut i = 0;
        while i + 4 <= hb.len() {
            let name_end = i + 4 + rd16(hb, i) as usize;
            let end = name_end + rd16(hb, i + 2) as usize;
            assert!(end <= hb.len(), "a header runs past the header block");
            headers.push((hb[i + 4..name_end].to_vec(), hb[name_end..end].to_vec()));
            i = end;
        }
        assert_eq!(i, hb.len(), "bytes left after the last header");
        let body = if flags & 4 != 0 {
            // external: address, token, length. Take it back and free it.
            assert_eq!(bl, 24);
            let word = |o: usize| u64::from_le_bytes(b[q + o..q + o + 8].try_into().unwrap());
            let v = unsafe { Box::from_raw(word(8) as *mut Vec<u8>) };
            assert_eq!(word(0), v.as_ptr() as u64);
            assert_eq!(word(16), v.len() as u64);
            assert!(v.len() >= EXTERNAL_MIN);
            *v
        } else {
            b[q..q + bl].to_vec()
        };
        let rec = Rec {
            flags,
            method,
            target,
            headers,
            body,
        };
        (rec, len)
    }

    /// Optional white space: spaces and tabs (RFC 9110 5.6.3).
    fn ows(v: &[u8]) -> &[u8] {
        let is_ws = |c: &u8| *c == b' ' || *c == b'\t';
        let start = v.iter().position(|c| !is_ws(c)).unwrap_or(v.len());
        let end = v.iter().rposition(|c| !is_ws(c)).map_or(start, |e| e + 1);
        &v[start..end]
    }

    /// The values of all headers with this name.
    fn values<'a>(req: &httparse::Request<'_, 'a>, name: &str) -> Vec<&'a [u8]> {
        req.headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case(name))
            .map(|h| h.value)
            .collect()
    }

    /// The list items of all headers with this name, lowercase (RFC 9110
    /// 5.3: many headers of a list are one list).
    fn tokens(req: &httparse::Request, name: &str) -> Vec<Vec<u8>> {
        values(req, name)
            .into_iter()
            .flat_map(|v| v.split(|&c| c == b','))
            .map(|t| ows(t).to_ascii_lowercase())
            .collect()
    }

    /// How the body of a request is framed.
    #[derive(Debug, PartialEq)]
    enum Framing {
        Len(u64),
        Chunked,
        Reject,
    }

    /// The body framing by RFC 9112 section 6, written apart from
    /// `parse_one`. Where the server is stricter on purpose, it says so.
    fn framing(req: &httparse::Request) -> Framing {
        let te = values(req, "transfer-encoding");
        let cl = values(req, "content-length");
        if !te.is_empty() {
            // 6.1: Transfer-Encoding in HTTP/1.0 means faulty framing
            if req.version == Some(0) {
                return Framing::Reject;
            }
            // 6.1: both is a smuggling try. The RFC lets a server drop
            // the length and go on. The server rejects: stricter on purpose.
            if !cl.is_empty() {
                return Framing::Reject;
            }
            // 6.3 and 7: chunked must be the last coding, and only once.
            // The server knows no other coding, so any other gets 501
            // (6.1). An empty list item (`chunked,`) is refused too:
            // stricter on purpose.
            let codings = tokens(req, "transfer-encoding");
            return if codings == [b"chunked".to_vec()] {
                Framing::Chunked
            } else {
                Framing::Reject
            };
        }
        // 6.3 item 5. The same length in two headers is fine. A list
        // (`5, 5`) is refused: stricter on purpose. Only digits, no sign.
        let mut len = None;
        for v in cl {
            let v = ows(v);
            if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
                return Framing::Reject;
            }
            let Ok(n) = std::str::from_utf8(v).unwrap().parse::<u64>() else {
                return Framing::Reject; // too big
            };
            if usize::try_from(n).is_err() || len.is_some_and(|l| l != n) {
                return Framing::Reject;
            }
            len = Some(n);
        }
        // GET and HEAD may have a body too: the framing is the same for
        // all methods (RFC 9110 9.3.1)
        Framing::Len(len.unwrap_or(0))
    }

    /// What the server must do with a request whose head httparse parsed.
    struct Want {
        framing: Framing,
        http10: bool,
        close: bool,
        /// the WebSocket key, when the request can upgrade
        key: Option<Vec<u8>>,
        expect: bool,
    }

    fn want(req: &httparse::Request) -> Want {
        let framing = framing(req);
        let http10 = req.version == Some(0);
        let conn = tokens(req, "connection");
        let has = |t: &[u8]| conn.iter().any(|c| c == t);
        // RFC 9112 9.3
        let close = has(b"close") || (http10 && !has(b"keep-alive"));
        // RFC 6455 4.2.1, and 11.3: version and key come only once
        let ver = values(req, "sec-websocket-version");
        let keys = values(req, "sec-websocket-key");
        let can_upgrade = req.method == Some("GET")
            && !http10
            && framing == Framing::Len(0)
            && has(b"upgrade")
            && tokens(req, "upgrade").iter().any(|t| t == b"websocket")
            && ver.len() == 1
            && ows(ver[0]) == b"13"
            && keys.len() == 1
            && base64(ows(keys[0])).is_some_and(|k| k.len() == 16);
        let expect = values(req, "expect")
            .iter()
            .any(|v| ows(v).eq_ignore_ascii_case(b"100-continue"));
        Want {
            framing,
            http10,
            close,
            key: can_upgrade.then(|| ows(keys[0]).to_vec()),
            expect,
        }
    }

    /// The target for Dart: raw bytes over 0x7f as %XX, all else as it is.
    fn escaped(path: &str) -> Vec<u8> {
        let mut out = Vec::new();
        for &c in path.as_bytes() {
            if c.is_ascii() {
                out.push(c);
            } else {
                out.extend_from_slice(format!("%{c:02X}").as_bytes());
            }
        }
        out
    }

    /// The bytes of a base64 value (RFC 4648, with padding), or None.
    fn base64(v: &[u8]) -> Option<Vec<u8>> {
        let pad = v.iter().rev().take_while(|&&c| c == b'=').count();
        if !v.len().is_multiple_of(4) || pad > 2 {
            return None;
        }
        let (mut bits, mut n, mut out) = (0u32, 0, Vec::new());
        for &c in &v[..v.len() - pad] {
            let d = match c {
                b'A'..=b'Z' => c - b'A',
                b'a'..=b'z' => c - b'a' + 26,
                b'0'..=b'9' => c - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                _ => return None,
            };
            bits = (bits << 6 | u32::from(d)) & 0xffff;
            n += 6;
            if n >= 8 {
                n -= 8;
                out.push((bits >> n) as u8);
            }
        }
        Some(out)
    }

    /// The record must hold what httparse read from the same bytes.
    fn check_rec(rec: &Rec, req: &httparse::Request, want: &Want) {
        assert_eq!(rec.method, req.method.unwrap().as_bytes());
        assert!(rec.target.is_ascii(), "the target is not ASCII");
        assert_eq!(rec.target, escaped(req.path.unwrap()));
        assert_eq!(rec.headers.len(), req.headers.len());
        for ((name, value), h) in rec.headers.iter().zip(req.headers.iter()) {
            assert!(
                !name.iter().any(u8::is_ascii_uppercase),
                "a name is not lowercase"
            );
            assert_eq!(*name, h.name.to_ascii_lowercase().as_bytes());
            // headers.dart splits lines on `\n`: a line break would add a header
            assert!(
                !value.iter().any(|&c| c == b'\r' || c == b'\n'),
                "CR or LF in a value"
            );
            assert_eq!(value, h.value);
        }
        let flags = u32::from(want.http10) | (u32::from(want.key.is_some()) << 1);
        assert_eq!(rec.flags & !4, flags, "flags");
    }

    fn check_slot(s: &Slot, req: &httparse::Request, want: &Want) {
        assert_eq!(s.close, want.close, "close");
        assert_eq!(s.http10, want.http10);
        assert_eq!(s.head, req.method == Some("HEAD"));
        assert_eq!(s.key.as_deref(), want.key.as_deref(), "upgrade key");
    }

    /// Random bytes as a request head (and maybe a body). Checks the
    /// record for Dart and the body framing against httparse and
    /// [framing].
    pub fn parse_head(data: &[u8]) {
        let lim = default_limits();
        let mut hs = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut req = httparse::Request::new(&mut hs);
        let parsed = req.parse(data);
        // room for any record, and a small room to hit `NoRoom`
        let mut inb = vec![0u8; REQ_HEAD + 4 * data.len()];
        let full = inb.len();
        for room in [full, data.len() / 2] {
            let step = parse_one(data, 1, &PEER, &mut inb[..room], 0, &lim, Instant::now());
            let head = match parsed {
                Ok(httparse::Status::Complete(h)) if h <= MAX_HEAD => h,
                Ok(httparse::Status::Complete(_)) => {
                    assert!(matches!(step, Step::Fail(431)));
                    continue;
                }
                Ok(httparse::Status::Partial) if data.len() > MAX_HEAD => {
                    assert!(matches!(step, Step::Fail(431)));
                    continue;
                }
                Ok(httparse::Status::Partial) => {
                    assert!(matches!(step, Step::More));
                    continue;
                }
                Err(httparse::Error::TooManyHeaders) => {
                    assert!(matches!(step, Step::Fail(431)));
                    continue;
                }
                Err(_) => {
                    assert!(matches!(step, Step::Fail(400)));
                    continue;
                }
            };
            let want = want(&req);
            let len = match want.framing {
                Framing::Reject => {
                    assert!(matches!(step, Step::Fail(400 | 501)), "must reject");
                    continue;
                }
                Framing::Len(n) => Some(n as usize),
                Framing::Chunked => None,
            };
            let max = lim.body.max(lim.multipart);
            if let Some(n) = len
                && n > max
                && (want.expect || n > max + DRAIN_MAX)
            {
                assert!(matches!(step, Step::Fail(413)));
                continue;
            }
            let whole = len.is_some_and(|n| data.len() >= head + n);
            let n = len.unwrap_or(0);
            match step {
                Step::Done(used, slot) => {
                    assert!(
                        whole && n < EXTERNAL_MIN && n <= max,
                        "a record, but the body is not here"
                    );
                    assert_eq!(used, head + n);
                    let (rec, rl) = decode(&inb[..room]);
                    assert_eq!(
                        rl,
                        REQ_HEAD + rec.method.len() + rec.target.len() + rd32(&inb, 16) + n
                    );
                    assert_eq!(rec.flags & 4, 0);
                    assert_eq!(rec.body, &data[head..head + n]);
                    check_rec(&rec, &req, &want);
                    check_slot(&slot, &req, &want);
                }
                Step::Body(used, b, go_on) => {
                    assert!(
                        !whole || n >= EXTERNAL_MIN || n > max,
                        "the body is here, but no record"
                    );
                    assert_eq!(used, head);
                    match len {
                        Some(n) => assert!(b.chunk.is_none() && b.left == n),
                        None => assert!(b.chunk == Some(Chunk::Size)),
                    }
                    assert!(b.data.is_empty() && b.seen == 0 && !b.whole && !b.too_big);
                    // `100 Continue` only when the client waits for it
                    assert_eq!(go_on, want.expect && (len.is_none() || !whole));
                    let (rec, rl) = decode(&b.rec);
                    assert_eq!(rl, b.rec.len());
                    assert!(rec.body.is_empty());
                    check_rec(&rec, &req, &want);
                    check_slot(&b.slot, &req, &want);
                }
                Step::NoRoom(size) => {
                    assert!(size > room);
                    assert!(room < full, "no room in the big buffer");
                }
                Step::More | Step::Fail(_) => panic!("the request is good"),
            }
        }
    }

    /// A chunked body: byte 0 picks the size of the read pieces, byte 1 the
    /// body limit, the rest is the body.
    pub fn chunked_body(data: &[u8]) {
        let [seed, max, body @ ..] = data else {
            return;
        };
        let lim = Limits {
            body: usize::from(*max) * 64,
            ..default_limits()
        };
        let head = b"POST / HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\n";
        let start = || match parse_one(head, 1, &[0; 18], &mut [], 0, &lim, Instant::now()) {
            Step::Body(_, b, _) => b,
            _ => unreachable!(),
        };
        // all at once
        let mut whole = start();
        let mut whole_pos = 0;
        let want = whole.feed(body, &mut whole_pos);
        // in pieces, like socket reads
        let mut b = start();
        let (mut pos, mut end, mut rng) = (0, 0, u32::from(*seed) | 1);
        let got = loop {
            end = (end + 1 + next(&mut rng) % 64).min(body.len());
            let before = pos;
            let r = b.feed(&body[..end], &mut pos);
            assert!(before <= pos && pos <= end);
            if r != Feed::More || end == body.len() {
                break r;
            }
        };
        assert!(b.data.len() <= pos);
        // the pieces must not change the result
        assert_eq!(got, want);
        if !matches!(got, Feed::Fail(_)) {
            // a fail can count bytes in `seen` before it moves `pos`, so
            // these only hold for More and Whole
            assert!(b.seen <= pos);
            assert_eq!(pos, whole_pos);
        }
        if got == Feed::Whole {
            assert!(b.data.len() == b.seen && b.seen <= lim.body);
            assert_eq!(b.data, whole.data);
        }
    }

    /// xorshift: cheap random numbers from a fuzz byte
    fn next(rng: &mut u32) -> usize {
        *rng ^= *rng << 13;
        *rng ^= *rng >> 17;
        *rng ^= *rng << 5;
        *rng as usize
    }

    /// A socket for a `Conn`. `parse` does no socket I/O, so all of them
    /// are copies of one loopback socket, made once.
    fn sock() -> TcpStream {
        static SOCK: OnceLock<(std::net::TcpStream, std::net::TcpStream)> = OnceLock::new();
        let (s, _) = SOCK.get_or_init(|| {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let c = std::net::TcpStream::connect(l.local_addr().unwrap()).unwrap();
            (l.accept().unwrap().0, c)
        });
        TcpStream::from_std(s.try_clone().unwrap())
    }

    const CONTINUE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

    /// close, head, http10, upgrade key
    type SlotSeen = (bool, bool, bool, Option<Box<[u8]>>);

    /// What one connection gave Dart.
    #[derive(Debug, PartialEq)]
    struct Seen {
        recs: Vec<Rec>,
        /// per request: close, head, http10, upgrade key
        slots: Vec<SlotSeen>,
        /// the answer of a bad request, when one came
        fail: Vec<u8>,
        /// a body is still read
        body: bool,
    }

    /// Feeds `stream` to one `Conn`, cut at `cuts`, like socket reads.
    /// `small` is the room in Dart's buffer: a record that does not fit
    /// gets the room it asks for, in a call of its own. Dart answers
    /// each request at once, and says no to each upgrade.
    /// Also gives the number of `100 Continue` lines.
    fn drive(stream: &[u8], cuts: &[usize], small: usize, lim: &Limits) -> (Seen, usize) {
        let now = Instant::now();
        let mut c = Conn::new(sock(), PEER, now);
        let mut seq = 0u32;
        let mut seen = Seen {
            recs: Vec::new(),
            slots: Vec::new(),
            fail: Vec::new(),
            body: false,
        };
        let mut fed = 0;
        let mut need = 0;
        for (i, &end) in cuts.iter().enumerate() {
            // like `read`: keep `rpos..rlen`, add the new bytes
            c.rbuf.drain(..c.rpos);
            c.rlen -= c.rpos;
            c.rpos = 0;
            c.rbuf.extend_from_slice(&stream[fed..end]);
            c.rlen = c.rbuf.len();
            fed = end;
            if i + 1 == cuts.len() {
                c.eof = true;
            }
            loop {
                if c.closing && c.body.is_none() {
                    // nothing is parsed after a fail or a close
                    let mut n = 0;
                    let r = c.parse(1, &mut [0; 64], &mut n, &mut seq, lim, b"", now);
                    assert!(r.is_ok() && n == 0 && c.slots.iter().all(|s| s.done));
                    // `fill` drops what a closing connection sends
                    c.rpos = c.rlen;
                    break;
                }
                let before = (seen.recs.len(), c.rpos, c.slots.len());
                let room = small.max(need);
                need = 0;
                let mut inb = vec![0u8; room];
                let mut n = 0;
                let res = c.parse(1, &mut inb, &mut n, &mut seq, lim, b"", now);
                assert!(c.rpos <= c.rlen && c.rlen <= c.rbuf.len());
                let mut p = 0;
                let mut upgrade = false;
                while p < n {
                    assert!(!upgrade, "a request after an upgrade request");
                    let (rec, len) = decode(&inb[p..n]);
                    upgrade = rec.flags & 2 != 0;
                    seen.recs.push(rec);
                    p += len;
                }
                assert_eq!(p, n);
                if let Err(size) = res {
                    assert!(size > room - n);
                    need = size;
                }
                // Dart answers: plain answers, no upgrade
                for s in c.slots.iter_mut().filter(|s| !s.done) {
                    seen.slots.push((s.close, s.head, s.http10, s.key.clone()));
                    s.done = true;
                }
                c.pump();
                assert!(c.slots.is_empty() && !c.upgrading);
                let after = (seen.recs.len(), c.rpos, c.slots.len());
                if need == 0 && after == before {
                    break;
                }
            }
        }
        assert_eq!(seen.recs.len(), seen.slots.len());
        // only the last request may close
        let first_close = seen.slots.iter().position(|s| s.0);
        assert!(first_close.is_none_or(|i| i + 1 == seen.slots.len()));
        // the write buffer: some `100 Continue`, then maybe the answer
        // of a bad request. Nothing else, and nothing after it.
        let mut w = &c.wbuf[..];
        let mut conts = 0;
        while let Some(rest) = w.strip_prefix(CONTINUE) {
            w = rest;
            conts += 1;
        }
        if !w.is_empty() {
            assert!(w.starts_with(b"HTTP/1.1 4") || w.starts_with(b"HTTP/1.1 5"));
            assert!(c.closing);
        }
        seen.fail = w.to_vec();
        seen.body = c.body.is_some();
        if c.closing {
            assert!(seen.slots.last().is_some_and(|s| s.0) || !seen.fail.is_empty());
        }
        (seen, conts)
    }

    /// How the walk of [expected] ended.
    #[derive(Debug, PartialEq)]
    enum End {
        /// no more records can come (the end, a close, a body that is
        /// too big or not whole yet)
        Stop,
        /// a bad request: no more records, and a fail answer
        Reject,
    }

    /// The records Dart must get for `stream`, found request after
    /// request with httparse, [framing] and [dechunk], apart from `Conn`.
    /// Dart says no to each upgrade, so the walk goes on after one. Also
    /// gives the requests with `Expect: 100-continue` and a body.
    fn expected(stream: &[u8], lim: &Limits) -> (Vec<Rec>, usize, End) {
        let (mut out, mut asked, mut pos) = (Vec::new(), 0, 0);
        let end = loop {
            let s = &stream[pos..];
            if s.is_empty() {
                break End::Stop;
            }
            let mut hs = [httparse::EMPTY_HEADER; MAX_HEADERS];
            let mut req = httparse::Request::new(&mut hs);
            let head = match req.parse(s) {
                Ok(httparse::Status::Complete(h)) if h <= MAX_HEAD => h,
                Ok(httparse::Status::Partial) if s.len() <= MAX_HEAD => break End::Stop,
                _ => break End::Reject,
            };
            let want = want(&req);
            let rest = &s[head..];
            // a file upload (`multipart/form-data`, RFC 7578) has its own
            // limit. Content-Type comes once (RFC 9110 8.3). When a
            // client sends more, the last one counts.
            let form = values(&req, "content-type").last().is_some_and(|v| {
                let mime = v.split(|&c| c == b';').next().unwrap();
                ows(mime).eq_ignore_ascii_case(b"multipart/form-data")
            });
            let max = if form { lim.multipart } else { lim.body };
            let (body, used) = match want.framing {
                Framing::Reject => break End::Reject,
                Framing::Len(n) => {
                    let n = n as usize;
                    asked += usize::from(want.expect && n > 0);
                    if n > max || rest.len() < n {
                        break End::Stop;
                    }
                    (rest[..n].to_vec(), n)
                }
                Framing::Chunked => {
                    asked += usize::from(want.expect);
                    match dechunk(rest) {
                        Some((b, _)) if b.len() > max => break End::Stop,
                        Some(x) => x,
                        None => break End::Stop,
                    }
                }
            };
            out.push(Rec {
                flags: u32::from(want.http10) | (u32::from(want.key.is_some()) << 1),
                method: req.method.unwrap().as_bytes().to_vec(),
                target: escaped(req.path.unwrap()),
                headers: req
                    .headers
                    .iter()
                    .map(|h| (h.name.to_ascii_lowercase().into_bytes(), h.value.to_vec()))
                    .collect(),
                body,
            });
            pos += head + used;
            if want.close {
                break End::Stop;
            }
        };
        (out, asked, end)
    }

    /// A chunked body at the start of `s` (RFC 9112 7.1): the body and the
    /// bytes used. None when it is bad or not whole. Chunk size lines go
    /// to httparse. Like the server, a size line over [CHUNK_LINE] bytes
    /// and a trailer over [MAX_HEAD] bytes are bad: stricter on purpose.
    fn dechunk(s: &[u8]) -> Option<(Vec<u8>, usize)> {
        let (mut body, mut p) = (Vec::new(), 0);
        loop {
            let Ok(httparse::Status::Complete((used, size))) = httparse::parse_chunk_size(&s[p..])
            else {
                return None;
            };
            if used > CHUNK_LINE {
                return None;
            }
            p += used;
            if size == 0 {
                // no trailer: an empty line. Else lines up to an empty one.
                if s[p..].starts_with(b"\r\n") {
                    return Some((body, p + 2));
                }
                let at = s[p..].windows(4).position(|w| w == b"\r\n\r\n")?;
                return (at + 4 <= MAX_HEAD).then_some((body, p + at + 4));
            }
            let n = usize::try_from(size).ok()?;
            let data = s.get(p..p.checked_add(n)?.checked_add(2)?)?;
            if !data.ends_with(b"\r\n") {
                return None;
            }
            body.extend_from_slice(&data[..n]);
            p += n + 2;
        }
    }

    /// Many requests on one connection (pipelined, with bodies). Byte 0
    /// picks the read pieces, byte 1 the room in Dart's buffer, byte 2 the
    /// body limit, the rest is what the client sends. All at once or in
    /// pieces, with a big or a small buffer: Dart must get the same.
    pub fn conn(data: &[u8]) {
        let [seed, small, max, stream @ ..] = data else {
            return;
        };
        let lim = Limits {
            body: usize::from(*max) * 64,
            multipart: usize::from(*max) * 128,
            ..default_limits()
        };
        let big = REQ_HEAD + 4 * stream.len() + EXTERNAL_MIN;
        let (want, conts) = drive(stream, &[stream.len()], big, &lim);
        // the records match a walk of the stream that does not use `Conn`
        let (exp, asked, walk) = expected(stream, &lim);
        assert!(want.recs.len() >= exp.len(), "records are missing");
        assert_eq!(
            want.recs[..exp.len()],
            exp[..],
            "a record is not what the client sent"
        );
        assert_eq!(
            want.recs.len(),
            exp.len(),
            "a record after the end of the walk"
        );
        // a `100 Continue` only for a request that asked for it and has a
        // body. How many go out also hangs on how the bytes came in and on
        // which requests share one `parse` call, so this is an upper
        // bound, not an exact count.
        assert!(
            conts <= asked,
            "{conts} `100 Continue` for {asked} requests"
        );
        if walk == End::Reject {
            assert!(!want.fail.is_empty(), "a bad request got no answer");
        }
        let mut cuts = Vec::new();
        let (mut end, mut rng) = (0, u32::from(*seed) | 1);
        while end < stream.len() {
            end = (end + 1 + next(&mut rng) % 128).min(stream.len());
            cuts.push(end);
        }
        if cuts.is_empty() {
            return;
        }
        let small = usize::from(*small) * 4;
        for (cuts, room, what) in [
            (&[stream.len()][..], small, "small buffer"),
            (&cuts[..], big, "in pieces"),
            (&cuts[..], small, "in pieces, small buffer"),
        ] {
            let (seen, conts) = drive(stream, cuts, room, &lim);
            assert_eq!(seen, want, "{what}");
            assert!(
                conts <= asked,
                "{what}: {conts} `100 Continue` for {asked} requests"
            );
        }
    }

    /// What one HTTP/2 connection gave Dart and the client.
    #[derive(Debug, PartialEq)]
    struct H2Seen {
        recs: Vec<Rec>,
        /// all bytes the server wrote
        out: Vec<u8>,
        /// the streamed body records: (seq, kind, status, bytes)
        body: Vec<(u32, u32, u16, Vec<u8>)>,
    }

    /// Feeds `stream` (after the preface) to one `Conn` with HTTP/2 on,
    /// cut at `cuts`. Request bodies may have `max` bytes. With `answer`,
    /// Dart answers each request with a body of that many bytes: whole
    /// (kind 0) or streamed in 3 parts (kind 6, 7, 8, some with trailers),
    /// in turn. A streamed request body gets each part acked at once
    /// (BODY_ACK), and its answer at its BODY_END.
    fn drive_h2(
        stream: &[u8],
        cuts: &[usize],
        small: usize,
        max: usize,
        answer: Option<usize>,
    ) -> H2Seen {
        let lim = Limits {
            http2: true,
            max_streams: 8,
            body: max,
            multipart: 2 * max,
            ..default_limits()
        };
        let now = Instant::now();
        let mut c = Conn::new(sock(), PEER, now);
        c.sniff = true;
        let (mut seq, mut fed, mut need, mut streamed) = (0u32, 0, 0, 0u64);
        let date = b"date: Thu, 01 Jan 2026 00:00:00 GMT\r\n";
        let mut seen = H2Seen {
            recs: Vec::new(),
            out: Vec::new(),
            body: Vec::new(),
        };
        // seqs whose body streams (flag bit 4), and those that ended
        let (mut open, mut ended) = (
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
        );
        let answer_one = |h2: &mut H2, seq: u32, w: &mut Vec<u8>| {
            let size = answer.unwrap();
            let body = vec![b'x'; size];
            if seq.is_multiple_of(2) {
                h2.answer(seq, 200, b"X-A: 1\r\n", &body, date, w);
            } else if h2.start(seq, 200, b"", u64::from(seq), &body[..size / 3], date, w) {
                let trailers: &[u8] = if seq % 4 == 3 {
                    b"Grpc-Status: 0\r\n:bad: 1\r\nconnection: x\r\n"
                } else {
                    b""
                };
                h2.part(seq, &body[size / 3..], false, true, b"", w);
                h2.part(seq, b"", true, seq % 4 != 1, trailers, w);
            }
        };
        for &end in cuts {
            c.rbuf.drain(..c.rpos);
            c.rlen -= c.rpos;
            c.rpos = 0;
            c.rbuf.extend_from_slice(&stream[fed..end]);
            c.rlen = c.rbuf.len();
            fed = end;
            loop {
                let before = (seen.recs.len(), c.rpos);
                let room = small.max(need);
                need = 0;
                let mut inb = vec![0u8; room];
                let mut n = 0;
                let res = c.parse(1, &mut inb, &mut n, &mut seq, &lim, date, now);
                assert!(c.rpos <= c.rlen && c.rlen <= c.rbuf.len());
                let mut p = 0;
                while p < n {
                    let (rec, len) = decode(&inb[p..n]);
                    // flag bit 3, and bit 2 for a big body or bit 4 for
                    // a streamed one (no body in the record then)
                    assert_eq!(rec.flags & !(4 | 16), 8, "flags {}", rec.flags);
                    assert!(rec.flags & 16 == 0 || rec.body.is_empty());
                    assert!(!rec.method.is_empty() && !rec.target.is_empty());
                    assert!(rec.target.is_ascii() && rec.body.len() <= 2 * max);
                    for (name, value) in &rec.headers {
                        assert!(!name.iter().any(u8::is_ascii_uppercase), "{name:?}");
                        assert!(!value.iter().any(|&b| b == 0 || b == b'\r' || b == b'\n'));
                    }
                    let seq = u64::from_le_bytes(inb[p..p + 8].try_into().unwrap()) as u32;
                    if rec.flags & 16 != 0 {
                        open.insert(seq);
                    } else if let (Some(_), Some(h2)) = (answer, c.h2.as_mut()) {
                        answer_one(h2, seq, &mut c.wbuf);
                    }
                    seen.recs.push(rec);
                    p += len;
                }
                assert_eq!(p, n);
                if let Err(size) = res {
                    assert!(size > room - n);
                    need = size;
                }
                // write all that may go: the client reads everything
                if let Some(h2) = c.h2.as_mut() {
                    // streamed bodies: only after their record, nothing
                    // after the end, one end each
                    for (seq, kind, code, data) in std::mem::take(&mut h2.to_dart) {
                        assert!(open.contains(&seq) && !ended.contains(&seq));
                        if kind == BODY_END {
                            ended.insert(seq);
                            if answer.is_some() && code == 0 {
                                answer_one(h2, seq, &mut c.wbuf);
                            }
                        } else {
                            assert!(kind == crate::BODY_PART && !data.is_empty() && code == 0);
                            if answer.is_some() {
                                h2.ack(seq, data.len() as u64, &mut c.wbuf);
                            }
                        }
                        seen.body.push((seq, kind, code, data));
                    }
                    loop {
                        h2.send(&mut c.wbuf, c.wpos, c.written, &mut c.marks);
                        c.written += c.wbuf.len() as u64;
                        seen.out.append(&mut c.wbuf);
                        if !h2.can_send() {
                            break;
                        }
                    }
                    // STREAM_SENT: never more bytes than the answers have
                    streamed += c.marks.drain(..).map(|m| m.2).sum::<u64>();
                    assert!(streamed <= (answer.unwrap_or(0) * seen.recs.len()) as u64);
                    h2.gone.clear();
                    h2.gone_streams.clear();
                }
                c.paused = false;
                if need == 0 && (seen.recs.len(), c.rpos) == before {
                    break;
                }
            }
        }
        assert!(c.h2.as_ref().is_none_or(|h| h.held.is_none()));
        // a body that still streams gets its end when the connection dies
        if let Some(h2) = c.h2.as_ref() {
            for seq in h2.streaming() {
                assert!(open.contains(&seq) && ended.insert(seq));
            }
        }
        assert_eq!(open, ended, "each streamed body ends once");
        // parts that waited for Dart are joined, so how the bytes came in
        // changes the part sizes: compare one part per stream
        let mut joined: Vec<(u32, u32, u16, Vec<u8>)> = Vec::new();
        for rec in std::mem::take(&mut seen.body) {
            let at = joined
                .iter()
                .position(|j| j.0 == rec.0 && j.1 == crate::BODY_PART && rec.1 == crate::BODY_PART);
            match at {
                Some(i) => joined[i].3.extend_from_slice(&rec.3),
                None => joined.push(rec),
            }
        }
        seen.body = joined;
        seen
    }

    /// Checks the frames the server wrote: whole and valid, no frame
    /// after a GOAWAY with an error, and each stream ends once.
    fn check_h2_out(out: &[u8]) {
        use crate::h2::frame;
        let mut p = 0;
        let mut ended = std::collections::HashSet::new();
        let mut dead = false;
        while p < out.len() {
            let h = frame::read_head(&out[p..]).expect("a whole frame head");
            let end = p + frame::HEAD + h.len;
            assert!(end <= out.len(), "a cut frame");
            assert!(!dead, "a frame after GOAWAY with an error");
            let f = frame::parse(&h, &out[p + frame::HEAD..end]).expect("a valid frame");
            match f {
                crate::h2::frame::Frame::GoAway { code, .. } => dead = code != 0,
                crate::h2::frame::Frame::Data { end, .. }
                | crate::h2::frame::Frame::Headers {
                    end_stream: end, ..
                } => {
                    assert!(!ended.contains(&h.id), "a frame after END_STREAM");
                    if end {
                        ended.insert(h.id);
                    }
                }
                _ => {}
            }
            p = end;
        }
    }

    /// HTTP/2: random bytes after the preface and an empty SETTINGS, so
    /// most of them are frames. Byte 0 picks the read pieces, byte 1 the
    /// room in Dart's buffer, byte 2 the answer size and the body limit.
    /// When Dart does not answer, the output hangs only on the bytes, not
    /// on how they came in: all at once or in pieces must give the same.
    /// With answers, the frames must be valid.
    pub fn h2_conn(data: &[u8]) {
        let [seed, small, size, rest @ ..] = data else {
            return;
        };
        let mut stream = crate::h2::PREFACE.to_vec();
        crate::h2::frame::write(&mut stream, crate::h2::frame::SETTINGS, 0, 0, b"");
        if seed & 1 == 1 {
            // gRPC requests on streams 1 and 3: their bodies stream to Dart
            for (id, ct) in [
                (1, &b"application/grpc"[..]),
                (3, b"Application/GRPC+proto"),
            ] {
                let mut block = Vec::new();
                for (n, v) in [
                    (&b":method"[..], &b"POST"[..]),
                    (b":scheme", b"http"),
                    (b":path", b"/a.B/C"),
                    (b"content-type", ct),
                    (b"te", b"trailers"),
                ] {
                    crate::h2::hpack::encode_field(n, v, &mut block);
                }
                let flags = crate::h2::frame::END_HEADERS;
                crate::h2::frame::write(&mut stream, crate::h2::frame::HEADERS, flags, id, &block);
            }
        }
        stream.extend_from_slice(rest);
        let big = REQ_HEAD + 4 * stream.len() + MAX_HEAD;
        let max = usize::from(*size) * 256;
        let want = drive_h2(&stream, &[stream.len()], big, max, None);
        check_h2_out(&want.out);
        let mut cuts = Vec::new();
        let (mut end, mut rng) = (0, u32::from(*seed) | 1);
        while end < stream.len() {
            end = (end + 1 + next(&mut rng) % 128).min(stream.len());
            cuts.push(end);
        }
        let small = usize::from(*small) * 4;
        for (cuts, room, what) in [
            (&[stream.len()][..], small, "small buffer"),
            (&cuts[..], big, "in pieces"),
            (&cuts[..], small, "in pieces, small buffer"),
        ] {
            assert_eq!(drive_h2(&stream, cuts, room, max, None), want, "{what}");
        }
        let size = usize::from(*size) * 1024;
        for cuts in [&[stream.len()][..], &cuts[..]] {
            let seen = drive_h2(&stream, cuts, big, max, Some(size));
            check_h2_out(&seen.out);
        }
    }

    /// `aead::open` on untrusted cookie bytes. Byte 0 is the aad length,
    /// bytes 1 and 2 pick a byte to change, then aad, then the data.
    pub fn aead_open(data: &[u8]) {
        let [alen, f0, f1, rest @ ..] = data else {
            return;
        };
        let key = [7u8; aead::KEY_LEN];
        let (aad, data) = rest.split_at(usize::from(*alen).min(rest.len()));
        // random data: no panic, and the FFI call agrees
        let plain_len = data.len().checked_sub(aead::NONCE_LEN + aead::TAG_LEN);
        let mut out = vec![0u8; plain_len.unwrap_or(0)];
        let ok = plain_len.is_some() && aead::open(&key, aad, data, &mut out);
        assert_eq!(ffi_open(&key, aad, data), ok.then_some(out));
        // seal the data as plain text: open gives it back
        let nonce: [u8; aead::NONCE_LEN] =
            std::array::from_fn(|i| aad.get(i).copied().unwrap_or(0));
        let mut sealed = vec![0u8; data.len() + aead::NONCE_LEN + aead::TAG_LEN];
        aead::seal_with_nonce(&key, &nonce, aad, data, &mut sealed);
        assert_eq!(ffi_open(&key, aad, &sealed).as_deref(), Some(data));
        // one changed byte of the sealed data or the aad: open fails
        let mut aad = aad.to_vec();
        let at = usize::from(*f0) << 8 | usize::from(*f1);
        let at = at % (sealed.len() + aad.len());
        if at < sealed.len() {
            sealed[at] ^= 1 << (f1 % 8);
        } else {
            aad[at - sealed.len()] ^= 1 << (f1 % 8);
        }
        assert_eq!(ffi_open(&key, &aad, &sealed), None, "a changed byte opened");
        // cut data never opens
        for cut in [0, 1, aead::NONCE_LEN + aead::TAG_LEN - 1] {
            assert_eq!(ffi_open(&key, &aad, &sealed[..cut.min(sealed.len())]), None);
        }
        // the FFI seal writes exactly `plain + 28` bytes, then it opens
        let mut out = vec![0u8; data.len() + aead::NONCE_LEN + aead::TAG_LEN];
        let r = unsafe {
            aead::dox_aes_gcm_seal(
                key.as_ptr(),
                aad.as_ptr(),
                aad.len() as i64,
                data.as_ptr(),
                data.len() as i64,
                out.as_mut_ptr(),
            )
        };
        assert_eq!(r, 0);
        assert_eq!(ffi_open(&key, &aad, &out).as_deref(), Some(data));
        // bad arguments from Dart: a null pointer with a length, or a
        // negative length. -1, and nothing is read or written.
        let (k, nul, neg) = (key.as_ptr(), std::ptr::null(), -1 - i64::from(*f0));
        let (d, n) = (out.as_ptr(), out.len() as i64);
        let mut o = vec![0u8; out.len()];
        let o = o.as_mut_ptr();
        unsafe {
            assert_eq!(aead::dox_aes_gcm_open(nul, d, 0, d, n, o), -1);
            assert_eq!(aead::dox_aes_gcm_open(k, nul, 1, d, n, o), -1);
            assert_eq!(aead::dox_aes_gcm_open(k, d, neg, d, n, o), -1);
            assert_eq!(aead::dox_aes_gcm_open(k, d, 0, nul, n, o), -1);
            assert_eq!(aead::dox_aes_gcm_open(k, d, 0, d, neg, o), -1);
            if n > 28 {
                let null_out = std::ptr::null_mut();
                assert_eq!(aead::dox_aes_gcm_open(k, d, 0, d, n, null_out), -1);
            }
            assert_eq!(aead::dox_aes_gcm_seal(nul, d, 0, d, 0, o), -1);
            assert_eq!(aead::dox_aes_gcm_seal(k, d, neg, d, 0, o), -1);
            assert_eq!(aead::dox_aes_gcm_seal(k, d, 0, d, neg, o), -1);
            assert_eq!(aead::dox_aes_gcm_seal(k, d, 0, nul, 1, o), -1);
            assert_eq!(
                aead::dox_aes_gcm_seal(k, d, 0, d, 0, std::ptr::null_mut()),
                -1
            );
        }
    }

    /// `dox_aes_gcm_open` with an out buffer of exactly `data - 28` bytes
    /// (the sanitizer sees a write past it). None when it fails.
    fn ffi_open(key: &[u8; aead::KEY_LEN], aad: &[u8], data: &[u8]) -> Option<Vec<u8>> {
        let mut out = vec![0u8; data.len().saturating_sub(aead::NONCE_LEN + aead::TAG_LEN)];
        let r = unsafe {
            aead::dox_aes_gcm_open(
                key.as_ptr(),
                aad.as_ptr(),
                aad.len() as i64,
                data.as_ptr(),
                data.len() as i64,
                out.as_mut_ptr(),
            )
        };
        if r < 0 {
            assert_eq!(r, -1);
            assert!(out.iter().all(|&b| b == 0), "bytes left after a fail");
            return None;
        }
        assert_eq!(r as usize, out.len());
        Some(out)
    }
}
