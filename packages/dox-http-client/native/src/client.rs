//! The HTTP client (HTTP/1.1 and HTTP/2). It runs on the thread of the Dart
//! isolate.
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
//! Request record (Dart to Rust, little endian), [REQ_HEAD] bytes, then a
//! payload. The kind says what the record is:
//!   0  u32 length of the record
//!   4  u64 id
//!   12 u8  family (4 or 6)
//!   13 u8  flags: [HEAD_ONLY], [AGAIN], [TLS], [DECODE], [STREAM], [UPLOAD],
//!          [CHUNKED]
//!   14 u16 port
//!   16 16 bytes address (IPv4: the first 4)
//!   32 u32 timeout in milliseconds (0: none)
//!   36 u16 length of the server name (the host of an `https://` URL)
//!   38 u8  kind: [Q_NEW] ...
//! - [Q_NEW]: a new request. The payload is the server name and then the raw
//!   HTTP request (head and body). With [UPLOAD] it is the head only: the
//!   body comes in [Q_PART] records.
//! - [Q_PART]: bytes of the request body of request `id`. Rust does the
//!   framing: chunks for HTTP/1.1 with [CHUNKED], DATA frames for HTTP/2.
//!   Only after the first [R_PROGRESS] record of the request. The payload
//!   is the bytes.
//! - [Q_END]: the body of request `id` is whole. Byte 16 is 1 when Dart
//!   stops the upload before its end: the connection is not used again.
//! - [Q_CREDIT]: u64 at 16: the total bytes of the answer body of request
//!   `id` that Dart has used so far.
//! - [Q_CANCEL]: close the connection of request `id` and forget it.
//!
//! Only [Q_NEW] uses the address, the flags, the timeout and the name.
//!
//! Response record (Rust to Dart), [RES_HEAD] bytes, then the header lines
//! (`name:value\n`, names in lower case) and then the body:
//!   0  u32 length of the record
//!   4  u64 id
//!   12 u16 status (0: the request failed)
//!   14 u16 error ([E_CONNECT] ...) when the status is 0
//!   16 u32 length of the header lines
//!   20 u8  kind: [R_WHOLE] ...
//!   21 3 bytes unused
//! - [R_WHOLE]: the whole answer, or a failure (status 0). A failure after
//!   [R_HEAD] ends the body of that request.
//! - [R_HEAD]: the head of a streamed answer ([STREAM]): status and header
//!   lines, no body.
//! - [R_PART]: bytes of the body of a streamed answer (the body field).
//! - [R_END]: the body of a streamed answer is whole.
//! - [R_PROGRESS]: the body field has a u64: the total bytes of the request
//!   (the head and the body as Dart gave them) that went out. While chunks
//!   wait, the number can be a little too small. The first one also says
//!   that the connection works.
//!
//! With [DECODE], a body with `content-encoding: gzip` or `br` comes to
//! Dart decoded, without its `content-encoding` and `content-length`
//! lines. The body limit is for the decoded bytes.
//!
//! Streamed bodies ([STREAM]): the answer body comes in parts as it is
//! read. Rust stops reading the socket when [HIGH] bytes are in Rust or
//! sent to Dart and not yet used (see [Q_CREDIT]). The timeout of a
//! streamed request ends with its [R_HEAD]. A request with [UPLOAD] is
//! never sent again.
//!
//! TLS (`https://`): rustls, with the root certificates of Mozilla
//! (webpki-roots) and the ones Dart gives. The server name is checked
//! against the certificate. TLS 1.2 and 1.3.
//!
//! HTTP/2 (see `h2.rs`): an `https://` connection offers `h2` and
//! `http/1.1` (ALPN), when Dart did not turn HTTP/2 off. The request waits
//! for the end of the TLS handshake, because only then the protocol is
//! known. With `h2`, the requests to one address share the connection:
//! each one is a stream. Dart sees no difference: Rust makes the HTTP/2
//! head from the HTTP/1.1 head of the request record.
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
use std::io::{BufRead, ErrorKind, Read, Write};
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

use crate::{POST, post, rd32, rd64};

mod h2;

const REQ_HEAD: usize = 40;
const RES_HEAD: usize = 24;
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
/// A streamed answer holds at most this many bytes that Dart has not used
/// yet, in Rust and in the records for Dart.
const HIGH: usize = 256 * 1024;
/// The most body bytes in one [R_PART] record.
const PART_MAX: usize = 64 * 1024;
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
/// Request flag: the body in the [Q_PART] records has no length. HTTP/1.1
/// sends it in chunks.
const CHUNKED: u8 = 64;
/// Request flag: decode a gzip or brotli answer body.
const DECODE: u8 = 8;
/// Request flag: the answer comes in parts (head, body parts, end).
const STREAM: u8 = 16;
/// Request flag: the request body comes in [Q_PART] records.
const UPLOAD: u8 = 32;

/// Request kinds, see the top of this file.
const Q_NEW: u8 = 0;
const Q_PART: u8 = 1;
const Q_END: u8 = 2;
const Q_CREDIT: u8 = 3;
const Q_CANCEL: u8 = 4;

/// Response kinds, see the top of this file.
const R_WHOLE: u8 = 0;
const R_HEAD: u8 = 1;
const R_PART: u8 = 2;
const R_END: u8 = 3;
const R_PROGRESS: u8 = 4;

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
    /// The timeout of a request with [UPLOAD]. Its deadline starts again
    /// each time bytes of the request go out: a big body may need more
    /// time than the timeout.
    wait: Option<Duration>,
    bytes: Vec<u8>,
    /// the body comes in [Q_PART] records, until [Q_END]
    more: bool,
    /// Dart stopped the upload: the connection is not used again
    aborted: bool,
    /// bytes of chunk framing that Rust put in `bytes` so far ([CHUNKED])
    framing: u64,
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

/// The `content-encoding` of an answer that [decode] can read.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Coding {
    /// none, or one that stays as it is
    Raw,
    Gzip,
    Brotli,
}

/// Decodes a body. The result has at most `max` bytes: a small body can
/// decode to a very big one.
fn decode(coding: Coding, body: &[u8], max: usize) -> Result<Vec<u8>, u16> {
    let mut out = Vec::new();
    let limit = max as u64 + 1;
    match coding {
        Coding::Raw => Ok(body.to_vec()),
        Coding::Gzip => {
            // A gzip body may have more than one member. Bytes after the
            // last member that are not gzip (padding) are dropped.
            let mut rest = body;
            loop {
                let mut member = flate2::bufread::GzDecoder::new(rest);
                let read = (&mut member)
                    .take(limit - out.len() as u64)
                    .read_to_end(&mut out);
                if out.len() > max {
                    return Err(E_BIG);
                }
                read.map_err(|_| E_BAD)?;
                rest = member.into_inner();
                if !rest.starts_with(&[0x1f, 0x8b]) {
                    return Ok(out);
                }
            }
        }
        Coding::Brotli => {
            let read = brotli_decompressor::Decompressor::new(body, 4096)
                .take(limit)
                .read_to_end(&mut out);
            if out.len() > max {
                return Err(E_BIG);
            }
            read.map(|_| out).map_err(|_| E_BAD)
        }
    }
}

/// The header lines without `content-encoding` and `content-length`: they
/// are not true for a decoded body.
fn decoded_lines(lines: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(lines.len());
    for line in lines.split_inclusive(|&c| c == b'\n') {
        if !line.starts_with(b"content-encoding:") && !line.starts_with(b"content-length:") {
            out.extend_from_slice(line);
        }
    }
    out
}

/// The input of a decoder that waits for more bytes: with no byte left it
/// gives `WouldBlock`, or the end when `eof` is set.
#[derive(Default)]
struct Feed {
    buf: Vec<u8>,
    pos: usize,
    eof: bool,
}

impl Feed {
    fn push(&mut self, bytes: &[u8]) {
        // The bytes the decoder took go away. The decoder stops when Dart
        // has no room, so it does not often take all of them.
        if self.pos > 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    fn left(&self) -> usize {
        self.buf.len() - self.pos
    }
}

impl Read for Feed {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = out.len().min(self.left());
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        if n == 0 && !out.is_empty() && !self.eof {
            return Err(ErrorKind::WouldBlock.into());
        }
        Ok(n)
    }
}

impl BufRead for Feed {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if self.left() == 0 && !self.eof {
            return Err(ErrorKind::WouldBlock.into());
        }
        Ok(&self.buf[self.pos..])
    }

    fn consume(&mut self, n: usize) {
        self.pos = (self.pos + n).min(self.buf.len());
    }
}

enum GzState {
    /// inside a member
    Member(Box<flate2::bufread::GzDecoder<Feed>>),
    /// after a member: the next 2 bytes say if another member comes
    Between(Feed),
    /// padding after the last member: dropped
    Padding(Feed),
}

type BrotliDec = brotli_decompressor::BrotliState<
    brotli_decompressor::StandardAlloc,
    brotli_decompressor::StandardAlloc,
    brotli_decompressor::StandardAlloc,
>;

enum Pull {
    Data(usize),
    /// more input must come
    Wait,
    /// the coded body is whole
    End,
}

/// Decodes a gzip or brotli body in steps, as the bytes come. `pull` gives
/// at most the size of its buffer: a small input can not make a big
/// output at once.
enum Inflate {
    Gzip(GzState),
    Brotli {
        state: Box<BrotliDec>,
        feed: Feed,
        total: usize,
        done: bool,
        /// bytes came after the end of the brotli body
        extra: bool,
    },
}

impl Inflate {
    fn new(coding: Coding) -> Option<Inflate> {
        match coding {
            Coding::Raw => None,
            Coding::Gzip => Some(Inflate::Gzip(GzState::Member(Box::new(
                flate2::bufread::GzDecoder::new(Feed::default()),
            )))),
            Coding::Brotli => Some(Inflate::Brotli {
                state: Box::new(BrotliDec::new(
                    Default::default(),
                    Default::default(),
                    Default::default(),
                )),
                feed: Feed::default(),
                total: 0,
                done: false,
                extra: false,
            }),
        }
    }

    fn feed_mut(&mut self) -> &mut Feed {
        match self {
            Inflate::Gzip(GzState::Member(d)) => d.get_mut(),
            Inflate::Gzip(GzState::Between(f) | GzState::Padding(f)) => f,
            Inflate::Brotli { feed, .. } => feed,
        }
    }

    /// Gives it coded bytes. `end`: no more come.
    fn push(&mut self, bytes: &[u8], end: bool) {
        if let Inflate::Brotli { done, extra, .. } = self {
            *extra |= *done && !bytes.is_empty();
        }
        let feed = self.feed_mut();
        feed.push(bytes);
        feed.eof |= end;
    }

    /// The coded bytes it has not read yet.
    fn held(&self) -> usize {
        match self {
            Inflate::Gzip(GzState::Member(d)) => d.get_ref().left(),
            Inflate::Gzip(GzState::Between(f) | GzState::Padding(f)) => f.left(),
            Inflate::Brotli { feed, .. } => feed.left(),
        }
    }

    fn pull(&mut self, out: &mut [u8]) -> Result<Pull, u16> {
        match self {
            Inflate::Gzip(state) => loop {
                match state {
                    GzState::Member(d) => match d.read(out) {
                        Ok(0) => {
                            let GzState::Member(d) =
                                std::mem::replace(state, GzState::Between(Feed::default()))
                            else {
                                return Err(E_BAD);
                            };
                            *state = GzState::Between(d.into_inner());
                        }
                        Ok(n) => return Ok(Pull::Data(n)),
                        Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(Pull::Wait),
                        Err(_) => return Err(E_BAD),
                    },
                    GzState::Between(feed) => {
                        let rest = &feed.buf[feed.pos..];
                        if rest.len() < 2 && !feed.eof {
                            if rest.first().is_some_and(|&b| b != 0x1f) {
                                feed.pos = feed.buf.len();
                                let feed = std::mem::take(feed);
                                *state = GzState::Padding(feed);
                                continue;
                            }
                            return Ok(Pull::Wait);
                        }
                        if rest.starts_with(&[0x1f, 0x8b]) {
                            let feed = std::mem::take(feed);
                            *state =
                                GzState::Member(Box::new(flate2::bufread::GzDecoder::new(feed)));
                        } else {
                            feed.pos = feed.buf.len();
                            let feed = std::mem::take(feed);
                            *state = GzState::Padding(feed);
                        }
                    }
                    GzState::Padding(feed) => {
                        feed.pos = feed.buf.len();
                        return Ok(if feed.eof { Pull::End } else { Pull::Wait });
                    }
                }
            },
            Inflate::Brotli {
                state,
                feed,
                total,
                done,
                extra,
            } => {
                if out.is_empty() {
                    return Ok(Pull::Wait);
                }
                let mut avail_in = feed.left();
                let mut in_at = feed.pos;
                let mut avail_out = out.len();
                let mut out_at = 0;
                let result = brotli_decompressor::BrotliDecompressStream(
                    &mut avail_in,
                    &mut in_at,
                    &feed.buf,
                    &mut avail_out,
                    &mut out_at,
                    out,
                    total,
                    state,
                );
                feed.pos = in_at;
                use brotli_decompressor::BrotliResult;
                match result {
                    BrotliResult::ResultFailure => Err(E_BAD),
                    BrotliResult::ResultSuccess => {
                        *done = true;
                        if out_at > 0 {
                            Ok(Pull::Data(out_at))
                        } else if *extra || feed.left() > 0 {
                            Err(E_BAD)
                        } else if feed.eof {
                            Ok(Pull::End)
                        } else {
                            Ok(Pull::Wait)
                        }
                    }
                    BrotliResult::NeedsMoreOutput => Ok(Pull::Data(out_at)),
                    BrotliResult::NeedsMoreInput if out_at > 0 => Ok(Pull::Data(out_at)),
                    BrotliResult::NeedsMoreInput if feed.eof => Err(E_BAD),
                    BrotliResult::NeedsMoreInput => Ok(Pull::Wait),
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
    coding: Coding,
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
        let mut coding = Coding::Raw;
        let mut codings = 0;
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
            } else if h.name.eq_ignore_ascii_case("content-encoding") {
                let v = h.value.trim_ascii();
                codings += 1;
                if v.eq_ignore_ascii_case(b"gzip") || v.eq_ignore_ascii_case(b"x-gzip") {
                    coding = Coding::Gzip;
                } else if v.eq_ignore_ascii_case(b"br") {
                    coding = Coding::Brotli;
                }
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
        // more than one coding (rare) stays as it is
        if codings != 1 {
            coding = Coding::Raw;
        }
        return Ok(Some(Head {
            status,
            start,
            lines,
            keep,
            body,
            coding,
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
    /// the state of a streamed answer, from its head on
    down: Option<Box<Down>>,
    /// bytes of the request that went out (head and body), and how many
    /// of them Dart knows
    wrote: u64,
    told: u64,
    /// The TLS handshake is not done, and the server may pick HTTP/2: the
    /// request waits.
    wait_alpn: bool,
    /// the requests of an HTTP/2 connection. `req` is then not in use.
    h2: Option<Box<Mux>>,
}

/// An HTTP/2 connection: its protocol state and its requests by stream.
struct Mux {
    proto: h2::H2,
    subs: HashMap<u32, Sub>,
    /// it has no request, and `idle_since` of the connection says since
    /// when
    idle: bool,
}

/// One request on an HTTP/2 connection.
struct Sub {
    req: Req,
    /// bytes of the HTTP/1.1 head that Dart gave: [R_PROGRESS] counts them
    head_len: u64,
    /// 0: no head of the answer yet
    status: u16,
    lines: Vec<u8>,
    coding: Coding,
    /// the body of an answer that is not streamed
    body: Vec<u8>,
    /// the state of a streamed answer, from its head on
    down: Option<Box<Down>>,
    /// body bytes that the server got back as room (flow control)
    released: u64,
    /// the server ended the answer: Dart may still have to use bytes
    ended: bool,
}

impl Sub {
    /// Gives the server room for the body bytes that are used: the ones
    /// Dart used, or for a coded body the ones the decoder took.
    fn window(&mut self, proto: &mut h2::H2, stream: u32) {
        let Some(down) = &self.down else { return };
        let used = match &down.inflate {
            Some(inflate) => down.raw.saturating_sub(inflate.held() as u64),
            None => down.credit,
        };
        if used > self.released {
            proto.release(stream, used - self.released);
            self.released = used;
        }
    }
}

/// The `content-encoding` in header lines (`name:value\n`, lower case)
/// that [decode] can read. More than one coding stays as it is.
fn coding_of(lines: &[u8]) -> Coding {
    let mut found = Coding::Raw;
    let mut count = 0;
    for line in lines.split(|&c| c == b'\n') {
        if let Some(value) = line.strip_prefix(b"content-encoding:") {
            count += 1;
            let value = value.trim_ascii();
            if value.eq_ignore_ascii_case(b"gzip") || value.eq_ignore_ascii_case(b"x-gzip") {
                found = Coding::Gzip;
            } else if value.eq_ignore_ascii_case(b"br") {
                found = Coding::Brotli;
            }
        }
    }
    if count == 1 { found } else { Coding::Raw }
}

/// A name and a value of a request head.
type Field = (Vec<u8>, Vec<u8>);

/// The HTTP/2 fields of an HTTP/1.1 request head as Dart writes it, and
/// the length of that head. The headers that are only for an HTTP/1.1
/// connection are left out (RFC 9113 8.2.2).
fn h2_fields(bytes: &[u8]) -> Option<(usize, Vec<Field>)> {
    let len = bytes.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
    let mut lines = bytes[..len - 4]
        .split(|&c| c == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line));
    let mut first = lines.next()?.split(|&c| c == b' ');
    let method = first.next()?;
    let path = first.next()?;
    if method.is_empty() || path.is_empty() {
        return None;
    }
    let mut authority: &[u8] = b"";
    let mut rest: Vec<Field> = Vec::new();
    for line in lines {
        let colon = line.iter().position(|&c| c == b':')?;
        let name = line[..colon].to_ascii_lowercase();
        let value = line[colon + 1..].trim_ascii();
        match name.as_slice() {
            b"host" => authority = value,
            b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding"
            | b"upgrade" | b"te" => {}
            _ => rest.push((name, value.to_vec())),
        }
    }
    if authority.is_empty() {
        return None;
    }
    let mut fields: Vec<Field> = vec![
        (b":method".to_vec(), method.to_vec()),
        (b":scheme".to_vec(), b"https".to_vec()),
        (b":authority".to_vec(), authority.to_vec()),
        (b":path".to_vec(), path.to_vec()),
    ];
    fields.append(&mut rest);
    Some((len, fields))
}

/// A streamed answer that goes to Dart in parts.
struct Down {
    /// bytes in [R_PART] records so far, and the bytes Dart says it used
    sent: u64,
    credit: u64,
    /// decoded bytes in all: for the body limit
    total: usize,
    /// bytes of the body as the server sent them
    raw: u64,
    inflate: Option<Inflate>,
    /// the status of the answer: a bad body of a redirect is not an error
    redirect: bool,
    /// All the raw body bytes are read, and this many bytes came after
    /// them. The decoder may still wait for Dart to use bytes.
    raw_done: Option<usize>,
}

impl Down {
    /// Bytes Rust may still take for Dart before it stops reading.
    fn room(&self) -> usize {
        HIGH.saturating_sub((self.sent - self.credit.min(self.sent)) as usize)
    }
}

impl Conn {
    /// Writes what is left of the request and reads what the socket has.
    /// Returns the error when the connection failed, true when the server
    /// closed it, and true when a TLS connection ended with no TLS close.
    ///
    /// It reads at most `cap` bytes: a streamed answer that Dart does not
    /// use leaves the socket alone. With no byte left to read, no new event
    /// comes: the caller must call `io` again when `cap` is bigger.
    fn io(&mut self, cap: usize) -> (Option<u16>, bool, bool) {
        self.more = false;
        match self.tls.take() {
            Some(mut tls) => {
                let result = self.tls_io(&mut tls, cap);
                self.tls = Some(tls);
                result
            }
            None => {
                let (fail, eof) = self.plain_io(cap);
                (fail, eof, false)
            }
        }
    }

    /// How many bytes the next `io` reads at most.
    fn cap(&self) -> usize {
        let Some(down) = &self.down else {
            return READ_MAX;
        };
        // bytes in the read buffer and in the decoder are in Rust too
        let held = self.filled + down.inflate.as_ref().map_or(0, Inflate::held);
        READ_MAX.min(down.room().saturating_sub(held))
    }

    fn plain_io(&mut self, cap: usize) -> (Option<u16>, bool) {
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
                    self.wrote += n as u64;
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
            if self.filled - start >= cap {
                self.more = cap > 0;
                break;
            }
            room(&mut self.buf, self.filled);
            let end = self
                .buf
                .len()
                .min(self.filled + (cap - (self.filled - start)));
            match self.stream.read(&mut self.buf[self.filled..end]) {
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
    fn tls_io(&mut self, tls: &mut ClientConnection, cap: usize) -> (Option<u16>, bool, bool) {
        let bytes: &[u8] = match (&self.h2, &self.req) {
            (Some(mux), _) => &mux.proto.out,
            // the protocol is not known before the end of the handshake
            (None, Some(req)) if !self.wait_alpn => &req.bytes,
            _ => &[],
        };
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
                        self.wrote += n as u64;
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
            // The plaintext first: TLS does not take more records when it
            // holds a lot of it, and `cap` may have stopped the last read.
            // This is also after a failure: the answer may be whole before it.
            let drained = tls_drain(tls, &mut self.buf, &mut self.filled, start, cap);
            progress |= drained.progress;
            eof |= drained.eof;
            cut |= drained.cut;
            fail = fail.or(drained.fail);
            // also after a write that failed: the server may have answered
            // and closed before it read the whole request (eg. 413)
            if fail != Some(E_TLS) && !eof && cap > self.filled - start {
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
                let drained = tls_drain(tls, &mut self.buf, &mut self.filled, start, cap);
                progress |= drained.progress;
                eof |= drained.eof;
                cut |= drained.cut;
                fail = fail.or(drained.fail);
            }
            if fail.is_some() || !progress || (eof && !tls.wants_write()) {
                return (fail, eof, cut);
            }
            if self.filled - start >= cap {
                self.more = cap > 0;
                return (fail, eof, cut);
            }
        }
    }

    /// Gives Dart the body bytes that are in the read buffer, as far as it
    /// has room. Some(rest): the body is whole and `rest` bytes came after
    /// it. None: more must come, or Dart must use bytes first.
    fn body_step(
        &mut self,
        eof: bool,
        cut: bool,
        ended: u16,
        max: usize,
        ready: &mut VecDeque<Done>,
    ) -> Result<Option<usize>, u16> {
        let (Some(req), Some(head), Some(down)) =
            (self.req.as_ref(), self.head.as_mut(), self.down.as_mut())
        else {
            return Ok(None);
        };
        let id = req.id;
        if let Some(rest) = down.raw_done {
            let out = emit(down, id, &[], true, max, ready)?;
            // more bytes may have come after the body since then: the
            // connection is then not clean
            return Ok(out.then_some(rest.max(self.filled)));
        }
        // the raw body bytes that are here, and if they end the body
        let (out, raw_end, rest) = match &mut head.body {
            Body::Len(left) => {
                let take = (*left).min(self.filled);
                *left -= take;
                let out = emit(down, id, &self.buf[..take], *left == 0, max, ready)?;
                self.buf.copy_within(take..self.filled, 0);
                self.filled -= take;
                (out, *left == 0, self.filled)
            }
            Body::Chunked(ch) => {
                let end = ch.step(&self.buf[..self.filled], usize::MAX)?;
                let data = std::mem::take(&mut ch.out);
                let out = emit(down, id, &data, end.is_some(), max, ready)?;
                // the bytes before `pos` are out: use the room again
                self.buf.copy_within(ch.pos..self.filled, 0);
                self.filled -= ch.pos;
                let rest = end.map_or(0, |end| self.filled + ch.pos - end);
                ch.pos = 0;
                (out, end.is_some(), rest)
            }
            Body::Eof => {
                // the end of the body is the end of the connection: an end
                // with no TLS close, or a failure, may be a cut body
                if cut {
                    return Err(ended);
                }
                let out = emit(down, id, &self.buf[..self.filled], eof, max, ready)?;
                self.filled = 0;
                (out, eof, 0)
            }
        };
        if !raw_end && eof {
            return Err(ended);
        }
        if raw_end {
            down.raw_done = Some(rest);
        }
        Ok(out.then_some(rest))
    }
}

/// Moves the plaintext that `tls` has to the read buffer, up to `cap`
/// bytes since `start`.
fn tls_drain(
    tls: &mut ClientConnection,
    buf: &mut Vec<u8>,
    filled: &mut usize,
    start: usize,
    cap: usize,
) -> Drained {
    let mut out = Drained::default();
    while *filled - start < cap {
        room(buf, *filled);
        let end = buf.len().min(*filled + (cap - (*filled - start)));
        match tls.reader().read(&mut buf[*filled..end]) {
            // the server closed the TLS connection
            Ok(0) => {
                out.eof = true;
                break;
            }
            Ok(n) => {
                *filled += n;
                out.progress = true;
            }
            Err(e) => match e.kind() {
                ErrorKind::Interrupted => {}
                ErrorKind::WouldBlock => break,
                // the socket closed with no TLS close
                ErrorKind::UnexpectedEof => {
                    out.eof = true;
                    out.cut = true;
                    break;
                }
                _ => {
                    out.fail = Some(E_TLS);
                    break;
                }
            },
        }
    }
    out
}

/// What `tls_drain` found.
#[derive(Default)]
struct Drained {
    progress: bool,
    eof: bool,
    cut: bool,
    fail: Option<u16>,
}

/// One answer for Dart.
struct Done {
    id: u64,
    kind: u8,
    status: u16,
    error: u16,
    lines: Vec<u8>,
    body: Vec<u8>,
}

/// Puts a part of a streamed answer body in `ready` for Dart. Returns true
/// when the whole body is out (`end`: `raw` is the last of the body). The
/// decoded bytes are counted against `max` as they come.
fn emit(
    down: &mut Down,
    id: u64,
    raw: &[u8],
    end: bool,
    max: usize,
    ready: &mut VecDeque<Done>,
) -> Result<bool, u16> {
    let mut part = |down: &mut Down, bytes: &[u8]| -> Result<(), u16> {
        down.total += bytes.len();
        if down.total > max {
            return Err(E_BIG);
        }
        down.sent += bytes.len() as u64;
        ready.push_back(Done {
            id,
            kind: R_PART,
            status: 0,
            error: 0,
            lines: Vec::new(),
            body: bytes.to_vec(),
        });
        Ok(())
    };
    down.raw += raw.len() as u64;
    let Some(inflate) = &mut down.inflate else {
        for bytes in raw.chunks(PART_MAX) {
            part(down, bytes)?;
        }
        return Ok(end);
    };
    // an empty body is not coded
    if end && down.raw == 0 {
        return Ok(true);
    }
    inflate.push(raw, end);
    let mut out = vec![0u8; PART_MAX];
    loop {
        // Dart has no room: the coded bytes wait in the decoder
        let want = PART_MAX
            .min(down.room())
            .min((max - down.total).saturating_add(1));
        if want == 0 {
            return Ok(false);
        }
        let Some(inflate) = &mut down.inflate else {
            return Ok(false);
        };
        match inflate.pull(&mut out[..want])? {
            Pull::Data(n) => part(down, &out[..n])?,
            Pull::Wait => return Ok(false),
            Pull::End => return Ok(end),
        }
    }
}

/// Where a request is.
enum Spot {
    Conn(usize),
    /// a stream of an HTTP/2 connection
    Mux(usize, u32),
    /// in the queue of a host, at this place
    Queued(Key, usize),
}

/// Tells Dart how many bytes of request `id` went out. A report that Dart
/// did not read yet gets the new number.
fn push_progress(ready: &mut VecDeque<Done>, id: u64, wrote: u64) {
    let body = wrote.to_le_bytes().to_vec();
    if let Some(last) = ready.back_mut()
        && last.kind == R_PROGRESS
        && last.id == id
    {
        last.body = body;
        return;
    }
    ready.push_back(Done {
        id,
        kind: R_PROGRESS,
        status: 0,
        error: 0,
        lines: Vec::new(),
        body,
    });
}

/// The connections to one address (and one TLS name).
#[derive(Default)]
struct Host {
    idle: Vec<usize>,
    /// open connections, busy and idle
    count: usize,
    /// requests that wait for a free connection
    queue: VecDeque<Req>,
    /// the HTTP/2 connections: they are never in `idle`
    h2: Vec<usize>,
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
    /// The biggest body of a streamed answer. It comes in parts, so it has
    /// no limit when Dart gives none.
    stream_max: usize,
    /// an idle connection closes after this time. None: never.
    idle_timeout: Option<Duration>,
    /// when `run` next looks for idle connections to close
    next_sweep: Instant,
    tls: Arc<ClientConfig>,
    /// `pump` runs, and the keys that it must look at after it
    pumping: bool,
    pumps: Vec<Key>,
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
            stream_max: if max_body == 0 { usize::MAX } else { max_body },
            idle_timeout,
            next_sweep: Instant::now(),
            tls,
            pumping: false,
            pumps: Vec::new(),
            bufs: [Vec::new(), Vec::new()],
        })
    }

    fn done(&mut self, id: u64, status: u16, lines: Vec<u8>, body: Vec<u8>) {
        self.ready.push_back(Done {
            id,
            kind: R_WHOLE,
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
            kind: R_WHOLE,
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
            host.h2.retain(|&i| i != idx);
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
        if let Some(mux) = conn.h2 {
            let mut subs: Vec<(u32, Sub)> = mux.subs.into_iter().collect();
            // the oldest first, as they go back in front of the queue
            subs.sort_unstable_by_key(|(stream, _)| std::cmp::Reverse(*stream));
            for (_, sub) in subs {
                self.h2_lost(&key, sub, error, false, error == E_CLOSED);
            }
        }
        if let Some(mut req) = conn.req {
            // `head`: the read buffer of a chunked answer is empty again
            // after its head, so `filled` alone does not say "no answer byte"
            let again = conn.reused
                && conn.filled == 0
                && conn.head.is_none()
                && req.flags & AGAIN != 0
                && req.flags & UPLOAD == 0
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
        // with one protocol to offer there is nothing to wait for
        let wait_alpn = tls.is_some() && self.tls.alpn_protocols.len() > 1;
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
            down: None,
            wrote: 0,
            told: 0,
            wait_alpn,
            h2: None,
        });
        self.hosts.entry(key).or_default().count += 1;
    }

    /// Sends a request on an idle connection for `key` or on a new one, or
    /// lets it wait when `max_per_host` connections are open.
    fn start(&mut self, key: Key, req: Req) {
        while let Some(idx) = self.h2_free(&key) {
            let idle = self
                .conns
                .get(idx)
                .is_some_and(|c| c.h2.as_ref().is_some_and(|mux| mux.subs.is_empty()));
            if idle {
                // Read first, as for an idle HTTP/1.1 connection: the
                // server may have closed it or said GOAWAY.
                self.mux(idx);
                if self.h2_free(&key) != Some(idx) {
                    continue;
                }
            }
            self.h2_open(idx, req);
            // one write for all the requests that start in this call
            if !self.again.contains(&idx) {
                self.again.push(idx);
            }
            return;
        }
        while let Some(idx) = self.hosts.get_mut(&key).and_then(|h| h.idle.pop()) {
            let Some(c) = self.conns.get_mut(idx) else {
                continue;
            };
            // Read first. The server may have closed the connection, or
            // sent bytes nobody asked for: they must not become the answer
            // to this request.
            let (fail, eof, _) = c.io(READ_MAX);
            if fail.is_some() || eof || c.filled > 0 {
                self.close(idx);
                continue;
            }
            c.req = Some(req);
            c.sent = 0;
            c.wrote = 0;
            c.told = 0;
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
    ///
    /// A request that starts can end a request of the same call chain (an
    /// answer that is there already, a connection that fails), and that
    /// pumps again. Such a call inside a call only notes its key: thus
    /// many requests that wait do not make the stack deep.
    fn pump(&mut self, key: &Key) {
        if self.pumping {
            return self.pumps.push(key.clone());
        }
        self.pumping = true;
        self.pump_one(key);
        while let Some(key) = self.pumps.pop() {
            self.pump_one(&key);
        }
        self.pumping = false;
    }

    fn pump_one(&mut self, key: &Key) {
        loop {
            let shared = self.h2_free(key).is_some();
            let Some(host) = self.hosts.get_mut(key) else {
                return;
            };
            if host.queue.is_empty() {
                if host.count == 0 {
                    self.hosts.remove(key);
                }
                return;
            }
            let full = self.max_per_host != 0 && host.count >= self.max_per_host;
            if !shared && host.idle.is_empty() && full {
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
        if c.h2.is_some() {
            return self.mux(idx);
        }
        let cap = c.cap();
        let (mut fail, mut eof, mut cut) = c.io(cap);
        if c.wait_alpn && fail.is_none() && c.tls.as_ref().is_some_and(|t| !t.is_handshaking()) {
            // the handshake is done: the server picked the protocol
            c.wait_alpn = false;
            if c.tls.as_ref().and_then(|t| t.alpn_protocol()) == Some(b"h2") {
                return self.adopt(idx);
            }
            // the request goes out now, and no event comes for that
            let (fail2, eof2, cut2) = c.io(cap);
            fail = fail2;
            eof |= eof2;
            cut |= cut2;
        }
        let Some(req) = &mut c.req else {
            // an idle connection: the server closed it, or sent bytes
            // nobody asked for
            if fail.is_some() || eof || c.filled > 0 {
                let key = c.key.clone();
                self.close(idx);
                self.pump(&key);
            }
            return;
        };
        if req.flags & UPLOAD != 0 {
            // the room of the bytes that went out is free again
            if c.sent > 0 && c.sent == req.bytes.len() {
                req.bytes.clear();
                c.sent = 0;
            }
            // the first report also tells Dart that the connection works
            if c.connected && c.wrote > c.told {
                c.told = c.wrote;
                // Dart counts the bytes it gave, not the chunk framing
                push_progress(&mut self.ready, req.id, c.wrote.saturating_sub(req.framing));
                // `due` may now be too early: then `expire` finds nothing
                if let (Some(wait), Some(_)) = (req.wait, req.deadline) {
                    req.deadline = Some(self.now + wait);
                }
            }
        }
        // A connection that failed is at its end too. The bytes that came
        // before may be a whole answer: then the request is done.
        let eof = eof || fail.is_some();
        let cut = cut || fail.is_some();
        let ended = fail.unwrap_or(if c.connected { E_CLOSED } else { E_CONNECT });
        let streamed = req.flags & STREAM != 0;
        if c.head.is_none() {
            // a streamed body has its limit while it comes
            let limit = if streamed { usize::MAX } else { max };
            match parse_head(&c.buf[..c.filled], req.flags & HEAD_ONLY != 0, limit) {
                Ok(Some(head)) => c.head = Some(head),
                Ok(None) if c.filled > MAX_HEAD => return self.fail(idx, E_BAD),
                Ok(None) if eof => return self.fail(idx, ended),
                Ok(None) => return,
                Err(error) => return self.fail(idx, error),
            }
        }
        if streamed {
            return self.stream(idx, eof, cut, ended);
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
        let mut lines = std::mem::take(&mut head.lines);
        let key = c.key.clone();
        // an answer with no body (eg. to HEAD) keeps its lines
        let coded = req.flags & DECODE != 0 && head.coding != Coding::Raw && !body.is_empty();
        let coding = head.coding;
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
        // the connection is in order, also when the body is not
        let body = if coded {
            match decode(coding, &body, max) {
                Ok(plain) => {
                    lines = decoded_lines(&lines);
                    plain
                }
                // a redirect is read for its `location`, not for its body
                Err(E_BAD) if (300..400).contains(&status) => body,
                Err(error) => {
                    self.failed(id, error);
                    return self.pump(&key);
                }
            }
        } else {
            body
        };
        self.done(id, status, lines, body);
        self.pump(&key);
    }

    /// The answer of a streamed request: the head goes to Dart, then the
    /// body in parts, as far as Dart has room for them.
    fn stream(&mut self, idx: usize, eof: bool, cut: bool, ended: u16) {
        let max = self.stream_max;
        let Some(c) = self.conns.get_mut(idx) else {
            return;
        };
        let (Some(req), Some(head)) = (c.req.as_mut(), c.head.as_mut()) else {
            return;
        };
        let id = req.id;
        if c.down.is_none() {
            // more than one coding (rare) stays as it is, as an answer with
            // no body does
            let coded = req.flags & DECODE != 0
                && head.coding != Coding::Raw
                && !matches!(head.body, Body::Len(0));
            let lines = if coded {
                decoded_lines(&head.lines)
            } else {
                std::mem::take(&mut head.lines)
            };
            c.down = Some(Box::new(Down {
                sent: 0,
                credit: 0,
                total: 0,
                raw: 0,
                inflate: if coded {
                    Inflate::new(head.coding)
                } else {
                    None
                },
                redirect: (300..400).contains(&head.status),
                raw_done: None,
            }));
            // the timeout is for the head
            req.deadline = None;
            // the body starts at the front of the buffer
            c.buf.copy_within(head.start..c.filled, 0);
            c.filled -= head.start;
            head.start = 0;
            if let Body::Chunked(ch) = &mut head.body {
                ch.pos = 0;
            }
            self.ready.push_back(Done {
                id,
                kind: R_HEAD,
                status: head.status,
                error: 0,
                lines,
                body: Vec::new(),
            });
        }
        let result = c.body_step(eof, cut, ended, max, &mut self.ready);
        // only a coded body that is bad: bad chunks are still a failure
        let redirect = c
            .down
            .as_ref()
            .is_some_and(|d| d.redirect && d.inflate.is_some());
        match result {
            Ok(None) => {}
            Ok(Some(rest)) => self.finish(idx, eof, rest == 0),
            // a redirect is read for its `location`, not for its body
            Err(E_BAD) if redirect => self.finish(idx, eof, false),
            Err(error) => self.fail(idx, error),
        }
    }

    /// The streamed answer of connection `idx` is whole. `clean`: no byte
    /// came after it.
    fn finish(&mut self, idx: usize, eof: bool, clean: bool) {
        let Some(c) = self.conns.get_mut(idx) else {
            return;
        };
        let (Some(req), Some(head)) = (&c.req, &c.head) else {
            return;
        };
        self.ready.push_back(Done {
            id: req.id,
            kind: R_END,
            status: 0,
            error: 0,
            lines: Vec::new(),
            body: Vec::new(),
        });
        self.open -= 1;
        // an answer before the whole request (eg. 401): the server waits
        // for the other bytes, so the connection is not used again
        let sent_all = c.sent == req.bytes.len()
            && !req.more
            && !req.aborted
            && c.tls.as_ref().is_none_or(|t| !t.wants_write());
        let key = c.key.clone();
        if head.keep && !eof && clean && sent_all {
            c.req = None;
            c.head = None;
            c.down = None;
            c.sent = 0;
            c.wrote = 0;
            c.told = 0;
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
        self.pump(&key);
    }

    /// An HTTP/2 connection for `key` that can take one more request.
    fn h2_free(&self, key: &Key) -> Option<usize> {
        let free = |idx: &usize| {
            let mux = self.conns.get(*idx).and_then(|c| c.h2.as_ref());
            mux.is_some_and(|mux| mux.proto.capacity())
        };
        self.hosts.get(key)?.h2.iter().copied().find(free)
    }

    /// The server of connection `idx` picked HTTP/2: the request of the
    /// connection is its first stream, and the requests that wait may
    /// follow.
    fn adopt(&mut self, idx: usize) {
        let Some(c) = self.conns.get_mut(idx) else {
            return;
        };
        let key = c.key.clone();
        let req = c.req.take();
        c.sent = 0;
        c.h2 = Some(Box::new(Mux {
            proto: h2::H2::new(),
            subs: HashMap::new(),
            idle: false,
        }));
        self.hosts.entry(key.clone()).or_default().h2.push(idx);
        if let Some(req) = req {
            self.h2_open(idx, req);
        }
        self.drive(idx);
        self.pump(&key);
    }

    /// Makes `req` a stream of the HTTP/2 connection `idx`.
    fn h2_open(&mut self, idx: usize, mut req: Req) {
        let Some((head_len, fields)) = h2_fields(&req.bytes) else {
            return self.failed(req.id, E_BAD);
        };
        let Some(mux) = self.conns.get_mut(idx).and_then(|c| c.h2.as_mut()) else {
            return self.failed(req.id, E_CLOSED);
        };
        let upload = req.flags & UPLOAD != 0;
        let body = &req.bytes[head_len..];
        let list: Vec<(&[u8], &[u8])> = fields
            .iter()
            .map(|(name, value)| (name.as_slice(), value.as_slice()))
            .collect();
        let stream = mux.proto.open(&list, body.is_empty() && !upload);
        if !body.is_empty() {
            mux.proto.send(stream, body, !upload);
        }
        if upload {
            // the connection works: Dart may give the body now
            push_progress(&mut self.ready, req.id, head_len as u64);
            req.bytes = Vec::new();
        } else if req.retried {
            // A request that may go out again keeps its bytes until the
            // head of its answer comes.
            req.bytes = Vec::new();
        }
        mux.idle = false;
        mux.subs.insert(
            stream,
            Sub {
                req,
                head_len: head_len as u64,
                status: 0,
                lines: Vec::new(),
                coding: Coding::Raw,
                body: Vec::new(),
                down: None,
                released: 0,
                ended: false,
            },
        );
    }

    /// Reads and writes the HTTP/2 connection `idx` and gives Dart what
    /// came for its requests.
    fn mux(&mut self, idx: usize) {
        let mut turns = 0;
        let key = loop {
            let Some(c) = self.conns.get_mut(idx) else {
                return;
            };
            let (fail, eof, _) = c.io(READ_MAX);
            let Some(mux) = c.h2.as_mut() else { return };
            // what went out
            let sent = c.sent.min(mux.proto.out.len());
            mux.proto.out.drain(..sent);
            c.sent = 0;
            let waits = mux.proto.out.len();
            let Ok(used) = mux.proto.recv(&c.buf[..c.filled]) else {
                // the answers that were whole before the bad frame count
                let events = std::mem::take(&mut mux.proto.events);
                // tell the server why (GOAWAY), if the socket takes it
                let _ = c.io(0);
                self.h2_events(idx, events);
                return self.fail(idx, E_BAD);
            };
            c.buf.copy_within(used..c.filled, 0);
            c.filled -= used;
            if c.filled == 0 && c.buf.len() > KEEP_BUF {
                c.buf = Vec::new();
            }
            let events = std::mem::take(&mut mux.proto.events);
            let key = c.key.clone();
            self.h2_events(idx, events);
            // The answers that came before a failure are whole: only now
            // the others fail.
            if fail.is_some() || eof {
                return self.fail(idx, fail.unwrap_or(E_CLOSED));
            }
            let Some(c) = self.conns.get_mut(idx) else {
                return;
            };
            let Some(mux) = c.h2.as_mut() else { return };
            mux.proto.flush();
            if mux.subs.is_empty() {
                // the server said GOAWAY and has all its answers out
                if mux.proto.goaway {
                    self.close(idx);
                    break key;
                }
                // Its last request is done: it is idle from now on. Each
                // way a request ends comes through here.
                if !mux.idle {
                    mux.idle = true;
                    c.idle_since = self.now;
                }
            }
            // Bytes that came to `out` after the `io`: no socket event
            // comes for them, so they need one more `io`.
            turns += 1;
            if mux.proto.out.len() <= waits {
                break key;
            }
            if turns == 8 {
                c.more = true;
                break key;
            }
        };
        // a stream that ended makes room for a request that waits
        self.pump(&key);
    }

    fn h2_events(&mut self, idx: usize, events: Vec<h2::Event>) {
        for event in events {
            match event {
                h2::Event::Head {
                    stream,
                    status,
                    lines,
                } => self.h2_head(idx, stream, status, lines),
                h2::Event::Data { stream, bytes } => self.h2_pull(idx, stream, &bytes),
                h2::Event::End { stream } => {
                    let sub = self
                        .conns
                        .get_mut(idx)
                        .and_then(|c| c.h2.as_mut())
                        .and_then(|mux| mux.subs.get_mut(&stream));
                    if let Some(sub) = sub {
                        sub.ended = true;
                    }
                    self.h2_pull(idx, stream, &[]);
                }
                h2::Event::Reset {
                    stream,
                    error,
                    retry,
                } => {
                    let Some(c) = self.conns.get_mut(idx) else {
                        return;
                    };
                    let key = c.key.clone();
                    let sub = c.h2.as_mut().and_then(|mux| mux.subs.remove(&stream));
                    if let Some(sub) = sub {
                        self.h2_lost(&key, sub, error, retry, false);
                    }
                }
                h2::Event::Wrote { stream, total } => {
                    let sub = self
                        .conns
                        .get_mut(idx)
                        .and_then(|c| c.h2.as_mut())
                        .and_then(|mux| mux.subs.get_mut(&stream));
                    if let Some(sub) = sub
                        && sub.req.flags & UPLOAD != 0
                    {
                        push_progress(&mut self.ready, sub.req.id, sub.head_len + total);
                        // see `step`: the timeout of an upload starts again
                        if let (Some(wait), Some(_)) = (sub.req.wait, sub.req.deadline) {
                            sub.req.deadline = Some(self.now + wait);
                        }
                    }
                }
            }
        }
    }

    /// The head of the answer of a stream.
    fn h2_head(&mut self, idx: usize, stream: u32, status: u16, lines: Vec<u8>) {
        let sub = self
            .conns
            .get_mut(idx)
            .and_then(|c| c.h2.as_mut())
            .and_then(|mux| mux.subs.get_mut(&stream));
        let Some(sub) = sub else { return };
        sub.status = status;
        // it has an answer: it does not go out again
        sub.req.bytes = Vec::new();
        if sub.req.flags & DECODE != 0 {
            sub.coding = coding_of(&lines);
        }
        if sub.req.flags & STREAM == 0 {
            sub.lines = lines;
            return;
        }
        // an answer with no body keeps its lines
        let bodiless = sub.req.flags & HEAD_ONLY != 0 || status == 204 || status == 304;
        let coded = sub.coding != Coding::Raw && !bodiless;
        sub.down = Some(Box::new(Down {
            sent: 0,
            credit: 0,
            total: 0,
            raw: 0,
            inflate: if coded {
                Inflate::new(sub.coding)
            } else {
                None
            },
            redirect: (300..400).contains(&status),
            raw_done: None,
        }));
        // the timeout is for the head
        sub.req.deadline = None;
        self.ready.push_back(Done {
            id: sub.req.id,
            kind: R_HEAD,
            status,
            error: 0,
            lines: if coded { decoded_lines(&lines) } else { lines },
            body: Vec::new(),
        });
    }

    /// Body bytes of a stream came (`data`), or Dart made room, or the
    /// server ended the answer: gives Dart what it can take, and finishes
    /// the request when its answer is whole.
    fn h2_pull(&mut self, idx: usize, stream: u32, data: &[u8]) {
        let (max_body, stream_max) = (self.max_body, self.stream_max);
        let Some(mux) = self.conns.get_mut(idx).and_then(|c| c.h2.as_mut()) else {
            return;
        };
        let Some(sub) = mux.subs.get_mut(&stream) else {
            return;
        };
        let id = sub.req.id;
        // An answer to HEAD has no body. Bytes of a server that sends one
        // are dropped, as HTTP/1.1 does not read them.
        let data: &[u8] = if sub.req.flags & HEAD_ONLY != 0 {
            mux.proto.release(stream, data.len() as u64);
            &[]
        } else {
            data
        };
        let Some(down) = sub.down.as_mut() else {
            // not streamed: the body waits here for its end
            if sub.req.flags & STREAM != 0 {
                return;
            }
            sub.body.extend_from_slice(data);
            mux.proto.release(stream, data.len() as u64);
            if sub.body.len() > max_body {
                mux.subs.remove(&stream);
                mux.proto.reset(stream, h2::CANCEL);
                return self.failed(id, E_BIG);
            }
            if !sub.ended {
                return;
            }
            let Some(sub) = mux.subs.remove(&stream) else {
                return;
            };
            let coded = sub.coding != Coding::Raw && !sub.body.is_empty();
            if !coded {
                return self.done(id, sub.status, sub.lines, sub.body);
            }
            return match decode(sub.coding, &sub.body, max_body) {
                Ok(plain) => self.done(id, sub.status, decoded_lines(&sub.lines), plain),
                // a redirect is read for its `location`, not for its body
                Err(E_BAD) if (300..400).contains(&sub.status) => {
                    self.done(id, sub.status, sub.lines, sub.body)
                }
                Err(error) => self.failed(id, error),
            };
        };
        let quiet = down.redirect && down.inflate.is_some();
        let whole = match emit(down, id, data, sub.ended, stream_max, &mut self.ready) {
            Ok(whole) => whole && sub.ended,
            // a bad body of a redirect ends here, before the server ends it
            Err(E_BAD) if quiet => true,
            Err(error) => {
                mux.subs.remove(&stream);
                mux.proto.reset(stream, h2::CANCEL);
                return self.failed(id, error);
            }
        };
        if !whole {
            return sub.window(&mut mux.proto, stream);
        }
        mux.subs.remove(&stream);
        // no stream is left after a whole answer: this is for the bad body
        mux.proto.reset(stream, h2::CANCEL);
        self.ready.push_back(Done {
            id,
            kind: R_END,
            status: 0,
            error: 0,
            lines: Vec::new(),
            body: Vec::new(),
        });
        self.open -= 1;
    }

    /// A request of an HTTP/2 connection got no whole answer. It goes out
    /// one more time when that is safe:
    /// - `unused`: the server says that it did not work on the request
    ///   (REFUSED_STREAM, or GOAWAY for an earlier stream). Safe for each
    ///   method (RFC 9113 8.7).
    /// - `closed`: the connection closed before a head came. The server
    ///   may have done the work, so only for a request with [AGAIN].
    fn h2_lost(&mut self, key: &Key, mut sub: Sub, error: u16, unused: bool, closed: bool) {
        let safe = unused || (closed && sub.req.flags & AGAIN != 0);
        let again = safe
            && sub.status == 0
            && sub.req.flags & UPLOAD == 0
            && !sub.req.retried
            && !sub.req.bytes.is_empty();
        if again {
            sub.req.retried = true;
            self.hosts
                .entry(key.clone())
                .or_default()
                .queue
                .push_front(sub.req);
        } else {
            self.failed(sub.req.id, error);
        }
    }

    /// Finds the request `id`, on a connection or in a queue.
    fn locate(&self, id: u64) -> Option<Spot> {
        let on = |r: &Req| r.id == id;
        if let Some((idx, _)) = self
            .conns
            .iter()
            .find(|(_, c)| c.req.as_ref().is_some_and(on))
        {
            return Some(Spot::Conn(idx));
        }
        for (idx, c) in self.conns.iter() {
            let Some(mux) = &c.h2 else { continue };
            if let Some((&stream, _)) = mux.subs.iter().find(|(_, sub)| sub.req.id == id) {
                return Some(Spot::Mux(idx, stream));
            }
        }
        self.hosts.iter().find_map(|(key, host)| {
            let at = host.queue.iter().position(on)?;
            Some(Spot::Queued(key.clone(), at))
        })
    }

    fn req_at(&mut self, spot: &Spot) -> Option<&mut Req> {
        match spot {
            Spot::Conn(idx) => self.conns.get_mut(*idx)?.req.as_mut(),
            Spot::Mux(idx, stream) => {
                let mux = self.conns.get_mut(*idx)?.h2.as_mut()?;
                mux.subs.get_mut(stream).map(|sub| &mut sub.req)
            }
            Spot::Queued(key, at) => self.hosts.get_mut(key)?.queue.get_mut(*at),
        }
    }

    /// [Q_PART]: more bytes of the request body.
    fn part(&mut self, id: u64, data: &[u8]) {
        let Some(spot) = self.locate(id) else { return };
        if let Spot::Mux(idx, stream) = spot {
            if let Some(mux) = self.conns.get_mut(idx).and_then(|c| c.h2.as_mut())
                && mux.subs.get(&stream).is_some_and(|sub| sub.req.more)
            {
                mux.proto.send(stream, data, false);
            }
            return self.drive(idx);
        }
        if let Spot::Conn(idx) = spot
            && let Some(c) = self.conns.get_mut(idx)
            && let Some(req) = &mut c.req
        {
            // the bytes that went out make room
            if c.sent >= req.bytes.len() {
                req.bytes.clear();
                c.sent = 0;
            } else if c.sent >= 1 << 20 {
                req.bytes.drain(..c.sent);
                c.sent = 0;
            }
        }
        match self.req_at(&spot) {
            Some(req) if req.more && !req.aborted => {
                if req.flags & CHUNKED != 0 && !data.is_empty() {
                    let size = format!("{:x}\r\n", data.len());
                    req.bytes.extend_from_slice(size.as_bytes());
                    req.bytes.extend_from_slice(data);
                    req.bytes.extend_from_slice(b"\r\n");
                    req.framing += size.len() as u64 + 2;
                } else {
                    req.bytes.extend_from_slice(data);
                }
            }
            _ => return,
        }
        // the socket may take the bytes now, and no event comes for that
        if let Spot::Conn(idx) = spot {
            self.drive(idx);
        }
    }

    /// [Q_END]: the request body is whole, or Dart stopped it.
    fn end_body(&mut self, id: u64, aborted: bool) {
        let Some(spot) = self.locate(id) else { return };
        let mut last_chunk = false;
        if let Some(req) = self.req_at(&spot) {
            if !req.more {
                return;
            }
            req.more = false;
            if aborted {
                req.aborted = true;
                req.bytes.clear();
            } else if req.flags & CHUNKED != 0 && matches!(spot, Spot::Conn(_)) {
                req.bytes.extend_from_slice(b"0\r\n\r\n");
                req.framing += 5;
                last_chunk = true;
            }
        }
        match spot {
            Spot::Mux(idx, stream) => {
                if let Some(mux) = self.conns.get_mut(idx).and_then(|c| c.h2.as_mut()) {
                    if aborted {
                        mux.proto.abort_send(stream);
                    } else {
                        mux.proto.send(stream, &[], true);
                    }
                }
                self.drive(idx);
            }
            Spot::Conn(idx) => {
                if aborted && let Some(c) = self.conns.get_mut(idx) {
                    c.sent = 0;
                }
                // the socket may take the last chunk now
                if last_chunk {
                    self.drive(idx);
                }
            }
            Spot::Queued(..) => {}
        }
    }

    /// [Q_CREDIT]: Dart used `total` bytes of the answer body.
    fn credit(&mut self, id: u64, total: u64) {
        let idx = match self.locate(id) {
            Some(Spot::Conn(idx)) => idx,
            Some(Spot::Mux(idx, stream)) => {
                let down = self
                    .conns
                    .get_mut(idx)
                    .and_then(|c| c.h2.as_mut())
                    .and_then(|mux| mux.subs.get_mut(&stream))
                    .and_then(|sub| sub.down.as_mut());
                if let Some(down) = down {
                    down.credit = down.credit.max(total.min(down.sent));
                }
                self.h2_pull(idx, stream, &[]);
                // a WINDOW_UPDATE may wait to go out
                return self.drive(idx);
            }
            _ => return,
        };
        let Some(down) = self.conns.get_mut(idx).and_then(|c| c.down.as_mut()) else {
            return;
        };
        down.credit = down.credit.max(total.min(down.sent));
        // Rust stopped reading when the room was gone, and no event comes
        // for the bytes in the socket
        self.drive(idx);
    }

    /// [Q_CANCEL]: forget request `id` and close its connection.
    fn cancel(&mut self, id: u64) {
        match self.locate(id) {
            Some(Spot::Conn(idx)) => {
                if let Some(conn) = self.close(idx) {
                    self.open -= 1;
                    self.pump(&conn.key);
                }
            }
            Some(Spot::Mux(idx, stream)) => {
                let Some(c) = self.conns.get_mut(idx) else {
                    return;
                };
                let key = c.key.clone();
                if let Some(mux) = c.h2.as_mut()
                    && mux.subs.remove(&stream).is_some()
                {
                    mux.proto.reset(stream, h2::CANCEL);
                    self.open -= 1;
                }
                self.drive(idx);
                self.pump(&key);
            }
            Some(Spot::Queued(key, at)) => {
                if let Some(host) = self.hosts.get_mut(&key) {
                    host.queue.remove(at);
                    self.open -= 1;
                }
                self.forget(&key);
            }
            None => {}
        }
    }

    /// [Q_NEW]: starts a request. A record that is not whole is dropped.
    fn request(&mut self, id: u64, rec: &[u8]) {
        let port = u16::from_le_bytes([rec[14], rec[15]]);
        let addr: SocketAddr = if rec[12] == 6 {
            let mut ip = [0u8; 16];
            ip.copy_from_slice(&rec[16..32]);
            (Ipv6Addr::from(ip), port).into()
        } else {
            (Ipv4Addr::new(rec[16], rec[17], rec[18], rec[19]), port).into()
        };
        let ms = rd32(rec, 32) as u64;
        let name_len = u16::from_le_bytes([rec[36], rec[37]]) as usize;
        if REQ_HEAD + name_len > rec.len() {
            return;
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
            return self.failed(id, E_TLS);
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
                wait: (ms > 0 && flags & UPLOAD != 0).then(|| Duration::from_millis(ms)),
                bytes: rec[REQ_HEAD + name_len..].to_vec(),
                more: flags & UPLOAD != 0,
                aborted: false,
                framing: 0,
            },
        );
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
            .filter(|(_, c)| {
                c.req.is_none()
                    && c.h2.as_ref().is_none_or(|mux| mux.subs.is_empty())
                    && now.duration_since(c.idle_since) >= limit
            })
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
        // the streams of the HTTP/2 connections: the connection stays
        let mut late: Vec<(usize, u32)> = Vec::new();
        for (idx, c) in self.conns.iter() {
            let Some(mux) = &c.h2 else { continue };
            late.extend(
                mux.subs
                    .iter()
                    .filter(|(_, sub)| over(&sub.req))
                    .map(|(&stream, _)| (idx, stream)),
            );
        }
        for (idx, stream) in late {
            let Some(c) = self.conns.get_mut(idx) else {
                continue;
            };
            let key = c.key.clone();
            let Some(mux) = c.h2.as_mut() else { continue };
            if let Some(sub) = mux.subs.remove(&stream) {
                mux.proto.reset(stream, h2::CANCEL);
                self.failed(sub.req.id, E_TIMEOUT);
            }
            self.drive(idx);
            self.pump(&key);
        }
        let busy = self.conns.iter().filter_map(|(_, c)| c.req.as_ref());
        let shared = self
            .conns
            .iter()
            .filter_map(|(_, c)| c.h2.as_ref())
            .flat_map(|mux| mux.subs.values().map(|sub| &sub.req));
        let waiting = self.hosts.values().flat_map(|h| h.queue.iter());
        self.due = busy
            .chain(shared)
            .chain(waiting)
            .filter_map(|r| r.deadline)
            .min();
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
            let id = rd64(rec, 4);
            match rec[38] {
                Q_NEW => self.request(id, rec),
                Q_PART => self.part(id, &rec[REQ_HEAD..]),
                Q_END => self.end_body(id, rec[16] != 0),
                Q_CREDIT => self.credit(id, rd64(rec, 16)),
                Q_CANCEL => self.cancel(id),
                _ => {}
            }
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
            rec[20..RES_HEAD].fill(0);
            rec[20] = d.kind;
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
fn tls_config(extra: &[u8], http2: bool) -> Option<Arc<ClientConfig>> {
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
    // the server picks the first of them that it has
    config.alpn_protocols = if http2 {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    } else {
        vec![b"http/1.1".to_vec()]
    };
    Some(Arc::new(config))
}

/// Makes a client. Its idle helper posts `[1]` to `send_port` when a
/// socket has data for a waiting request. With `max_per_host` > 0 it holds
/// at most that many connections open to one address. With `max_body` > 0,
/// a request whose answer body is bigger fails. With `idle_ms` > 0, a
/// connection with no request for that long closes. `ca` (`ca_len` bytes,
/// PEM) has more root certificates to trust for `https://`. With `http2`
/// not 0, an `https://` connection uses HTTP/2 when the server has it.
/// Returns a handle
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
    http2: i32,
) -> i64 {
    if POST.get().is_none() {
        return -1;
    }
    let ca: &[u8] = if ca_len > 0 {
        unsafe { std::slice::from_raw_parts(ca, ca_len as usize) }
    } else {
        &[]
    };
    let Some(tls) = tls_config(ca, http2 != 0) else {
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

/// All the decoded bytes of `coded`, given to an [Inflate] `step` bytes at
/// a time. At most `max` bytes: more is [E_BIG].
#[cfg(fuzzing)]
fn inflate_bytes(coding: Coding, coded: &[u8], step: usize, max: usize) -> Result<Vec<u8>, u16> {
    let Some(mut inflate) = Inflate::new(coding) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let mut buf = vec![0u8; 4096];
    let mut pieces = coded.chunks(step.max(1)).peekable();
    if pieces.peek().is_none() {
        inflate.push(&[], true);
    }
    loop {
        let piece = pieces.next().unwrap_or(&[]);
        inflate.push(piece, pieces.peek().is_none());
        loop {
            match inflate.pull(&mut buf)? {
                Pull::Data(n) => {
                    out.extend_from_slice(&buf[..n]);
                    if out.len() > max {
                        return Err(E_BIG);
                    }
                }
                Pull::Wait => break,
                Pull::End => return Ok(out),
            }
        }
        if pieces.peek().is_none() {
            return Err(E_BAD);
        }
    }
}

/// The fuzz target `client_response`: random bytes as the answer of a
/// server. The head parser must not panic. A chunked body must give the
/// same result in one piece and byte by byte. So must a gzip or brotli
/// body, which also has a limit.
#[cfg(fuzzing)]
pub(crate) fn fuzz(data: &[u8]) {
    const MAX: usize = 1 << 20;
    h2::fuzz(data);
    let _ = h2_fields(data);
    let _ = coding_of(data);
    for coding in [Coding::Gzip, Coding::Brotli] {
        let whole = inflate_bytes(coding, data, data.len(), MAX);
        let split = inflate_bytes(coding, data, 1, MAX);
        assert_eq!(whole.is_ok(), split.is_ok());
        if let (Ok(a), Ok(b)) = (&whole, &split) {
            assert_eq!(a, b);
            assert!(a.len() <= MAX);
        }
    }
    let _ = parse_head(data, true, MAX);
    let Ok(Some(head)) = parse_head(data, false, MAX) else {
        return;
    };
    assert!(head.start <= data.len());
    // the bytes after the head as a coded body: no panic, and the limit
    // holds
    let coding = if head.coding == Coding::Raw {
        Coding::Gzip
    } else {
        head.coding
    };
    if let Ok(plain) = decode(coding, &data[head.start..], MAX) {
        assert!(plain.len() <= MAX);
    }
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
