//! HTTP/2 over cleartext TCP ("h2c" with prior knowledge, RFC 9113).
//!
//! With `http2` on, the first bytes of a new connection pick the protocol:
//! the client preface means HTTP/2 (see `Conn::sniff` in run.rs). Then all
//! bytes of the connection go to its [H2]. It turns each request into the
//! same request record Dart reads for HTTP/1.1 (flag bit 3 set), and each
//! answer of Dart into HEADERS and DATA frames. Answers go out as soon as
//! they come, in any order. Streams with body bytes take turns, one DATA
//! frame each.
//!
//! A request body is read whole before Dart gets the request (buffered,
//! like HTTP/1.1). A gRPC request (`content-type: application/grpc*`)
//! streams its body to Dart instead: BODY_PART records, and the client gets
//! its stream window back only when Dart acks the bytes (BODY_ACK). The
//! connection window comes back at once, so unread bodies never stall the
//! other streams of the connection. Streamed
//! answers (kind 6, 7, 8) go out as DATA frames
//! under the send windows. Floods of small frames get GOAWAY
//! ENHANCE_YOUR_CALM (plan 4.10): see the `MAX_*` limits below. Too many
//! resets first get a graceful GOAWAY NO_ERROR (see `spend_n`): one
//! connection of a proxy carries many users, and their open requests must
//! still get their answers.

pub(crate) mod frame;
pub(crate) mod hpack;
mod huffman;
mod stream;

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use frame::{Error, Frame, Head};
use hpack::{Decoded, Decoder, encode_field};
use stream::{Recv, Stream};

use crate::run::{MAX_HEADERS, escape_target, length_line, parse_len, write_head};
use crate::{BODY_END, BODY_PART, MAX_HEAD, REQ_HEAD};

/// The client preface (RFC 9113 3.4).
pub(crate) const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
/// Our SETTINGS_MAX_FRAME_SIZE (the default). A bigger frame from the
/// client is a FRAME_SIZE_ERROR.
const MAX_FRAME: usize = 16 * 1024;
/// Our SETTINGS_INITIAL_WINDOW_SIZE: bytes a client may send on one stream
/// before our WINDOW_UPDATE.
const STREAM_WINDOW: i64 = 1 << 20;
/// Our connection receive window, raised to this right after our SETTINGS.
const CONN_WINDOW: i64 = 16 << 20;
/// The window of a new connection or stream before any SETTINGS or
/// WINDOW_UPDATE (RFC 9113 6.9.2).
const DEFAULT_WINDOW: i64 = 65_535;
/// The HPACK table size we allow the client (the default 4096).
const TABLE_SIZE: usize = 4096;
/// How many reset stream ids we remember.
const RESETS: usize = 64;
/// The largest MAX_FRAME_SIZE a client may set (RFC 9113 6.5.2).
const MAX_FRAME_LIMIT: u32 = (1 << 24) - 1;

// Flood limits (plan 4.10). Over one: GOAWAY ENHANCE_YOUR_CALM, then close.
// The reset bucket first drains gracefully (see `H2::spend_n`).
// "Per second" counts in windows of one second (see [Rate]).

/// Resets and refusals: a token bucket. Each of these takes one token:
/// RST_STREAM of the client, our RST_STREAM for a stream error of the
/// client, and a HEADERS or DATA we refuse (they cost an HPACK decode).
/// Rapid Reset (CVE-2023-44487) and "made you reset" (CVE-2025-8671) make
/// the server start work and drop it at once.
///
/// The bucket holds a burst of `max(2 * max_streams, RESET_BURST_MIN)`, so
/// a client or proxy that cancels all its calls at once is not cut. It
/// refills `RESET_RATE` tokens a second: only a flood that goes on is cut.
/// Both can be set from Dart (`http2ResetBurst`, `http2ResetRate`).
/// An empty bucket is not cut at once: GOAWAY NO_ERROR lets the open
/// streams finish (they may be other users of a proxy), and the bucket
/// gets one more burst for the frames in flight. Empty again: cut.
/// Each answer with a body gives half a token back (`RESET_REFUND`). A
/// gRPC client sends all its calls on one connection, and a call over its
/// deadline is a reset: a busy client with some timeouts must not be cut.
/// A client with up to about a third of its calls reset is not cut. An
/// attacker needs two real answers per reset.
/// The answers may come from a cheap route and the resets from a costly
/// one. The bucket can not see that, but the handler of a reset request
/// still runs to its end in Dart. So those handlers count against
/// `max_streams` until Dart answers them (`zombies`, as grpc-go does). The
/// bucket still guards cheap frame floods and the HPACK decode cost.
const RESET_BURST_MIN: u32 = 1000;
/// Tokens that come back each second (the sustained rate of resets). The
/// default of `http2ResetRate` in Dart: the rate comes from there.
#[cfg(test)]
pub(crate) const RESET_RATE: f64 = 100.0;
/// Tokens an answer with a body gives back. It is half, so an attacker
/// that pairs each reset with one cheap answer still loses tokens. Answers
/// without a body (HEAD, 204, 304, empty, headers only) give nothing.
const RESET_REFUND: f64 = 0.5;
/// How long a request the client reset still counts against
/// `max_streams` while Dart has not ended it (see `H2::zombies`).
/// Some handlers never end (eg. one that forgets `close` once the client
/// is gone), and they must not hold a slot forever. It is the same as
/// SEND_WAIT (30 s, the wait for a client that does not read).
/// After it the handler stops counting, but it may still run in Dart. So a
/// slow server under attack (handlers that take over 30 s) lets a client
/// start `max_streams` more lost handlers per 30 s on each connection
/// (about 8 a second with 250). There is no GOAWAY on expiry: a slow
/// handler may be a real one. When a connection closes, its zombies are
/// forgotten while the Dart handlers go on, so a client that connects
/// again gets a fresh `max_streams`. grpc-go has the same limit.
const ZOMBIE_WAIT: Duration = Duration::from_secs(30);
/// What a refused HEADERS costs when only the zombies fill `max_streams`
/// (the live streams alone do not). A client can not see our zombies, so
/// after a mass cancel its next calls are refused through no fault of its
/// own, and gRPC retries them. A full token each would cut a real client
/// at once. It still costs an HPACK decode, so a flood of refused HEADERS
/// is bounded, only not as fast. When the live streams alone are at
/// `max_streams`, the client broke our SETTINGS and pays a full token.
const ZOMBIE_REFUSAL_COST: f64 = 0.25;
/// Answer body bytes of one connection that wait for the client's windows
/// (or for [HOLD]). Over it new streams get REFUSED_STREAM until the
/// client reads. Reads go on (its WINDOW_UPDATEs must come in).
/// ponytail: a fixed cap; make it an option if real answers need more.
const UNSENT_CAP: usize = 32 << 20;
/// Streamed body bytes of one connection that Dart has not acked. The
/// connection window comes back at once (so unread bodies do not stall the
/// other streams), and each stream holds at most STREAM_WINDOW. But any
/// client may send `content-type: application/grpc` to any route, and a
/// streamed body has no timeout: without this cap `max_streams` unread
/// bodies would pin `max_streams` MiB per connection. The DATA that would
/// pass it resets the streams that hold the most (see `evict`); the
/// other streams go on. A gRPC handler that reads acks at once and never
/// comes close. Same as the old cap: CONN_WINDOW.
const UNACKED_CAP: u64 = CONN_WINDOW as u64;
/// The encoded header block of one request, with its CONTINUATIONs. Over
/// it: GOAWAY. The decoded list keeps MAX_HEAD (over it: 431).
const MAX_BLOCK: usize = 4 * MAX_HEAD;
/// SETTINGS frames per second.
const MAX_SETTINGS: u32 = 100;
/// SETTINGS whose ACK is not written yet: the client does not read.
const MAX_UNACKED_SETTINGS: u32 = 10;
/// PINGs per second. DATA we send starts the count again: grpc-go sends
/// a PING (for its window estimate) each time DATA comes to it.
const MAX_PINGS: u32 = 100;
/// PING ACKs not written yet (17 bytes each): the client does not read.
const MAX_UNACKED_PINGS: u32 = 100;
/// Frames in a row that carry no work (see `no_work`), with no HEADERS or
/// DATA between: eg. empty DATA or CONTINUATION, PRIORITY, unknown types.
const MAX_EMPTY: u32 = 100;

/// How many full-size uploads one connection may hold at once. The memory
/// stays bounded at this many times one HTTP/1.1 connection.
const BODY_STREAMS_CAP: usize = 4;
/// Answer bytes in the write buffer before more DATA frames are added.
/// The rest waits in its stream, so the buffer stays small and the
/// streams take turns.
const HOLD: usize = 64 * 1024;
/// Request flag bit 3: the request came over HTTP/2.
const FLAG_H2: u32 = 8;
/// Request flag bit 4: the body comes in parts (BODY_PART, BODY_END).
const FLAG_STREAMED: u32 = 16;
/// A streamed body: acked bytes go back to the client (WINDOW_UPDATE) once
/// there are this many, so small acks do not cost a frame each. At most
/// this much of the window is held back after Dart acked all, so the
/// client never stalls on it.
const ACK_BATCH: u64 = STREAM_WINDOW as u64 / 2;
/// The content-type of a request whose body streams (any case, and
/// `+proto` etc. may follow)
const GRPC: &[u8] = b"application/grpc";

// SETTINGS ids (RFC 9113 6.5.2)
const ENABLE_PUSH: u16 = 2;
const MAX_CONCURRENT_STREAMS: u16 = 3;
const INITIAL_WINDOW_SIZE: u16 = 4;
const MAX_FRAME_SIZE: u16 = 5;
const MAX_HEADER_LIST_SIZE: u16 = 6;

/// A token bucket (see `RESET_RATE`).
struct Bucket {
    tokens: f64,
    at: Instant,
    /// tokens back per second
    rate: f64,
}

impl Bucket {
    fn new(now: Instant, burst: f64, rate: f64) -> Self {
        Bucket {
            tokens: burst,
            at: now,
            rate,
        }
    }

    /// Takes `cost` tokens at `now`. False when the bucket has less.
    fn take(&mut self, now: Instant, burst: f64, cost: f64) -> bool {
        let gone = now.saturating_duration_since(self.at).as_secs_f64();
        self.at = self.at.max(now);
        self.tokens = (self.tokens + gone * self.rate).min(burst);
        if self.tokens < cost {
            return false;
        }
        self.tokens -= cost;
        true
    }

    /// Gives `RESET_REFUND` back: a stream was answered with a body.
    fn give(&mut self, burst: f64) {
        self.tokens = (self.tokens + RESET_REFUND).min(burst);
    }
}

/// A count of frames in the current one-second window.
#[derive(Clone, Copy)]
struct Rate {
    start: Instant,
    n: u32,
}

impl Rate {
    fn new(now: Instant) -> Self {
        Rate { start: now, n: 0 }
    }

    /// Counts one frame at `now`. True when this second has more than `max`.
    fn over(&mut self, now: Instant, max: u32) -> bool {
        if now.saturating_duration_since(self.start) >= Duration::from_secs(1) {
            *self = Rate::new(now);
        }
        self.n += 1;
        self.n > max
    }
}

const fn conn_err(code: u32) -> Error {
    Error {
        code,
        stream: false,
    }
}

const fn stream_err(code: u32) -> Error {
    Error { code, stream: true }
}

/// Where a stream id is, as seen from the frames of the client (RFC 9113
/// 5.1). "Open" is both "open" (the body comes) and "half-closed
/// (remote)" (the answer is due): see `Stream::recv`.
#[derive(Debug, PartialEq)]
pub(crate) enum State {
    Idle,
    Open,
    Closed,
}

/// The HTTP/2 state of one connection.
pub(crate) struct H2 {
    /// (connection index << 32): the high half of each request id
    base: u64,
    peer: [u8; 18],
    dec: Decoder,
    /// our MAX_CONCURRENT_STREAMS
    max_streams: usize,
    /// most request body bytes: `maxBodySize`, `maxMultipartSize`
    max_body: usize,
    max_multipart: usize,
    /// the time of the current read (for the body timeout)
    now: Instant,
    /// the client sent its first SETTINGS
    greeted: bool,
    /// the client's MAX_FRAME_SIZE and INITIAL_WINDOW_SIZE
    peer_frame: usize,
    peer_window: i64,
    /// the connection send window
    window: i64,
    /// DATA bytes sent so far: the progress that counts when blocked
    pub(crate) data_sent: u64,
    /// the connection receive window left
    recv: i64,
    /// a header block that waits for CONTINUATION: (stream id,
    /// END_STREAM, bytes so far)
    block: Option<(u32, bool, Vec<u8>)>,
    /// the highest stream id of the client so far (for the stream states)
    seen: u32,
    /// the highest stream id we took in (for GOAWAY): ignored and refused
    /// streams are not in it (RFC 9113 6.8)
    last: u32,
    /// the last streams we reset or refused: frames of the client that
    /// were already on their way are ignored (RFC 9113 5.1). Up to
    /// `max_streams` (at least [RESETS]), so one round trip of resets fits.
    resets: VecDeque<u32>,
    /// the same ids as `resets`, for a fast `contains`
    resets_set: HashSet<u32>,
    /// the last open streams the client reset. A frame on one of them is a
    /// stream error STREAM_CLOSED (RFC 9113 5.1). Apart from `resets`, and
    /// only for streams that were open, so RSTs on closed ids add nothing.
    client_resets: VecDeque<u32>,
    /// the same ids as `client_resets`, for a fast `contains`
    client_resets_set: HashSet<u32>,
    /// request body bytes buffered in all streams (see `data`)
    buffered: usize,
    /// we sent GOAWAY NO_ERROR: no new streams
    away: bool,
    /// the client sent GOAWAY
    peer_away: bool,
    /// a connection error: GOAWAY is sent, close once it is written
    dead: bool,
    streams: HashMap<u32, Stream>,
    /// the last header lines from Dart, their status, and the block of
    /// `:status` and those lines: most answers repeat them
    head_lines: Vec<u8>,
    head_status: u16,
    head_cached: Vec<u8>,
    /// the header block of the answer being sent, kept to reuse its memory
    head_buf: Vec<u8>,
    /// request seq to stream id
    seqs: HashMap<u32, u32>,
    /// streams with body bytes to send, in turn order
    turns: VecDeque<u32>,
    /// Requests whose stream is gone while Dart still works on them: the
    /// client reset them (or they were cut) before Dart answered, a gzip
    /// job of the answer still runs, or a streamed answer was not over.
    /// Request seq to the time it went and, for a streamed answer, its
    /// stream id. A Dart handler can not be stopped, so each one counts
    /// against `max_streams` until its answer comes (any kind, see `due`
    /// and `zipped`), or the end of its streamed answer (see `part`), or
    /// until ZOMBIE_WAIT. After that the handler may still run, and no
    /// longer counts (see ZOMBIE_WAIT). Never more than `max_streams`: a
    /// stream only moves here from `streams`, and a new stream is refused
    /// while both together are full. When the connection dies they are
    /// forgotten while the Dart handlers go on, so a client that connects
    /// again gets a fresh `max_streams`. This is the same limit as in
    /// grpc-go. It is not bounded by `maxConnections`.
    zombies: HashMap<u32, (Instant, Option<u64>)>,
    /// the last request (record head and body), until it fits in Dart's
    /// buffer
    pub(crate) held: Option<(Vec<u8>, Vec<u8>)>,
    /// requests whose stream ended before Dart answered: Dart gets
    /// REQUEST_GONE for each
    pub(crate) gone: Vec<u32>,
    /// streamed answers whose stream ended before Dart's end: Dart gets
    /// STREAM_GONE for each
    pub(crate) gone_streams: Vec<u64>,
    /// Streamed answers whose zombie ran out of time (ZOMBIE_WAIT). Dart got
    /// STREAM_GONE before: the run loop only forgets the stream id.
    pub(crate) expired_streams: Vec<u64>,
    /// streamed request bodies: records for Dart (request seq, kind
    /// BODY_PART or BODY_END, status, bytes), in order
    pub(crate) to_dart: Vec<(u32, u32, u16, Vec<u8>)>,
    /// answer body bytes of all streams not sent yet (see UNSENT_CAP)
    unsent: usize,
    /// streamed body bytes Dart has not acked yet, all streams (see
    /// UNACKED_CAP)
    unacked: u64,
    /// flood counters, see the `MAX_*` limits
    reset_bucket: Bucket,
    reset_burst: f64,
    /// the reset bucket ran out once: we drain (GOAWAY NO_ERROR). Out
    /// again: GOAWAY ENHANCE_YOUR_CALM.
    calm: bool,
    settings_rate: Rate,
    pings_rate: Rate,
    /// RST_STREAMs on closed streams this second (see `dispatch`)
    closed_rsts: Rate,
    unacked_settings: u32,
    unacked_pings: u32,
    empty: u32,
    /// the current frame did real work / counts toward `empty`
    worked: bool,
    counted: bool,
    /// a stream opened or closed: the idle time starts again. PING and
    /// SETTINGS ACKs do not count (see `service` in run.rs).
    pub(crate) stirred: bool,
}

impl H2 {
    /// After the client preface: writes our SETTINGS and raises the
    /// connection window.
    pub(crate) fn new(
        idx: usize,
        peer: [u8; 18],
        max_streams: u32,
        (max_body, max_multipart): (usize, usize),
        (reset_burst, reset_rate): (u32, f64),
        out: &mut Vec<u8>,
    ) -> Box<Self> {
        frame::write_settings(
            out,
            &[
                (MAX_CONCURRENT_STREAMS, max_streams),
                (INITIAL_WINDOW_SIZE, STREAM_WINDOW as u32),
                (MAX_FRAME_SIZE, MAX_FRAME as u32),
                (MAX_HEADER_LIST_SIZE, MAX_HEAD as u32),
                (ENABLE_PUSH, 0),
            ],
        );
        frame::write_window(out, 0, (CONN_WINDOW - DEFAULT_WINDOW) as u32);
        let now = Instant::now();
        // 0: the default, see RESET_BURST_MIN
        let burst = match reset_burst {
            0 => f64::from(max_streams.saturating_mul(2).max(RESET_BURST_MIN)),
            n => f64::from(n),
        };
        Box::new(H2 {
            base: (idx as u64) << 32,
            peer,
            dec: Decoder::new(TABLE_SIZE),
            max_streams: max_streams as usize,
            max_body,
            max_multipart,
            now,
            greeted: false,
            peer_frame: MAX_FRAME,
            peer_window: DEFAULT_WINDOW,
            window: DEFAULT_WINDOW,
            data_sent: 0,
            recv: CONN_WINDOW,
            block: None,
            seen: 0,
            last: 0,
            resets: VecDeque::new(),
            resets_set: HashSet::new(),
            client_resets: VecDeque::new(),
            client_resets_set: HashSet::new(),
            buffered: 0,
            away: false,
            peer_away: false,
            dead: false,
            streams: HashMap::new(),
            head_lines: Vec::new(),
            head_status: 0,
            head_cached: Vec::new(),
            head_buf: Vec::new(),
            seqs: HashMap::new(),
            turns: VecDeque::new(),
            zombies: HashMap::new(),
            held: None,
            gone: Vec::new(),
            gone_streams: Vec::new(),
            expired_streams: Vec::new(),
            to_dart: Vec::new(),
            unsent: 0,
            unacked: 0,
            reset_bucket: Bucket::new(now, burst, reset_rate),
            reset_burst: burst,
            calm: false,
            settings_rate: Rate::new(now),
            pings_rate: Rate::new(now),
            closed_rsts: Rate::new(now),
            unacked_settings: 0,
            unacked_pings: 0,
            empty: 0,
            worked: false,
            counted: false,
            stirred: false,
        })
    }

    /// Bytes went out to the client: our ACKs are on their way.
    pub(crate) fn flushed(&mut self) {
        self.unacked_settings = 0;
        self.unacked_pings = 0;
    }

    /// Streams whose answer is not all sent.
    pub(crate) fn open(&self) -> usize {
        self.streams.len()
    }

    /// The seqs of requests Dart has and did not answer (for REQUEST_GONE
    /// when the connection dies).
    pub(crate) fn unanswered(&self) -> impl Iterator<Item = u32> + '_ {
        self.streams
            .values()
            .filter(|s| !s.answered && s.known())
            .map(|s| s.seq)
            .chain(self.gone.iter().copied())
    }

    /// The seqs of requests whose body still streams to Dart (for
    /// BODY_END when the connection dies).
    pub(crate) fn streaming(&self) -> impl Iterator<Item = u32> + '_ {
        self.streams
            .values()
            .filter(|s| s.streaming())
            .map(|s| s.seq)
    }

    /// The ids of Dart's streamed answers that are not over (for
    /// STREAM_GONE when the connection dies).
    pub(crate) fn streamed(&self) -> impl Iterator<Item = u64> + '_ {
        self.streams
            .values()
            .filter_map(|s| s.sid)
            .chain(self.gone_streams.iter().copied())
            .chain(self.expired_streams.iter().copied())
            .chain(self.zombies.values().filter_map(|z| z.1))
    }

    /// When the oldest wait for request body bytes started, if a buffered
    /// body comes (for the body timeout). A streamed body has no timeout:
    /// a gRPC stream may be quiet for long.
    pub(crate) fn body_since(&self) -> Option<Instant> {
        if self.dead {
            return None; // `expire` does nothing then: no timer must wait
        }
        self.streams
            .values()
            .filter_map(|s| s.recv.as_ref().filter(|r| !r.streamed).map(|r| r.last))
            .min()
    }

    /// When the oldest stream with answer bytes began to wait for a window
    /// of 0 (for the stall timer, see `stall_mark`).
    pub(crate) fn stall_since(&self) -> Option<Instant> {
        if self.dead {
            return None;
        }
        self.streams.values().filter_map(|s| s.stalled).min()
    }

    /// Notes which streams wait for their window (`now` is when they began
    /// to). Called after each write round.
    pub(crate) fn stall_mark(&mut self, now: Instant) {
        for id in &self.turns {
            if let Some(s) = self.streams.get_mut(id) {
                if s.window <= 0 && s.unsent() > 0 {
                    s.stalled.get_or_insert(now);
                } else {
                    s.stalled = None;
                }
            }
        }
    }

    /// The body timeout: a stream whose body got no bytes for `timeout`
    /// gets 408. Other streams go on. A streamed answer whose stream
    /// window stayed 0 for `wait` is cancelled (RST_STREAM CANCEL).
    pub(crate) fn expire(
        &mut self,
        now: Instant,
        timeout: Duration,
        wait: Duration,
        date: &[u8],
        out: &mut Vec<u8>,
    ) {
        if self.dead {
            return; // no frame after a GOAWAY with an error
        }
        // a connection with no frames has an old `now`: `drop_stream`
        // stamps its zombies with it
        self.now = now;
        // each timer source must change its state here, or the timer
        // would run again at once
        let mut dull = Vec::new();
        for (&id, s) in self.streams.iter_mut() {
            if let Some(t) = s.stalled {
                if s.window > 0 || s.unsent() == 0 {
                    s.stalled = None;
                } else if t + wait <= now {
                    dull.push(id);
                }
            }
        }
        for id in dull {
            self.reset(out, id, frame::CANCEL);
        }
        let late: Vec<u32> = self
            .streams
            .iter()
            .filter(|(_, s)| {
                s.recv
                    .as_ref()
                    .is_some_and(|r| !r.streamed && r.last + timeout <= now)
            })
            .map(|(&id, _)| id)
            .collect();
        for id in late {
            self.reply(out, id, 408, false, date);
            self.drop_stream(id);
        }
        self.settle(out);
    }

    /// True when the connection can close once the write buffer is out.
    pub(crate) fn over(&self) -> bool {
        self.dead || ((self.away || self.peer_away) && self.streams.is_empty())
    }

    pub(crate) fn state(&self, id: u32) -> State {
        if self.streams.contains_key(&id) {
            State::Open
        } else if !id.is_multiple_of(2) && id <= self.seen {
            State::Closed
        } else {
            State::Idle
        }
    }

    /// Reads one whole frame of `buf[*pos..]`. A request goes to `held`.
    /// False when no whole frame is there (or the connection is dead).
    pub(crate) fn step(
        &mut self,
        buf: &[u8],
        pos: &mut usize,
        out: &mut Vec<u8>,
        seq: &mut u32,
        date: &[u8],
        now: Instant,
    ) -> bool {
        self.now = now;
        if self.dead {
            *pos = buf.len(); // the connection closes: drop the rest
            return false;
        }
        let Some(h) = frame::read_head(&buf[*pos..]) else {
            return false;
        };
        if h.len > MAX_FRAME {
            self.fail(out, frame::FRAME_SIZE_ERROR);
            return false;
        }
        let end = *pos + frame::HEAD + h.len;
        if buf.len() < end {
            return false;
        }
        let payload = &buf[*pos + frame::HEAD..end];
        *pos = end;
        if let Err(e) = self.frame(&h, payload, out, seq, date) {
            // a stream error counts as a reset (CVE-2025-8671)
            if e.stream && self.spend().is_ok() {
                self.reset(out, h.id, e.code);
            } else if e.stream {
                self.fail(out, frame::ENHANCE_YOUR_CALM);
            } else {
                self.fail(out, e.code);
            }
        }
        if self.calm {
            self.drain(out); // see `spend_n`
        }
        self.settle(out);
        true
    }

    /// Gives connection window back to the client: all it sent, also the
    /// streamed bytes Dart has not acked (their stream window holds them
    /// back, so one stream holds at most STREAM_WINDOW). Only once enough
    /// is due, or the client is nearly stuck.
    fn settle(&mut self, out: &mut Vec<u8>) {
        if self.dead {
            return;
        }
        let due = CONN_WINDOW - self.recv;
        if due > 0 && (due >= CONN_WINDOW / 2 || self.recv < ACK_BATCH as i64) {
            frame::write_window(out, 0, due as u32);
            self.recv += due;
        }
    }

    /// True when `buf` holds no whole frame: more bytes are needed.
    pub(crate) fn partial(buf: &[u8]) -> bool {
        frame::read_head(buf).is_none_or(|h| buf.len() < frame::HEAD + h.len)
    }

    /// One frame. A frame that counts (see `counts`) and did no work adds
    /// one to the empty count. Work sets it to 0.
    fn frame(
        &mut self,
        h: &Head,
        p: &[u8],
        out: &mut Vec<u8>,
        seq: &mut u32,
        date: &[u8],
    ) -> Result<(), Error> {
        self.worked = false;
        self.counted = false;
        self.dispatch(h, p, out, seq, date)?;
        if self.worked {
            self.empty = 0;
        } else if self.counted {
            self.empty += 1;
            if self.empty > MAX_EMPTY {
                return Err(conn_err(frame::ENHANCE_YOUR_CALM));
            }
        }
        Ok(())
    }

    fn dispatch(
        &mut self,
        h: &Head,
        p: &[u8],
        out: &mut Vec<u8>,
        seq: &mut u32,
        date: &[u8],
    ) -> Result<(), Error> {
        // the client's first frame must be SETTINGS (RFC 9113 3.4)
        if !self.greeted {
            if h.kind != frame::SETTINGS || h.flags & frame::ACK != 0 {
                return Err(conn_err(frame::PROTOCOL_ERROR));
            }
            self.greeted = true;
        }
        // a header block in parts: only its CONTINUATION frames may come
        // in between (RFC 9113 6.10)
        if let Some((id, ..)) = self.block
            && (h.kind != frame::CONTINUATION || h.id != id)
        {
            return Err(conn_err(frame::PROTOCOL_ERROR));
        }
        let f = frame::parse(h, p)?;
        self.counted = Self::counts(&f);
        match f {
            Frame::Data { end, data, flow } => self.data(h.id, data, flow, end, out, date),
            Frame::Headers {
                end_stream,
                end_headers,
                dep,
                block,
            } => {
                // ponytail: a stream that depends on itself is a stream
                // error in the RFC. A connection error is allowed too and
                // needs no HPACK decode of the block.
                if dep == Some(h.id) || h.id.is_multiple_of(2) {
                    return Err(conn_err(frame::PROTOCOL_ERROR));
                }
                match self.state(h.id) {
                    // a stream we or the client reset: after the decode
                    // (see `request`)
                    State::Closed
                        if self.client_resets_set.contains(&h.id)
                            || self.resets_set.contains(&h.id) => {}
                    State::Closed => return Err(conn_err(frame::STREAM_CLOSED)),
                    // trailers, or an error after the decode (the table
                    // must stay in sync): see `request`
                    State::Open | State::Idle => {}
                }
                if end_headers {
                    self.request(h.id, end_stream, block, out, seq, date)
                } else {
                    self.block = Some((h.id, end_stream, block.to_vec()));
                    Ok(())
                }
            }
            Frame::Continuation { end_headers, block } => {
                let Some((_, _, b)) = self.block.as_mut() else {
                    return Err(conn_err(frame::PROTOCOL_ERROR));
                };
                // the decoded list over MAX_HEAD is a 431 for the stream
                // (see `request`), only a far bigger block cuts the
                // connection
                if b.len() + block.len() > MAX_BLOCK {
                    return Err(conn_err(frame::ENHANCE_YOUR_CALM));
                }
                b.extend_from_slice(block);
                if end_headers {
                    let (id, end_stream, b) = self.block.take().unwrap();
                    return self.request(id, end_stream, &b, out, seq, date);
                }
                Ok(())
            }
            // the priority scheme is deprecated (RFC 9113 5.3.2): ignored
            Frame::Priority { dep } if dep == h.id => Err(stream_err(frame::PROTOCOL_ERROR)),
            Frame::Priority { .. } => Ok(()),
            Frame::RstStream { .. } => {
                if self.state(h.id) == State::Idle {
                    return Err(conn_err(frame::PROTOCOL_ERROR));
                }
                // A closed stream: the RST cancels no work (eg. a proxy's
                // cancel crossed our answer, or our refusal after GOAWAY).
                // Up to one burst of them a second take no reset token,
                // so a burst of them can not cut the other users of the
                // connection. Past that each one takes a token like any
                // reset: a flood drains, then is cut (see `spend_n`).
                if !self.streams.contains_key(&h.id) {
                    if self.closed_rsts.over(self.now, self.reset_burst as u32) {
                        self.spend()?;
                    }
                    return Ok(());
                }
                // it cancels open work: a reset token
                self.spend()?;
                // an open stream is remembered: a later frame on it is
                // STREAM_CLOSED (RFC 9113 5.1)
                if !self.client_resets_set.contains(&h.id) {
                    if self.client_resets.len() >= self.max_streams.max(RESETS)
                        && let Some(old) = self.client_resets.pop_front()
                    {
                        self.client_resets_set.remove(&old);
                    }
                    self.client_resets.push_back(h.id);
                    self.client_resets_set.insert(h.id);
                }
                self.drop_stream(h.id);
                Ok(())
            }
            Frame::Settings { ack: true, .. } => Ok(()),
            Frame::Settings { params, .. } => {
                self.unacked_settings += 1;
                if self.settings_rate.over(self.now, MAX_SETTINGS)
                    || self.unacked_settings > MAX_UNACKED_SETTINGS
                {
                    return Err(conn_err(frame::ENHANCE_YOUR_CALM));
                }
                self.settings(params)?;
                frame::write(out, frame::SETTINGS, frame::ACK, 0, &[]);
                Ok(())
            }
            // a client may not push (RFC 9113 8.4)
            Frame::PushPromise { .. } => Err(conn_err(frame::PROTOCOL_ERROR)),
            Frame::Ping { ack: true, .. } => Ok(()),
            Frame::Ping { data, .. } => {
                self.unacked_pings += 1;
                if self.pings_rate.over(self.now, MAX_PINGS)
                    || self.unacked_pings > MAX_UNACKED_PINGS
                {
                    return Err(conn_err(frame::ENHANCE_YOUR_CALM));
                }
                frame::write(out, frame::PING, frame::ACK, 0, &data);
                Ok(())
            }
            // the client opens no more streams: finish the open ones
            Frame::GoAway { .. } => {
                self.peer_away = true;
                Ok(())
            }
            Frame::WindowUpdate { inc } => self.window_update(h.id, inc),
            Frame::Unknown => Ok(()),
        }
    }

    /// True for a frame that counts toward `MAX_EMPTY`: HEADERS, empty DATA,
    /// empty CONTINUATION, PRIORITY, an unknown type. The count starts
    /// again only when real work is done: a HEADERS that opens a stream,
    /// DATA with bytes or END_STREAM on an open stream (see `request` and
    /// `data`). The rest have their own limits or are normal in a row.
    /// WINDOW_UPDATE is one: package:http2 sends one for each DATA it got,
    /// also after the stream ended. DATA with bytes is one too: it is
    /// bounded by the receive window, and after we reset an upload (413)
    /// a whole window of it may still be in flight.
    fn counts(f: &Frame) -> bool {
        match f {
            Frame::Continuation { block, .. } => block.is_empty(),
            Frame::Data { data, .. } => data.is_empty(),
            Frame::Headers { .. } | Frame::Priority { .. } | Frame::Unknown => true,
            _ => false,
        }
    }

    /// Takes a token of the reset bucket: the client costs us a reset or a
    /// refusal. See `spend_n` for an empty bucket.
    fn spend(&mut self) -> Result<(), Error> {
        self.spend_n(1.0)
    }

    /// Like `spend`, for a part of a token. The first time the bucket is
    /// empty, the connection drains: `step` sends GOAWAY NO_ERROR with the
    /// last stream we took, so the open streams (other users behind a
    /// proxy) still get their answers, and new ones are refused. The
    /// bucket gets one more burst for the frames already in flight. Empty
    /// again: ENHANCE_YOUR_CALM, the connection closes at once.
    fn spend_n(&mut self, cost: f64) -> Result<(), Error> {
        if self.reset_bucket.take(self.now, self.reset_burst, cost) {
            return Ok(());
        }
        if self.calm {
            return Err(conn_err(frame::ENHANCE_YOUR_CALM));
        }
        self.calm = true;
        // the frame that ran it out pays from the new burst
        self.reset_bucket.tokens = (self.reset_burst - cost).max(0.0);
        Ok(())
    }

    /// DATA: request body bytes. `flow` (with the padding) counts against
    /// the receive windows. With `end` the request is whole and goes to
    /// `held`.
    fn data(
        &mut self,
        id: u32,
        data: &[u8],
        flow: usize,
        end: bool,
        out: &mut Vec<u8>,
        date: &[u8],
    ) -> Result<(), Error> {
        let state = self.state(id);
        if state == State::Idle {
            return Err(conn_err(frame::PROTOCOL_ERROR));
        }
        // the connection window counts all DATA, also of streams that are
        // gone
        if flow as i64 > self.recv {
            return Err(conn_err(frame::FLOW_CONTROL_ERROR));
        }
        self.recv -= flow as i64;
        // the window comes back in `settle`, after each frame
        if state == State::Closed {
            // After its own RST_STREAM the client may not send DATA: a
            // stream error (RFC 9113 5.1). DATA of a stream we reset may
            // still be on its way: ignored. On a stream that ended it is
            // a connection error.
            if self.client_resets_set.contains(&id) {
                return Err(stream_err(frame::STREAM_CLOSED));
            }
            if self.resets_set.contains(&id) {
                return Ok(());
            }
            return Err(conn_err(frame::STREAM_CLOSED));
        }
        if self.unacked + data.len() as u64 > UNACKED_CAP
            && self.streams[&id].streaming()
            && !self.evict(id, data.len() as u64, out)
        {
            return Ok(()); // this stream was the biggest: it is reset
        }
        let s = self.streams.get_mut(&id).expect("an open stream");
        let Some(r) = s.recv.as_mut() else {
            // the request is whole: no more DATA (RFC 9113 5.1)
            return Err(stream_err(frame::STREAM_CLOSED));
        };
        if flow as i64 > r.window {
            return Err(stream_err(frame::FLOW_CONTROL_ERROR));
        }
        r.window -= flow as i64;
        r.last = self.now;
        let got = r.taken + (r.data.len() + data.len()) as u64;
        // the body must have the length the request said (RFC 9113 8.1.1)
        if r.length.is_some_and(|l| got > l || (end && got < l)) {
            return Err(stream_err(frame::PROTOCOL_ERROR));
        }
        if r.streamed {
            // to Dart at once. The stream window comes back on Dart's
            // ack, the padding now (Dart never sees it). The connection
            // window comes back now (`settle`).
            r.taken = got;
            r.acked += (flow - data.len()) as u64;
            give_back(out, id, r);
            if !data.is_empty() || end {
                self.worked = true;
            }
            if !data.is_empty() {
                s.owed += data.len() as u64;
                self.unacked += data.len() as u64;
                // Join it to a part of this stream that waits for Dart:
                // 1M frames of 1 byte must not be 1M records.
                let last = self
                    .to_dart
                    .iter_mut()
                    .rev()
                    .find(|p| p.0 == s.seq && p.1 == BODY_PART);
                match last {
                    Some(p) => p.3.extend_from_slice(data),
                    None => self.to_dart.push((s.seq, BODY_PART, 0, data.to_vec())),
                }
            }
            if end {
                s.recv = None;
                self.to_dart.push((s.seq, BODY_END, 0, Vec::new()));
            }
            return Ok(());
        }
        if got > r.max as u64 {
            // only this stream ends, the connection goes on
            self.spend()?;
            self.reply(out, id, 413, false, date);
            self.drop_stream(id);
            return Ok(());
        }
        // all bodies of the connection together: BODY_STREAMS_CAP full-size
        // uploads at once. Dart has not seen this request, so the client
        // may retry it.
        if self.buffered + data.len()
            > self
                .max_body
                .max(self.max_multipart)
                .saturating_mul(BODY_STREAMS_CAP)
        {
            self.spend()?;
            self.reset(out, id, frame::REFUSED_STREAM);
            return Ok(());
        }
        if !data.is_empty() || end {
            self.worked = true;
        }
        self.buffered += data.len();
        r.data.extend_from_slice(data);
        if end {
            let r = s.recv.take().unwrap();
            self.buffered -= r.data.len();
            self.held = Some((r.rec, r.data));
        } else if r.window < STREAM_WINDOW / 2 {
            // give the window back once half of it is used. Not after
            // END_STREAM: no more DATA comes.
            frame::write_window(out, id, (STREAM_WINDOW - r.window) as u32);
            r.window = STREAM_WINDOW;
        }
        Ok(())
    }

    /// `n` more streamed bytes for stream `id` would pass UNACKED_CAP:
    /// resets the streams that hold the most unacked bytes (ties: the
    /// oldest) until they fit. So one user of a proxy connection that
    /// pins the cap with slow handlers loses its own streams, not the
    /// next user's. ENHANCE_YOUR_CALM, not FLOW_CONTROL_ERROR: the client
    /// kept to its windows, it only sends more than the server takes
    /// (gRPC maps it to RESOURCE_EXHAUSTED). No reset token: the client
    /// broke no rule, and each such stream cost it a full window. False
    /// when stream `id` itself was reset.
    fn evict(&mut self, id: u32, n: u64, out: &mut Vec<u8>) -> bool {
        while self.unacked + n > UNACKED_CAP {
            let Some((&victim, _)) = self
                .streams
                .iter()
                .filter(|(_, s)| s.owed > 0)
                .max_by_key(|(sid, s)| (s.owed, std::cmp::Reverse(**sid)))
            else {
                break; // nothing to free (can not happen: n <= MAX_FRAME)
            };
            self.reset(out, victim, frame::ENHANCE_YOUR_CALM);
            if victim == id {
                return false;
            }
        }
        true
    }

    fn window_update(&mut self, id: u32, inc: u32) -> Result<(), Error> {
        let inc = i64::from(inc);
        if id == 0 {
            if inc == 0 {
                return Err(conn_err(frame::PROTOCOL_ERROR));
            }
            self.window += inc;
            if self.window > frame::MAX_WINDOW {
                return Err(conn_err(frame::FLOW_CONTROL_ERROR));
            }
            return Ok(());
        }
        if self.state(id) == State::Idle {
            return Err(conn_err(frame::PROTOCOL_ERROR));
        }
        if inc == 0 {
            return Err(stream_err(frame::PROTOCOL_ERROR));
        }
        if let Some(s) = self.streams.get_mut(&id) {
            s.window += inc;
            if s.window > frame::MAX_WINDOW {
                return Err(stream_err(frame::FLOW_CONTROL_ERROR));
            }
        }
        Ok(())
    }

    fn settings(&mut self, params: &[u8]) -> Result<(), Error> {
        for (id, v) in frame::settings(params) {
            match id {
                ENABLE_PUSH if v > 1 => return Err(conn_err(frame::PROTOCOL_ERROR)),
                INITIAL_WINDOW_SIZE => {
                    let v = i64::from(v);
                    if v > frame::MAX_WINDOW {
                        return Err(conn_err(frame::FLOW_CONTROL_ERROR));
                    }
                    // the change applies to all open streams (RFC 9113 6.9.2)
                    let delta = v - self.peer_window;
                    for s in self.streams.values_mut() {
                        s.window += delta;
                        if s.window > frame::MAX_WINDOW {
                            return Err(conn_err(frame::FLOW_CONTROL_ERROR));
                        }
                    }
                    self.peer_window = v;
                }
                MAX_FRAME_SIZE => {
                    if !(MAX_FRAME as u32..=MAX_FRAME_LIMIT).contains(&v) {
                        return Err(conn_err(frame::PROTOCOL_ERROR));
                    }
                    self.peer_frame = v as usize;
                }
                // the rest does not matter to us: we never push, and our
                // encoder uses no dynamic table
                _ => {}
            }
        }
        Ok(())
    }

    /// A whole header block of stream `id`: decode it, check it, and make
    /// the request record (or answer the error).
    fn request(
        &mut self,
        id: u32,
        end_stream: bool,
        block: &[u8],
        out: &mut Vec<u8>,
        seq: &mut u32,
        date: &[u8],
    ) -> Result<(), Error> {
        // Some(true): the body comes, so these are trailers.
        // Some(false): the request is whole.
        let known = self.streams.get(&id).map(|s| s.recv.is_some());
        let closed = known.is_none() && self.state(id) == State::Closed;
        // trailers: a pseudo header is malformed (RFC 9113 8.1)
        let mut req = Req {
            regular: known == Some(true),
            ..Req::default()
        };
        // any HPACK error leaves our table out of sync with the client's:
        // the connection must close
        // a decode we drop costs a token (the client's own reset costs
        // one as a stream error)
        if closed && !self.client_resets_set.contains(&id) {
            self.spend()?;
        }
        let list = self
            .dec
            .decode(block, MAX_HEAD, |n, v| req.field(n, v))
            .map_err(|_| conn_err(frame::COMPRESSION_ERROR))?;
        if closed {
            // the client reset it: STREAM_CLOSED. We reset it: ignored.
            return if self.client_resets_set.contains(&id) {
                Err(stream_err(frame::STREAM_CLOSED))
            } else {
                Ok(())
            };
        }
        match known {
            Some(false) => return Err(stream_err(frame::STREAM_CLOSED)),
            // trailers end the body. Dart has no request trailers: they
            // are dropped.
            Some(true) if end_stream && !req.bad => {
                return self.data(id, b"", 0, true, out, date);
            }
            Some(true) => return Err(stream_err(frame::PROTOCOL_ERROR)),
            None => {}
        }
        self.seen = id;
        if self.away || self.peer_away {
            self.spend()?;
            self.remember(id);
            // a flood is bounded by the bucket, not by the empty count: a
            // busy client has many requests in flight after our GOAWAY
            self.counted = false;
            // after our GOAWAY: not taken (RFC 9113 6.8). REFUSED_STREAM
            // tells the client (or proxy) at once that it may retry it
            // on another connection.
            if self.away {
                frame::write_rst(out, id, frame::REFUSED_STREAM);
            }
            return Ok(());
        }
        // Requests Dart still works on count too (see `zombies`). A refusal
        // takes a token either way: each one costs an HPACK decode, so a
        // client that keeps sending HEADERS while full is still cut. A real
        // client is not: it is only full while it has `max_streams`
        // handlers in Dart, and gRPC retries a REFUSED_STREAM only once
        // on its own (gRPC A6), so it takes few tokens.
        if self.full() {
            // the zombies alone fill it: the client can not see them
            if self.streams.len() < self.max_streams {
                self.spend_n(ZOMBIE_REFUSAL_COST)?;
                // the token bounds a flood: it is not an empty frame, or a
                // client that retries after a mass cancel would be cut
                self.counted = false;
            } else {
                self.spend()?;
            }
            frame::write_rst(out, id, frame::REFUSED_STREAM);
            self.remember(id);
            return Ok(());
        }
        // too many answer bytes wait for this client: no new work until
        // it reads. Like the zombies, the client may not see it.
        if self.unsent > UNSENT_CAP {
            self.spend_n(ZOMBIE_REFUSAL_COST)?;
            self.counted = false;
            frame::write_rst(out, id, frame::REFUSED_STREAM);
            self.remember(id);
            return Ok(());
        }
        self.last = id;
        let check = if list == Decoded::TooLarge {
            Check::Status(431)
        } else {
            req.check(end_stream)
        };
        match check {
            Check::Bad => Err(stream_err(frame::PROTOCOL_ERROR)),
            Check::Status(status) => {
                self.spend()?;
                self.reply(out, id, status, end_stream, date);
                Ok(())
            }
            Check::Ok => {
                let max = if req.form {
                    self.max_multipart
                } else {
                    self.max_body
                };
                let length = req.length;
                // no body limit: dox_grpc limits each message
                let streamed = req.grpc && !end_stream;
                if !streamed && length.is_some_and(|l| l > max as u64) {
                    // too big: say no before the body comes
                    self.spend()?;
                    self.reply(out, id, 413, false, date);
                    return Ok(());
                }
                let head = req.method == b"HEAD";
                let flags = if streamed {
                    FLAG_H2 | FLAG_STREAMED
                } else {
                    FLAG_H2
                };
                let mut rec = req.record(self.base | u64::from(*seq), &self.peer, flags);
                // Dart gets the request now, or once the body is whole
                if end_stream || streamed {
                    self.held = Some((std::mem::take(&mut rec), Vec::new()));
                }
                let recv = (!end_stream).then(|| {
                    Box::new(Recv {
                        rec,
                        data: Vec::new(),
                        max,
                        length,
                        window: STREAM_WINDOW,
                        last: self.now,
                        streamed,
                        taken: 0,
                        acked: 0,
                    })
                });
                let s = Stream::new(*seq, self.peer_window, head, recv);
                self.streams.insert(id, s);
                self.stirred = true;
                self.worked = true;
                self.seqs.insert(*seq, id);
                *seq = seq.wrapping_add(1);
                Ok(())
            }
        }
    }

    /// No room for a new stream: the open streams and the `zombies` that
    /// are not older than ZOMBIE_WAIT fill `max_streams`.
    fn full(&mut self) -> bool {
        if self.streams.len() + self.zombies.len() < self.max_streams {
            return false;
        }
        // only when full, so at most `max_streams` entries, and each refusal
        // takes a token: the old ones go now, no timer is needed
        let now = self.now;
        let expired = &mut self.expired_streams;
        self.zombies.retain(|_, (at, sid)| {
            let keep = now.saturating_duration_since(*at) < ZOMBIE_WAIT;
            if let Some(sid) = sid.filter(|_| !keep) {
                expired.push(sid);
            }
            keep
        });
        self.streams.len() + self.zombies.len() >= self.max_streams
    }

    /// Answers from Rust (eg. 405) with no body. When the client still
    /// sends a body, RST_STREAM NO_ERROR then tells it to stop (RFC 9113
    /// 8.1).
    fn reply(&mut self, out: &mut Vec<u8>, id: u32, status: u16, whole: bool, date: &[u8]) {
        let mut block = Vec::new();
        status_field(status, &mut block);
        encode_field(b"content-length", b"0", &mut block);
        date_field(date, &mut block);
        frame::write_headers(out, id, &block, true, self.peer_frame);
        if !whole {
            frame::write_rst(out, id, frame::NO_ERROR);
            self.remember(id);
        }
    }

    /// Notes that we reset stream `id`. Only the last [RESETS] are kept.
    fn remember(&mut self, id: u32) {
        // already kept: a second copy would leave the set short when the
        // first one is evicted
        if self.resets_set.contains(&id) {
            return;
        }
        if self.resets.len() >= self.max_streams.max(RESETS)
            && let Some(old) = self.resets.pop_front()
        {
            self.resets_set.remove(&old);
        }
        self.resets.push_back(id);
        self.resets_set.insert(id);
    }

    /// A stream error: RST_STREAM, and the stream is gone.
    fn reset(&mut self, out: &mut Vec<u8>, id: u32, code: u32) {
        frame::write_rst(out, id, code);
        self.remember(id);
        self.drop_stream(id);
    }

    fn drop_stream(&mut self, id: u32) {
        if let Some(s) = self.streams.remove(&id) {
            self.stirred = true;
            if s.turn {
                self.turns.retain(|&t| t != id);
            }
            self.seqs.remove(&s.seq);
            self.unsent -= s.unsent();
            // what Dart never acked will not be acked now
            self.unacked -= s.owed;
            // Dart knows the request once its body is whole (or streams)
            if !s.answered && s.known() {
                self.gone.push(s.seq);
            }
            // Dart (or a gzip job) still works on it, or a streamed answer
            // is not over: it keeps its slot
            if (!s.answered && s.known()) || s.zip.is_some() || (s.sid.is_some() && !s.done) {
                self.zombies
                    .insert(s.seq, (self.now, s.sid.filter(|_| !s.done)));
            }
            if s.streaming() {
                self.to_dart.push((s.seq, BODY_END, 1, Vec::new()));
            }
            if let Some(r) = &s.recv {
                self.buffered -= r.data.len();
            }
            if let Some(sid) = s.sid.filter(|_| !s.done) {
                self.gone_streams.push(sid);
            }
        }
    }

    /// The answer of stream `id` is all out. A request body that still
    /// comes (streamed) is not needed any more: RST_STREAM NO_ERROR tells
    /// the client to stop (RFC 9113 8.1). `paid` is true when the answer
    /// sent a body: only then the reset bucket gets a refund.
    fn finish(&mut self, out: &mut Vec<u8>, id: u32, paid: bool) {
        if let Some(s) = self.streams.get_mut(&id)
            && s.recv.is_some()
        {
            // a streamed body ends for Dart with status 2: we answered
            // first, nobody failed
            if s.streaming() {
                s.recv = None;
                self.to_dart.push((s.seq, BODY_END, 2, Vec::new()));
            }
            frame::write_rst(out, id, frame::NO_ERROR);
            self.remember(id);
        }
        if paid && self.streams.contains_key(&id) {
            self.reset_bucket.give(self.reset_burst);
        }
        self.drop_stream(id);
        self.settle(out);
    }

    /// Dart consumed `n` bytes of the streamed body of request `seq`
    /// (BODY_ACK): the client may send them again.
    pub(crate) fn ack(&mut self, seq: u32, n: u64, out: &mut Vec<u8>) {
        if self.dead {
            return;
        }
        let Some(&id) = self.seqs.get(&seq) else {
            return;
        };
        let Some(s) = self.streams.get_mut(&id) else {
            return;
        };
        // never more than owed (also after the body ended)
        let free = n.min(s.owed);
        s.owed -= free;
        self.unacked -= free;
        if let Some(r) = s.recv.as_mut().filter(|r| r.streamed) {
            // never more than the client used: the window stays <= ours
            let used = (STREAM_WINDOW - r.window) as u64;
            r.acked = r.acked.saturating_add(n).min(used);
            give_back(out, id, r);
        } // else: the body ended, no more DATA comes
    }

    /// No more frames come (the client closed its side): each body that
    /// still streams ends with BODY_END 1. The answers may still go out.
    pub(crate) fn cut_bodies(&mut self) {
        for s in self.streams.values_mut() {
            if s.streaming() {
                s.recv = None;
                self.to_dart.push((s.seq, BODY_END, 1, Vec::new()));
            }
        }
    }

    /// A connection error: GOAWAY with this code, then close.
    fn fail(&mut self, out: &mut Vec<u8>, code: u32) {
        if !self.dead {
            frame::write_goaway(out, self.last, code);
            self.dead = true;
        }
    }

    /// Graceful close: GOAWAY NO_ERROR with the last stream id. The open
    /// streams finish, new ones are ignored.
    pub(crate) fn drain(&mut self, out: &mut Vec<u8>) {
        if !self.away && !self.dead {
            frame::write_goaway(out, self.last, frame::NO_ERROR);
            self.away = true;
        }
    }

    /// The stream of request `seq`, when it waits for its answer.
    /// Each answer of Dart comes here first (whole, streamed start, gzip,
    /// refused), so a request that was gone frees its slot.
    fn due(&mut self, seq: u32) -> Option<(u32, &mut Stream)> {
        if !self.zombies.is_empty() {
            self.zombies.remove(&seq);
        }
        if self.dead {
            return None;
        }
        let id = *self.seqs.get(&seq)?;
        let s = self.streams.get_mut(&id)?;
        (!s.answered).then_some((id, s))
    }

    /// A gzip job starts for the answer of `seq`: keep its status and
    /// lines. False when there is nothing to zip for (HEAD, or the stream
    /// is gone).
    pub(crate) fn defer(&mut self, seq: u32, status: u16, lines: &[u8]) -> bool {
        match self.due(seq) {
            Some((_, s)) if !s.head => {
                s.answered = true;
                s.zip = Some((status, lines.to_vec()));
                true
            }
            _ => false,
        }
    }

    /// The gzip job of `seq` is done: send the answer.
    pub(crate) fn zipped(&mut self, seq: u32, body: &[u8], date: &[u8], out: &mut Vec<u8>) {
        if !self.zombies.is_empty() {
            self.zombies.remove(&seq);
        }
        let Some(&id) = self.seqs.get(&seq) else {
            return;
        };
        let Some((status, lines)) = self.streams.get_mut(&id).and_then(|s| s.zip.take()) else {
            return;
        };
        self.send_answer(id, status, &lines, body, date, out);
    }

    /// A whole answer (kind 0) from Dart for request `seq`.
    pub(crate) fn answer(
        &mut self,
        seq: u32,
        status: u16,
        lines: &[u8],
        body: &[u8],
        date: &[u8],
        out: &mut Vec<u8>,
    ) {
        let Some((id, s)) = self.due(seq) else {
            return; // the client reset it (or a second answer)
        };
        s.answered = true;
        self.send_answer(id, status, lines, body, date, out);
    }

    /// A WebSocket accept (kind 1) can not come on HTTP/2 (no request has
    /// flag bit 1), nor can a broken kind 6: the stream gets RST_STREAM
    /// INTERNAL_ERROR.
    pub(crate) fn refuse(&mut self, seq: u32, out: &mut Vec<u8>) {
        if let Some((id, s)) = self.due(seq) {
            s.answered = true;
            self.reset(out, id, frame::INTERNAL_ERROR);
        }
    }

    /// A streamed answer (kind 6) of Dart's stream `sid`: HEADERS, then
    /// the first part. False when it has no stream (the client reset it,
    /// or the answer has no body): Dart must stop.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        &mut self,
        seq: u32,
        status: u16,
        lines: &[u8],
        sid: u64,
        first: &[u8],
        date: &[u8],
        out: &mut Vec<u8>,
    ) -> bool {
        let max = self.peer_frame;
        let Some((id, s)) = self.due(seq) else {
            return false;
        };
        s.answered = true;
        let status = final_status(status);
        let no_body = status == 204 || status == 304;
        // Dart's content-length, when it set one: the parts must match it
        let left = match length_line(lines) {
            Some(Some(n)) if !no_body => Some(n),
            _ => None,
        };
        let silent = no_body || s.head;
        frame::write_headers(out, id, &head_block(status, lines, left, date), silent, max);
        if silent {
            self.finish(out, id, false);
            return false;
        }
        s.sid = Some(sid);
        s.left = left;
        self.part(seq, first, false, true, b"", out);
        true
    }

    /// A part (kind 7) or the end (kind 8, `ok` false when it failed) of
    /// the streamed answer of `seq`. The bytes wait in the stream until
    /// the windows let them go (see [send]). The end may have `trailers`
    /// (header lines): they go out last, in HEADERS with END_STREAM.
    pub(crate) fn part(
        &mut self,
        seq: u32,
        data: &[u8],
        end: bool,
        ok: bool,
        trailers: &[u8],
        out: &mut Vec<u8>,
    ) {
        // the end of a streamed answer whose stream is gone frees its slot
        if end && !self.zombies.is_empty() {
            self.zombies.remove(&seq);
        }
        if self.dead {
            return;
        }
        let Some(&id) = self.seqs.get(&seq) else {
            return;
        };
        let Some(s) = self.streams.get_mut(&id) else {
            return;
        };
        if s.sid.is_none() || s.done {
            return;
        }
        let n = data.len() as u64;
        if !ok || s.left.is_some_and(|l| n > l || (end && n < l)) {
            // the body is not whole: the client must see it is cut. The
            // handler is over when this is the end: no zombie for it.
            s.done |= end;
            self.reset(out, id, frame::INTERNAL_ERROR);
            return;
        }
        if let Some(l) = s.left.as_mut() {
            *l -= n;
        }
        s.push(data);
        self.unsent += data.len();
        s.done = end;
        if end {
            push_lines(trailers, &mut s.trailers);
        }
        if s.unsent() > 0 {
            if !s.turn {
                s.turn = true;
                self.turns.push_back(id);
            }
        } else if end {
            // all bytes are out: the trailers, or an empty DATA, end it
            if s.trailers.is_empty() {
                frame::write(out, frame::DATA, frame::END_STREAM, id, &[]);
            } else {
                frame::write_headers(out, id, &s.trailers, true, self.peer_frame);
            }
            // no DATA bytes (eg. a gRPC error with trailers only): no refund
            let paid = s.wrote;
            self.finish(out, id, paid);
        }
    }

    fn send_answer(
        &mut self,
        id: u32,
        status: u16,
        lines: &[u8],
        body: &[u8],
        date: &[u8],
        out: &mut Vec<u8>,
    ) {
        if self.dead {
            return;
        }
        let Some(head) = self.streams.get(&id).map(|s| s.head) else {
            return;
        };
        let status = final_status(status);
        let no_body = status == 204 || status == 304;
        // a HEAD answer with no body keeps the length that Dart set
        let len = (!no_body).then(|| {
            if head
                && body.is_empty()
                && let Some(Some(n)) = length_line(lines)
            {
                return n;
            }
            body.len() as u64
        });
        let silent = no_body || head || body.is_empty();
        // the block of `:status` and the lines: encoded again only when
        // they change
        if status != self.head_status || lines != &self.head_lines[..] {
            self.head_cached.clear();
            status_field(status, &mut self.head_cached);
            push_lines(lines, &mut self.head_cached);
            self.head_status = status;
            self.head_lines.clear();
            self.head_lines.extend_from_slice(lines);
        }
        let mut block = std::mem::take(&mut self.head_buf);
        block.clear();
        block.extend_from_slice(&self.head_cached);
        if let Some(n) = len {
            length_field(n, &mut block);
        }
        date_field(date, &mut block);
        frame::write_headers(out, id, &block, silent, self.peer_frame);
        self.head_buf = block;
        if silent {
            self.finish(out, id, false);
            return;
        }
        let n = body.len();
        let Some(s) = self.streams.get_mut(&id) else {
            return;
        };
        // the whole body fits the windows and one frame, and no other
        // stream waits: one DATA frame now, with no copy of the body
        if self.turns.is_empty()
            && s.trailers.is_empty()
            && s.sid.is_none()
            && n <= self.peer_frame.min(MAX_FRAME)
            && (n as i64) <= self.window
            && (n as i64) <= s.window
        {
            frame::write(out, frame::DATA, frame::END_STREAM, id, body);
            s.wrote = true;
            s.stalled = None;
            s.window -= n as i64;
            s.done = true;
            self.window -= n as i64;
            self.data_sent += n as u64;
            self.pings_rate.n = 0;
            self.finish(out, id, true);
            return;
        }
        s.body = body.to_vec();
        self.unsent += n;
        s.done = true;
        s.turn = true;
        self.turns.push_back(id);
    }

    /// Adds DATA frames to `out` while less than [HOLD] waits there and
    /// the windows allow. Streams take turns, one frame each. The bytes of
    /// a streamed answer get a mark: Dart hears of them (STREAM_SENT) once
    /// `written` passes it, as for HTTP/1.1.
    pub(crate) fn send(
        &mut self,
        out: &mut Vec<u8>,
        wpos: usize,
        written: u64,
        marks: &mut VecDeque<(u64, u64, u64)>,
    ) {
        if self.dead {
            return; // no frame after a GOAWAY with an error
        }
        let mut misses = 0;
        while out.len() - wpos < HOLD && self.window > 0 && misses < self.turns.len() {
            let id = self.turns.pop_front().unwrap();
            let Some(s) = self.streams.get_mut(&id) else {
                continue; // reset by the client
            };
            let k = s
                .unsent()
                // a big peer frame size must not break [HOLD]
                .min(self.peer_frame.min(MAX_FRAME))
                .min(self.window as usize)
                .min(s.window.max(0) as usize);
            if k == 0 {
                // no stream window: it waits for a WINDOW_UPDATE
                self.turns.push_back(id);
                misses += 1;
                continue;
            }
            misses = 0;
            let end = s.done && k == s.unsent();
            // with trailers, their HEADERS ends the stream
            let flags = if end && s.trailers.is_empty() {
                frame::END_STREAM
            } else {
                0
            };
            frame::write(out, frame::DATA, flags, id, &s.body[s.sent..s.sent + k]);
            s.sent += k;
            self.unsent -= k;
            s.wrote |= k > 0;
            s.stalled = None; // bytes moved: the stall clock starts again
            self.data_sent += k as u64;
            self.pings_rate.n = 0; // see MAX_PINGS
            s.window -= k as i64;
            self.window -= k as i64;
            if let Some(sid) = s.sid {
                let at = written + (out.len() - wpos) as u64;
                marks.push_back((at, sid, k as u64));
            }
            if end {
                s.turn = false;
                if !s.trailers.is_empty() {
                    frame::write_headers(out, id, &s.trailers, true, self.peer_frame);
                }
                self.finish(out, id, true);
            } else if s.unsent() > 0 {
                self.turns.push_back(id);
            } else {
                s.turn = false; // a streamed answer waits for Dart
            }
        }
    }

    /// True when [send] can add a frame now.
    pub(crate) fn can_send(&self) -> bool {
        !self.dead
            && self.window > 0
            && self
                .turns
                .iter()
                .any(|id| self.streams.get(id).is_some_and(|s| s.window > 0))
    }

    /// True when body bytes wait but a window of the client is 0. The
    /// connection then needs a timer (`SEND_WAIT`): no write waits that
    /// could start one.
    pub(crate) fn blocked(&self) -> bool {
        !self.dead && !self.turns.is_empty() && !self.can_send()
    }
}

/// A 1xx as the final answer is not valid in HTTP/2, and a status out of
/// range is not either: both go out as 500.
fn final_status(status: u16) -> u16 {
    if (200..=999).contains(&status) {
        status
    } else {
        500
    }
}

/// The header block of an answer: `:status`, Dart's lines (lowercase, no
/// connection-specific ones), `content-length` when `len` is known, and
/// `date`.
fn head_block(status: u16, lines: &[u8], len: Option<u64>, date: &[u8]) -> Vec<u8> {
    let mut block = Vec::with_capacity(64 + lines.len());
    status_field(status, &mut block);
    push_lines(lines, &mut block);
    if let Some(n) = len {
        encode_field(b"content-length", n.to_string().as_bytes(), &mut block);
    }
    date_field(date, &mut block);
    block
}

/// Encodes Dart's header lines into `block`: names lowercase, no
/// connection-specific ones, no content-length (Rust sets it). A line with
/// no name (eg. a pseudo header `:x`) is dropped.
fn push_lines(lines: &[u8], block: &mut Vec<u8>) {
    for line in lines.split(|&c| c == b'\n') {
        let Some(at) = line.iter().position(|&c| c == b':') else {
            continue;
        };
        let name = line[..at].trim_ascii().to_ascii_lowercase();
        if matches!(
            &name[..],
            b"" | b"connection"
                | b"keep-alive"
                | b"proxy-connection"
                | b"transfer-encoding"
                | b"upgrade"
                | b"content-length"
        ) {
            continue;
        }
        encode_field(&name, line[at + 1..].trim_ascii(), block);
    }
}

/// Streamed body: gives the acked bytes back to the client (WINDOW_UPDATE)
/// once they are [ACK_BATCH] or more.
fn give_back(out: &mut Vec<u8>, id: u32, r: &mut Recv) {
    if r.acked >= ACK_BATCH {
        frame::write_window(out, id, r.acked as u32);
        r.window += r.acked as i64;
        r.acked = 0;
    }
}

/// `content-length: n`: a literal with the name of static entry 28, and
/// the digits with no `String` in between.
fn length_field(n: u64, out: &mut Vec<u8>) {
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    let mut v = n;
    loop {
        at -= 1;
        digits[at] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    hpack::write_int(28, 4, 0x00, out);
    hpack::write_str(&digits[at..], out);
}

fn status_field(status: u16, out: &mut Vec<u8>) {
    encode_field(b":status", status.to_string().as_bytes(), out);
}

/// `date` is the `date: ...\r\n` line of HTTP/1.1.
fn date_field(date: &[u8], out: &mut Vec<u8>) {
    if let Some(v) = date.strip_prefix(b"date: ") {
        encode_field(b"date", v.trim_ascii_end(), out);
    }
}

/// What the checks of a request found.
#[derive(Debug, PartialEq)]
enum Check {
    Ok,
    /// malformed (RFC 9113 8.1.1): RST_STREAM PROTOCOL_ERROR
    Bad,
    /// answer with this status (eg. 405 for CONNECT)
    Status(u16),
}

/// A request from its decoded header fields (RFC 9113 8.3).
#[derive(Default)]
struct Req {
    method: Vec<u8>,
    path: Vec<u8>,
    authority: Option<Vec<u8>>,
    scheme: bool,
    /// the headers in record format: u16 name length, u16 value length,
    /// name, value
    headers: Vec<u8>,
    count: usize,
    host: bool,
    cookie: Vec<u8>,
    /// the content-length
    length: Option<u64>,
    /// a multipart form: the body gets `maxMultipartSize`
    form: bool,
    /// a gRPC request: the body streams to Dart
    grpc: bool,
    /// a regular header came: no pseudo header may follow
    regular: bool,
    bad: bool,
}

impl Req {
    fn field(&mut self, name: &[u8], value: &[u8]) {
        if self.bad {
            return;
        }
        if let Some(pseudo) = name.strip_prefix(b":") {
            // each one once, not empty, and before the regular headers.
            // An unknown one (eg. `:status`) is malformed too.
            let ok = !self.regular
                && !value.is_empty()
                && match pseudo {
                    b"method" if self.method.is_empty() => {
                        self.method = value.to_vec();
                        true
                    }
                    b"path" if self.path.is_empty() => {
                        self.path = value.to_vec();
                        true
                    }
                    b"scheme" if !self.scheme => {
                        self.scheme = true;
                        true
                    }
                    b"authority" if self.authority.is_none() => {
                        self.authority = Some(value.to_vec());
                        value.iter().all(|&c| c > b' ' && c < 0x7f)
                    }
                    _ => false,
                };
            self.bad |= !ok;
            return;
        }
        self.regular = true;
        // names are lowercase tokens, values have no line breaks, no NUL
        // and no white space at the ends (RFC 9113 8.2.1)
        let name_ok = !name.is_empty()
            && name.iter().all(|&c| {
                c.is_ascii_lowercase() || c.is_ascii_digit() || b"!#$%&'*+-.^_`|~".contains(&c)
            });
        let value_ok = !value.iter().any(|&c| c == 0 || c == b'\r' || c == b'\n')
            && !value.first().is_some_and(|&c| c == b' ' || c == b'\t')
            && !value.last().is_some_and(|&c| c == b' ' || c == b'\t');
        let conn_specific = matches!(
            name,
            b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding" | b"upgrade"
        );
        if !name_ok || !value_ok || conn_specific || (name == b"te" && value != b"trailers") {
            self.bad = true;
            return;
        }
        match name {
            // many cookie fields are one header for Dart (RFC 9113 8.2.3)
            b"cookie" => {
                if !self.cookie.is_empty() {
                    self.cookie.extend_from_slice(b"; ");
                }
                self.cookie.extend_from_slice(value);
                return;
            }
            b"host" => self.host = true,
            // a number, and the same in each copy (RFC 9110 8.6)
            b"content-length" => match parse_len(value) {
                Some(n) if self.length.is_none_or(|l| l == n) => self.length = Some(n),
                _ => {
                    self.bad = true;
                    return;
                }
            },
            // the last content-type counts, as on HTTP/1.1
            b"content-type" => {
                let mime = value.split(|&c| c == b';').next().unwrap_or(b"");
                self.form = mime
                    .trim_ascii()
                    .eq_ignore_ascii_case(b"multipart/form-data");
                // `application/grpc`, then the end, `+` or `;` (not
                // `application/grpc-web` or `application/grpcfoo`)
                self.grpc = value.get(..GRPC.len()).is_some_and(|p| {
                    p.eq_ignore_ascii_case(GRPC)
                        && matches!(value.get(GRPC.len()), None | Some(b'+' | b';'))
                });
            }
            _ => {}
        }
        self.push(name, value);
    }

    fn push(&mut self, name: &[u8], value: &[u8]) {
        self.count += 1;
        let h = &mut self.headers;
        h.extend_from_slice(&(name.len() as u16).to_le_bytes());
        h.extend_from_slice(&(value.len() as u16).to_le_bytes());
        h.extend_from_slice(name);
        h.extend_from_slice(value);
    }

    fn check(&self, end_stream: bool) -> Check {
        if self.bad || self.method.is_empty() {
            return Check::Bad;
        }
        if self.method == b"CONNECT" {
            return Check::Status(405);
        }
        // origin form, or `*` for OPTIONS (RFC 9113 8.3.1)
        let path_ok = self.path.iter().all(|&c| c > b' ' && c != 0x7f)
            && (self.path.starts_with(b"/") || (self.path == b"*" && self.method == b"OPTIONS"));
        if !self.scheme || !path_ok {
            return Check::Bad;
        }
        if !self.method.iter().all(|c| c.is_ascii_graphic()) {
            return Check::Bad;
        }
        // a body length but no body
        if end_stream && self.length.is_some_and(|l| l > 0) {
            return Check::Bad;
        }
        if self.count + usize::from(!self.cookie.is_empty()) > MAX_HEADERS {
            return Check::Status(431);
        }
        Check::Ok
    }

    /// The request record for Dart (see lib.rs), with `flags` (bit 3 and
    /// maybe bit 4).
    fn record(mut self, id: u64, peer: &[u8; 18], flags: u32) -> Vec<u8> {
        if !self.cookie.is_empty() {
            let cookie = std::mem::take(&mut self.cookie);
            self.push(b"cookie", &cookie);
        }
        // `:authority` is the host (RFC 9113 8.3.1)
        if !self.host
            && let Some(a) = self.authority.take()
        {
            self.push(b"host", &a);
        }
        let escaped;
        let target = if self.path.is_ascii() {
            &self.path[..]
        } else {
            escaped = escape_target(&self.path);
            &escaped[..]
        };
        let (m, t, h) = (&self.method, target, &self.headers);
        let mut rec = vec![0u8; REQ_HEAD];
        write_head(
            &mut rec,
            id,
            flags,
            m.len(),
            t.len(),
            h.len(),
            0,
            [peer[16], peer[17]],
            &peer[..16],
        );
        rec.extend_from_slice(m);
        rec.extend_from_slice(t);
        rec.extend_from_slice(h);
        rec
    }
}

#[cfg(test)]
mod tests;
