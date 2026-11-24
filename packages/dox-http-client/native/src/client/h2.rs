//! HTTP/2 (RFC 9113) for the client. One [`H2`] has the state of one
//! connection. It has no socket: `recv` takes the bytes of the server,
//! `out` gets the bytes for the server and `events` says what came.
//!
//! - A stream is one request. The client opens it with `open` and gives
//!   the request body with `send`.
//! - Flow control for the answers: the server may send [`WINDOW`] bytes of
//!   a stream. `release` gives it room for more. The room of the whole
//!   connection is given back when the bytes come, so one slow stream
//!   does not stop the others.
//! - Flow control for the requests: `flush` frames only the bytes that
//!   the windows of the server permit.
//! - The server can not push (SETTINGS_ENABLE_PUSH is 0).
//!
//! ponytail: no check of `content-length` against the DATA bytes, and no
//! PING of our own to find a dead connection. Add them when a server shows
//! the need.
pub(super) mod hpack;
mod huffman;

use std::collections::{HashMap, VecDeque};

use super::{E_BAD, E_CLOSED, HIGH, MAX_HEAD};

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

const DATA: u8 = 0;
const HEADERS: u8 = 1;
const PRIORITY: u8 = 2;
const RST_STREAM: u8 = 3;
const SETTINGS: u8 = 4;
const PUSH_PROMISE: u8 = 5;
const PING: u8 = 6;
const GOAWAY: u8 = 7;
const WINDOW_UPDATE: u8 = 8;
const CONTINUATION: u8 = 9;

const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;
const HAS_PRIORITY: u8 = 0x20;

const PROTOCOL_ERROR: u32 = 1;
const FLOW_CONTROL_ERROR: u32 = 3;
const FRAME_SIZE_ERROR: u32 = 6;
const REFUSED_STREAM: u32 = 7;
pub(super) const CANCEL: u32 = 8;
const COMPRESSION_ERROR: u32 = 9;
const ENHANCE_YOUR_CALM: u32 = 11;

/// Bytes of one answer body that the server may send before `release`.
pub(super) const WINDOW: u32 = HIGH as u32;
/// The same for all streams of the connection together.
const CONN_WINDOW: u32 = 16 << 20;
/// The window of a connection and of a stream before a SETTINGS frame.
const DEFAULT_WINDOW: i64 = 65_535;
const MAX_WINDOW: i64 = 0x7fff_ffff;
/// The biggest frame payload we take and send (the default of HTTP/2).
const FRAME: usize = 16_384;
/// No more request body is framed while `out` has this many bytes.
const OUT_HIGH: usize = 256 * 1024;
/// A server that makes us answer (PING, SETTINGS) and does not read: the
/// connection closes when the answers in `out` are over this.
const OUT_MAX: usize = 4 << 20;
/// Most open streams before the SETTINGS frame of the server came.
const EARLY_STREAMS: u32 = 100;
/// Most bytes of one header block (HEADERS and its CONTINUATION frames).
const MAX_BLOCK: usize = 2 * MAX_HEAD;

/// What came from the server.
#[derive(Debug, PartialEq)]
pub(super) enum Event {
    /// The head of the answer. The lines are `name:value\n`.
    Head {
        stream: u32,
        status: u16,
        lines: Vec<u8>,
    },
    /// Bytes of the answer body.
    Data { stream: u32, bytes: Vec<u8> },
    /// The answer is whole. The stream is gone.
    End { stream: u32 },
    /// The stream failed and is gone. `retry`: the server did not work on
    /// the request, so it is safe to send it again.
    Reset {
        stream: u32,
        error: u16,
        retry: bool,
    },
    /// This many bytes of the request body are in frames, in all.
    Wrote { stream: u32, total: u64 },
}

struct Stream {
    /// bytes of the request body that the server still takes
    send_window: i64,
    /// bytes of the answer body that the server may still send
    recv_window: i64,
    /// bytes that came and are not given back to the server as room yet
    unacked: u32,
    /// the request body that is not in frames yet, from `pos` on
    body: Vec<u8>,
    pos: usize,
    /// the caller gave the whole body
    end: bool,
    /// our side of the stream is closed (END_STREAM went out)
    sent_end: bool,
    /// the caller stopped the body: no more of it goes out
    aborted: bool,
    /// it is in `sendq` or in `stalled`
    queued: bool,
    wrote: u64,
    got_head: bool,
}

pub(super) struct H2 {
    /// Bytes for the server. The caller writes them and removes them.
    pub(super) out: Vec<u8>,
    pub(super) events: Vec<Event>,
    streams: HashMap<u32, Stream>,
    /// streams with body bytes to frame
    sendq: VecDeque<u32>,
    /// streams whose body waits for a window of the server
    stalled: Vec<u32>,
    next_id: u32,
    decoder: hpack::Decoder,
    got_settings: bool,
    /// SETTINGS_MAX_CONCURRENT_STREAMS of the server
    peer_max: u32,
    /// SETTINGS_INITIAL_WINDOW_SIZE of the server
    peer_window: i64,
    send_window: i64,
    recv_window: i64,
    unacked: u32,
    /// The header block that is being read, its stream (0: none) and if
    /// its HEADERS frame had END_STREAM.
    block: Vec<u8>,
    block_stream: u32,
    block_end: bool,
    /// The server sent GOAWAY: no new streams.
    pub(super) goaway: bool,
    /// A connection error: the caller closes the connection.
    dead: bool,
    /// Bytes in `out` that answer frames of the server (ACK,
    /// WINDOW_UPDATE, RST_STREAM), since `out` was empty.
    replies: usize,
}

pub(super) fn frame(out: &mut Vec<u8>, ty: u8, flags: u8, stream: u32, payload: &[u8]) {
    let n = payload.len();
    out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8, n as u8, ty, flags]);
    out.extend_from_slice(&stream.to_be_bytes());
    out.extend_from_slice(payload);
}

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// The payload without its padding.
fn unpad(flags: u8, payload: &[u8]) -> Result<&[u8], u32> {
    if flags & PADDED == 0 {
        return Ok(payload);
    }
    let (&pad, rest) = payload.split_first().ok_or(FRAME_SIZE_ERROR)?;
    if pad as usize > rest.len() {
        return Err(PROTOCOL_ERROR);
    }
    Ok(&rest[..rest.len() - pad as usize])
}

impl H2 {
    pub(super) fn new() -> H2 {
        let mut out = PREFACE.to_vec();
        let mut settings = Vec::with_capacity(18);
        // ENABLE_PUSH, INITIAL_WINDOW_SIZE, MAX_HEADER_LIST_SIZE
        for (id, value) in [(2u16, 0u32), (4, WINDOW), (6, MAX_HEAD as u32)] {
            settings.extend_from_slice(&id.to_be_bytes());
            settings.extend_from_slice(&value.to_be_bytes());
        }
        frame(&mut out, SETTINGS, 0, 0, &settings);
        let more = CONN_WINDOW - DEFAULT_WINDOW as u32;
        frame(&mut out, WINDOW_UPDATE, 0, 0, &more.to_be_bytes());
        H2 {
            out,
            events: Vec::new(),
            streams: HashMap::new(),
            sendq: VecDeque::new(),
            stalled: Vec::new(),
            next_id: 1,
            decoder: hpack::Decoder::new(4096),
            got_settings: false,
            peer_max: u32::MAX,
            peer_window: DEFAULT_WINDOW,
            send_window: DEFAULT_WINDOW,
            recv_window: CONN_WINDOW as i64,
            unacked: 0,
            block: Vec::new(),
            block_stream: 0,
            block_end: false,
            goaway: false,
            dead: false,
            replies: 0,
        }
    }

    /// True when `open` may be called.
    pub(super) fn capacity(&self) -> bool {
        // before its SETTINGS the limit of the server is not known: not
        // too many streams that it may then refuse
        let max = if self.got_settings {
            self.peer_max
        } else {
            self.peer_max.min(EARLY_STREAMS)
        };
        !self.goaway
            && !self.dead
            && self.streams.len() < max as usize
            && self.next_id < 0x7fff_fff0
    }

    /// The streams that are open.
    #[cfg(test)]
    pub(super) fn active(&self) -> usize {
        self.streams.len()
    }

    /// Opens a stream with the request head `fields` (the pseudo fields
    /// first). `end`: the request has no body. Returns the stream.
    pub(super) fn open(&mut self, fields: &[(&[u8], &[u8])], end: bool) -> u32 {
        let id = self.next_id;
        self.next_id += 2;
        let mut block = Vec::new();
        for (name, value) in fields {
            hpack::encode_field(name, value, &mut block);
        }
        let mut parts = block.chunks(FRAME).peekable();
        let first = parts.next().unwrap_or(&[]);
        let mut flags = if end { END_STREAM } else { 0 };
        if parts.peek().is_none() {
            flags |= END_HEADERS;
        }
        frame(&mut self.out, HEADERS, flags, id, first);
        while let Some(part) = parts.next() {
            let flags = if parts.peek().is_none() {
                END_HEADERS
            } else {
                0
            };
            frame(&mut self.out, CONTINUATION, flags, id, part);
        }
        self.streams.insert(
            id,
            Stream {
                send_window: self.peer_window,
                recv_window: WINDOW as i64,
                unacked: 0,
                body: Vec::new(),
                pos: 0,
                end,
                sent_end: end,
                aborted: false,
                queued: false,
                wrote: 0,
                got_head: false,
            },
        );
        id
    }

    /// More bytes of the request body of `stream`. `end`: they are the last.
    pub(super) fn send(&mut self, stream: u32, data: &[u8], end: bool) {
        let Some(st) = self.streams.get_mut(&stream) else {
            return;
        };
        if st.end || st.aborted {
            return;
        }
        st.body.extend_from_slice(data);
        st.end = end;
        if !st.queued {
            st.queued = true;
            self.sendq.push_back(stream);
        }
        self.flush();
    }

    /// The caller has no more body for `stream` and the body is not whole.
    /// The stream stays open for the answer.
    pub(super) fn abort_send(&mut self, stream: u32) {
        if let Some(st) = self.streams.get_mut(&stream) {
            st.aborted = true;
            st.body.clear();
            st.pos = 0;
        }
    }

    /// Closes `stream` with RST_STREAM. No event comes for it.
    pub(super) fn reset(&mut self, stream: u32, code: u32) {
        if self.streams.remove(&stream).is_some() {
            frame(&mut self.out, RST_STREAM, 0, stream, &code.to_be_bytes());
        }
    }

    /// The caller used `n` bytes of the answer body of `stream`: the server
    /// may send that many more.
    pub(super) fn release(&mut self, stream: u32, n: u64) {
        let Some(st) = self.streams.get_mut(&stream) else {
            return;
        };
        st.unacked = st.unacked.saturating_add(n.min(u32::MAX as u64) as u32);
        // not a frame for each small part
        if st.unacked >= WINDOW / 2 {
            let more = st.unacked.min(MAX_WINDOW as u32);
            frame(&mut self.out, WINDOW_UPDATE, 0, stream, &more.to_be_bytes());
            st.recv_window += more as i64;
            st.unacked = 0;
        }
    }

    /// Puts the request bodies in DATA frames, as far as the windows of
    /// the server and [`OUT_HIGH`] permit. Call it again when `out` is
    /// written.
    pub(super) fn flush(&mut self) {
        while self.out.len() < OUT_HIGH {
            let Some(id) = self.sendq.pop_front() else {
                break;
            };
            let Some(st) = self.streams.get_mut(&id) else {
                continue;
            };
            st.queued = false;
            let before = st.wrote;
            loop {
                let left = st.body.len() - st.pos;
                if left == 0 {
                    if st.end && !st.sent_end {
                        frame(&mut self.out, DATA, END_STREAM, id, &[]);
                        st.sent_end = true;
                    }
                    st.body.clear();
                    st.pos = 0;
                    break;
                }
                let window = st.send_window.min(self.send_window).max(0) as usize;
                let n = left.min(FRAME).min(window);
                if n == 0 {
                    // a WINDOW_UPDATE or a SETTINGS frame makes room
                    st.queued = true;
                    self.stalled.push(id);
                    break;
                }
                let last = st.end && n == left;
                let flags = if last { END_STREAM } else { 0 };
                frame(&mut self.out, DATA, flags, id, &st.body[st.pos..st.pos + n]);
                st.pos += n;
                st.send_window -= n as i64;
                self.send_window -= n as i64;
                st.wrote += n as u64;
                if last {
                    st.sent_end = true;
                    st.body.clear();
                    st.pos = 0;
                    break;
                }
                if self.out.len() >= OUT_HIGH {
                    st.queued = true;
                    self.sendq.push_front(id);
                    break;
                }
            }
            if st.pos > OUT_HIGH {
                st.body.drain(..st.pos);
                st.pos = 0;
            }
            if st.wrote > before {
                self.events.push(Event::Wrote {
                    stream: id,
                    total: st.wrote,
                });
            }
            if st.queued && self.out.len() >= OUT_HIGH {
                break;
            }
        }
    }

    /// The request body bytes of `stream` that are not in frames yet.
    #[cfg(test)]
    pub(super) fn waiting(&self, stream: u32) -> usize {
        self.streams
            .get(&stream)
            .map_or(0, |st| st.body.len() - st.pos)
    }

    /// Takes bytes of the server. Returns how many it used: the others are
    /// a frame that is not whole yet. Err: a connection error. `out` then
    /// has a GOAWAY, and the caller closes the connection.
    pub(super) fn recv(&mut self, input: &[u8]) -> Result<usize, ()> {
        if self.dead {
            return Err(());
        }
        // the socket took all that was there
        if self.out.is_empty() {
            self.replies = 0;
        }
        let mut p = 0;
        while input.len() - p >= 9 {
            let len =
                (input[p] as usize) << 16 | (input[p + 1] as usize) << 8 | input[p + 2] as usize;
            if len > FRAME {
                return self.die(FRAME_SIZE_ERROR);
            }
            if input.len() - p - 9 < len {
                break;
            }
            let ty = input[p + 3];
            let flags = input[p + 4];
            let stream = be32(&input[p + 5..p + 9]) & 0x7fff_ffff;
            let payload = &input[p + 9..p + 9 + len];
            p += 9 + len;
            let before = self.out.len();
            if let Err(code) = self.on_frame(ty, flags, stream, payload) {
                return self.die(code);
            }
            // only the bytes that the server made us write count
            self.replies += self.out.len() - before;
            if self.replies > OUT_MAX {
                return self.die(ENHANCE_YOUR_CALM);
            }
        }
        self.flush();
        Ok(p)
    }

    fn die(&mut self, code: u32) -> Result<usize, ()> {
        self.dead = true;
        let mut payload = [0u8; 8];
        payload[4..].copy_from_slice(&code.to_be_bytes());
        frame(&mut self.out, GOAWAY, 0, 0, &payload);
        Err(())
    }

    /// True for a stream that we opened and that is closed. Frames for it
    /// may still come. A frame for a stream that was never open is a
    /// connection error.
    fn was_open(&self, stream: u32) -> bool {
        stream % 2 == 1 && stream < self.next_id
    }

    /// A stream error: the stream closes, the connection stays.
    fn stream_error(&mut self, stream: u32, code: u32) {
        if self.streams.remove(&stream).is_some() {
            frame(&mut self.out, RST_STREAM, 0, stream, &code.to_be_bytes());
            self.events.push(Event::Reset {
                stream,
                error: E_BAD,
                retry: false,
            });
        }
    }

    /// The server ended its side of `stream`.
    fn remote_end(&mut self, stream: u32) {
        if let Some(st) = self.streams.remove(&stream) {
            // the answer came before the whole request: the rest of the
            // request is of no use
            if !st.sent_end {
                frame(&mut self.out, RST_STREAM, 0, stream, &CANCEL.to_be_bytes());
            }
            self.events.push(Event::End { stream });
        }
    }

    fn unstall(&mut self) {
        self.sendq.extend(self.stalled.drain(..));
    }

    fn on_frame(&mut self, ty: u8, flags: u8, stream: u32, payload: &[u8]) -> Result<(), u32> {
        // the first frame of the server is its SETTINGS
        if !self.got_settings && (ty != SETTINGS || flags & ACK != 0) {
            return Err(PROTOCOL_ERROR);
        }
        // nothing may come between the frames of a header block
        if self.block_stream != 0 && (ty != CONTINUATION || stream != self.block_stream) {
            return Err(PROTOCOL_ERROR);
        }
        match ty {
            DATA => {
                if stream == 0 {
                    return Err(PROTOCOL_ERROR);
                }
                // the padding counts for flow control too
                let n = payload.len() as i64;
                if n > self.recv_window {
                    return Err(FLOW_CONTROL_ERROR);
                }
                self.recv_window -= n;
                self.unacked += n as u32;
                if self.unacked >= CONN_WINDOW / 2 {
                    frame(
                        &mut self.out,
                        WINDOW_UPDATE,
                        0,
                        0,
                        &self.unacked.to_be_bytes(),
                    );
                    self.recv_window += self.unacked as i64;
                    self.unacked = 0;
                }
                let data = unpad(flags, payload)?;
                let Some(st) = self.streams.get_mut(&stream) else {
                    return if self.was_open(stream) {
                        Ok(())
                    } else {
                        Err(PROTOCOL_ERROR)
                    };
                };
                if !st.got_head {
                    self.stream_error(stream, PROTOCOL_ERROR);
                    return Ok(());
                }
                if n > st.recv_window {
                    self.stream_error(stream, FLOW_CONTROL_ERROR);
                    return Ok(());
                }
                st.recv_window -= n;
                if !data.is_empty() {
                    self.events.push(Event::Data {
                        stream,
                        bytes: data.to_vec(),
                    });
                }
                if flags & END_STREAM != 0 {
                    self.remote_end(stream);
                } else {
                    // nobody reads the padding: its room is free at once
                    self.release(stream, (payload.len() - data.len()) as u64);
                }
            }
            HEADERS => {
                if stream == 0 {
                    return Err(PROTOCOL_ERROR);
                }
                let mut data = unpad(flags, payload)?;
                if flags & HAS_PRIORITY != 0 {
                    data = data.get(5..).ok_or(FRAME_SIZE_ERROR)?;
                }
                self.block.clear();
                self.block.extend_from_slice(data);
                self.block_end = flags & END_STREAM != 0;
                if flags & END_HEADERS != 0 {
                    self.headers(stream)?;
                } else {
                    self.block_stream = stream;
                }
            }
            CONTINUATION => {
                if self.block_stream == 0 {
                    return Err(PROTOCOL_ERROR);
                }
                if self.block.len() + payload.len() > MAX_BLOCK {
                    return Err(ENHANCE_YOUR_CALM);
                }
                self.block.extend_from_slice(payload);
                if flags & END_HEADERS != 0 {
                    self.block_stream = 0;
                    self.headers(stream)?;
                }
            }
            PRIORITY => {
                if stream == 0 {
                    return Err(PROTOCOL_ERROR);
                }
                if payload.len() != 5 {
                    return Err(FRAME_SIZE_ERROR);
                }
            }
            RST_STREAM => {
                if stream == 0 {
                    return Err(PROTOCOL_ERROR);
                }
                if payload.len() != 4 {
                    return Err(FRAME_SIZE_ERROR);
                }
                if self.streams.remove(&stream).is_some() {
                    self.events.push(Event::Reset {
                        stream,
                        error: E_CLOSED,
                        retry: be32(payload) == REFUSED_STREAM,
                    });
                } else if !self.was_open(stream) {
                    return Err(PROTOCOL_ERROR);
                }
            }
            SETTINGS => {
                if stream != 0 {
                    return Err(PROTOCOL_ERROR);
                }
                if flags & ACK != 0 {
                    return if payload.is_empty() {
                        Ok(())
                    } else {
                        Err(FRAME_SIZE_ERROR)
                    };
                }
                if !payload.len().is_multiple_of(6) {
                    return Err(FRAME_SIZE_ERROR);
                }
                for setting in payload.as_chunks::<6>().0 {
                    let value = be32(&setting[2..]);
                    match u16::from_be_bytes([setting[0], setting[1]]) {
                        3 => self.peer_max = value,
                        4 => {
                            if value as i64 > MAX_WINDOW {
                                return Err(FLOW_CONTROL_ERROR);
                            }
                            // the open streams get the change too
                            let delta = value as i64 - self.peer_window;
                            self.peer_window = value as i64;
                            for st in self.streams.values_mut() {
                                st.send_window += delta;
                                if st.send_window > MAX_WINDOW {
                                    return Err(FLOW_CONTROL_ERROR);
                                }
                            }
                        }
                        5 if !(FRAME as u32..=0xff_ffff).contains(&value) => {
                            return Err(PROTOCOL_ERROR);
                        }
                        // 1 (the header table): our encoder has no table
                        _ => {}
                    }
                }
                self.got_settings = true;
                frame(&mut self.out, SETTINGS, ACK, 0, &[]);
                self.unstall();
            }
            // we said that we take no push
            PUSH_PROMISE => return Err(PROTOCOL_ERROR),
            PING => {
                if stream != 0 {
                    return Err(PROTOCOL_ERROR);
                }
                if payload.len() != 8 {
                    return Err(FRAME_SIZE_ERROR);
                }
                if flags & ACK == 0 {
                    frame(&mut self.out, PING, ACK, 0, payload);
                }
            }
            GOAWAY => {
                if stream != 0 {
                    return Err(PROTOCOL_ERROR);
                }
                if payload.len() < 8 {
                    return Err(FRAME_SIZE_ERROR);
                }
                self.goaway = true;
                // the server did not work on the streams after `last`
                let last = be32(payload) & 0x7fff_ffff;
                let mut late: Vec<u32> = self
                    .streams
                    .keys()
                    .copied()
                    .filter(|&id| id > last)
                    .collect();
                late.sort_unstable();
                for id in late {
                    self.streams.remove(&id);
                    self.events.push(Event::Reset {
                        stream: id,
                        error: E_CLOSED,
                        retry: true,
                    });
                }
            }
            WINDOW_UPDATE => {
                if payload.len() != 4 {
                    return Err(FRAME_SIZE_ERROR);
                }
                let more = (be32(payload) & 0x7fff_ffff) as i64;
                if stream == 0 {
                    if more == 0 {
                        return Err(PROTOCOL_ERROR);
                    }
                    self.send_window += more;
                    if self.send_window > MAX_WINDOW {
                        return Err(FLOW_CONTROL_ERROR);
                    }
                } else if let Some(st) = self.streams.get_mut(&stream) {
                    st.send_window += more;
                    if more == 0 {
                        self.stream_error(stream, PROTOCOL_ERROR);
                    } else if st.send_window > MAX_WINDOW {
                        self.stream_error(stream, FLOW_CONTROL_ERROR);
                    }
                } else if !self.was_open(stream) {
                    return Err(PROTOCOL_ERROR);
                }
                self.unstall();
            }
            // a frame type that we do not know is dropped
            _ => {}
        }
        Ok(())
    }

    /// The header block in `block` is whole: decodes it. A block for a
    /// stream that is gone is decoded too, because the header table of the
    /// connection needs each block.
    fn headers(&mut self, stream: u32) -> Result<(), u32> {
        let block = std::mem::take(&mut self.block);
        let end = self.block_end;
        let mut status: Option<u16> = None;
        let mut bad = false;
        let mut fields = false;
        let mut lines = Vec::new();
        let decoded = self.decoder.decode(&block, MAX_HEAD, |name, value| {
            if name.first() == Some(&b':') {
                // `:status` is the only pseudo field of an answer, and it
                // comes before the other fields
                let digits = value.len() == 3 && value.iter().all(u8::is_ascii_digit);
                if name != b":status" || status.is_some() || fields || !digits {
                    bad = true;
                    return;
                }
                let n = value.iter().fold(0u16, |n, &c| n * 10 + (c - b'0') as u16);
                status = Some(n);
                return;
            }
            fields = true;
            let name_ok = !name.is_empty()
                && name
                    .iter()
                    .all(|&c| c > b' ' && c < 0x7f && c != b':' && !c.is_ascii_uppercase());
            let value_ok = !value.iter().any(|&c| c == b'\n' || c == b'\r' || c == 0);
            if !name_ok || !value_ok {
                bad = true;
                return;
            }
            lines.extend_from_slice(name);
            lines.push(b':');
            lines.extend_from_slice(value.trim_ascii());
            lines.push(b'\n');
        });
        // keep the memory of the block for the next one
        self.block = block;
        self.block.clear();
        let decoded = decoded.map_err(|_| COMPRESSION_ERROR)?;
        let Some(st) = self.streams.get_mut(&stream) else {
            return if self.was_open(stream) {
                Ok(())
            } else {
                Err(PROTOCOL_ERROR)
            };
        };
        if st.got_head {
            // fields after the body (trailers): they end the answer
            if end {
                self.remote_end(stream);
            } else {
                self.stream_error(stream, PROTOCOL_ERROR);
            }
            return Ok(());
        }
        let whole = decoded == hpack::Decoded::Whole && !bad;
        let status = match status {
            // 101 is not a part of HTTP/2
            Some(status) if whole && status >= 100 && status != 101 => status,
            _ => {
                self.stream_error(stream, PROTOCOL_ERROR);
                return Ok(());
            }
        };
        if status < 200 {
            // eg. 100 Continue: the head of the answer comes after it
            if end {
                self.stream_error(stream, PROTOCOL_ERROR);
            }
            return Ok(());
        }
        st.got_head = true;
        self.events.push(Event::Head {
            stream,
            status,
            lines,
        });
        if end {
            self.remote_end(stream);
        }
        Ok(())
    }
}

/// For the fuzz target: random bytes as the frames of a server, to a
/// connection with open streams. No panic, and the same events when the
/// bytes come one by one.
#[cfg(fuzzing)]
pub(super) fn fuzz(data: &[u8]) {
    hpack::fuzz(data);
    let start = || {
        let mut h = H2::new();
        for end in [true, false, false] {
            let stream = h.open(&[(b":method", b"GET"), (b":path", b"/")], end);
            h.send(stream, &[7u8; 100_000], stream == 5);
        }
        h
    };
    let mut whole = start();
    let all = whole.recv(data);
    let mut parts = start();
    let mut buf: Vec<u8> = Vec::new();
    let mut split = Ok(0);
    for &byte in data {
        buf.push(byte);
        split = parts.recv(&buf);
        match split {
            Ok(used) => {
                buf.drain(..used);
            }
            Err(()) => break,
        }
    }
    assert_eq!(all.is_err(), split.is_err());
    // a frame may be cut in other places, so the DATA parts can differ:
    // the bytes of each stream must be the same
    let body = |h: &H2| -> Vec<(u32, u8)> {
        let mut list = Vec::new();
        for event in &h.events {
            match event {
                Event::Data { stream, bytes } => list.extend(bytes.iter().map(|&b| (*stream, b))),
                Event::Head { stream, .. } => list.push((*stream, 1)),
                Event::End { stream } => list.push((*stream, 2)),
                Event::Reset { stream, .. } => list.push((*stream, 3)),
                Event::Wrote { .. } => {}
            }
        }
        list
    };
    if all.is_ok() {
        assert_eq!(body(&whole), body(&parts));
    }
    assert!(whole.out.len() <= OUT_MAX + 2 * OUT_HIGH + FRAME);
}
