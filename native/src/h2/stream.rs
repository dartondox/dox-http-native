//! One HTTP/2 stream on the server side.
//!
//! A stream is kept from its HEADERS until its answer is all sent. While
//! `recv` is set, the request body comes in ("open" in RFC 9113 5.1).
//! After END_STREAM it is "half-closed (remote)": Dart has the request
//! and the answer is due or goes out.

use std::time::Instant;

/// A request body that comes in DATA frames. Dart gets the request once
/// it is whole (buffered mode).
pub(crate) struct Recv {
    /// the record head, method, target and headers (id set, no body)
    pub(crate) rec: Vec<u8>,
    pub(crate) data: Vec<u8>,
    /// most body bytes (`maxBodySize` or `maxMultipartSize`)
    pub(crate) max: usize,
    /// the content-length of the request, if it has one
    pub(crate) length: Option<u64>,
    /// our receive window of this stream
    pub(crate) window: i64,
    /// when the last DATA came (for the body timeout)
    pub(crate) last: Instant,
}

pub(crate) struct Stream {
    /// the request sequence number Dart knows (low 32 bits of the id)
    pub(crate) seq: u32,
    /// the send window of the client for this stream. It can go below 0
    /// when the client lowers SETTINGS_INITIAL_WINDOW_SIZE.
    pub(crate) window: i64,
    /// the request is HEAD: the answer has no body
    pub(crate) head: bool,
    /// the body still comes: Dart does not know the request yet
    pub(crate) recv: Option<Box<Recv>>,
    /// Dart answered (maybe a gzip job still runs)
    pub(crate) answered: bool,
    /// a gzip job runs: the status and the header lines wait for it
    pub(crate) zip: Option<(u16, Vec<u8>)>,
    /// a streamed answer (kind 6): Dart's stream id
    pub(crate) sid: Option<u64>,
    /// streamed: body bytes Dart may still send (its content-length)
    pub(crate) left: Option<u64>,
    /// no more body bytes come from Dart (always true for kind 0)
    pub(crate) done: bool,
    /// the stream is in the turns of [H2::send](super::H2::send)
    pub(crate) turn: bool,
    /// answer body bytes, `sent..` is not sent yet
    pub(crate) body: Vec<u8>,
    pub(crate) sent: usize,
    /// since when body bytes wait for a stream window of 0 or less
    pub(crate) stalled: Option<Instant>,
}

impl Stream {
    pub(crate) fn new(seq: u32, window: i64, head: bool, recv: Option<Box<Recv>>) -> Self {
        Stream {
            seq,
            window,
            head,
            recv,
            answered: false,
            zip: None,
            sid: None,
            left: None,
            done: false,
            turn: false,
            body: Vec::new(),
            sent: 0,
            stalled: None,
        }
    }

    /// body bytes not sent yet
    pub(crate) fn unsent(&self) -> usize {
        self.body.len() - self.sent
    }

    /// Adds answer body bytes. Sent bytes are dropped first, so a long
    /// stream does not grow the buffer.
    pub(crate) fn push(&mut self, data: &[u8]) {
        if self.sent == self.body.len() {
            self.body.clear();
            self.sent = 0;
        } else if self.sent > self.body.len() / 2 {
            self.body.drain(..self.sent);
            self.sent = 0;
        }
        self.body.extend_from_slice(data);
    }
}
