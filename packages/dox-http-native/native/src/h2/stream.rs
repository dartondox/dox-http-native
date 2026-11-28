//! One HTTP/2 stream on the server side.
//!
//! A stream is kept from its HEADERS until its answer is all sent. While
//! `recv` is set, the request body comes in ("open" in RFC 9113 5.1).
//! After END_STREAM it is "half-closed (remote)": Dart has the request
//! and the answer is due or goes out.

use std::collections::HashMap;
use std::time::Instant;

use slab::Slab;

/// A request body that comes in DATA frames. Buffered: Dart gets the
/// request once it is whole. Streamed: Dart has the request and gets each
/// DATA as a BODY_PART.
pub(crate) struct Recv {
    /// the record head, method, target and headers (id set, no body).
    /// Empty when streamed: Dart has it.
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
    /// the body goes to Dart in parts (BODY_PART), with no size limit and
    /// no body timeout
    pub(crate) streamed: bool,
    /// streamed: body bytes given to Dart so far
    pub(crate) taken: u64,
    /// streamed: window bytes Dart acked (BODY_ACK) and padding, not
    /// given back to the client yet
    pub(crate) acked: u64,
}

/// The state that only some streams need: a gzip answer, a streamed
/// answer or a streamed request body. Apart from [Stream], so a plain
/// stream stays small: each connection keeps a map of its streams, and
/// the size of one entry times the slots of that map is memory the
/// connection holds (see `stream_is_small`).
#[derive(Default)]
pub(crate) struct Rare {
    /// a gzip job runs: the status and the header lines wait for it
    pub(crate) zip: Option<(u16, Vec<u8>)>,
    /// a streamed answer (kind 6): Dart's stream id
    pub(crate) sid: Option<u64>,
    /// streamed: body bytes Dart may still send (its content-length)
    pub(crate) left: Option<u64>,
    /// streamed request body: bytes given to Dart that it has not acked
    /// (see `H2::unacked`)
    pub(crate) owed: u64,
    /// the trailers of a streamed answer (an HPACK block), sent after the
    /// last DATA. Empty: none.
    pub(crate) trailers: Vec<u8>,
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
    /// no more body bytes come from Dart (always true for kind 0)
    pub(crate) done: bool,
    /// the stream is in the turns of [H2::send](super::H2::send)
    pub(crate) turn: bool,
    /// answer body bytes, `sent..` is not sent yet
    pub(crate) body: Vec<u8>,
    pub(crate) sent: usize,
    /// Some answer bytes went out in DATA: a full answer then gives a
    /// reset token back (see `RESET_REFUND`).
    pub(crate) wrote: bool,
    /// since when body bytes wait for a stream window of 0 or less
    pub(crate) stalled: Option<Instant>,
    /// None until the stream needs one of the [Rare] fields
    pub(crate) rare: Option<Box<Rare>>,
}

impl Stream {
    pub(crate) fn new(seq: u32, window: i64, head: bool, recv: Option<Box<Recv>>) -> Self {
        Stream {
            seq,
            window,
            head,
            recv,
            answered: false,
            done: false,
            turn: false,
            body: Vec::new(),
            sent: 0,
            wrote: false,
            stalled: None,
            rare: None,
        }
    }

    /// The [Rare] fields, made on first use.
    pub(crate) fn rare(&mut self) -> &mut Rare {
        self.rare.get_or_insert_with(Box::default)
    }

    /// a streamed answer: Dart's stream id
    pub(crate) fn sid(&self) -> Option<u64> {
        self.rare.as_ref().and_then(|r| r.sid)
    }

    /// a gzip job runs for the answer
    pub(crate) fn zipping(&self) -> bool {
        self.rare.as_ref().is_some_and(|r| r.zip.is_some())
    }

    /// streamed request body bytes Dart has not acked
    pub(crate) fn owed(&self) -> u64 {
        self.rare.as_ref().map_or(0, |r| r.owed)
    }

    /// the trailers of a streamed answer (empty: none)
    pub(crate) fn trailers(&self) -> &[u8] {
        self.rare.as_ref().map_or(&[], |r| &r.trailers)
    }

    /// The request record went to Dart: it knows the request.
    pub(crate) fn known(&self) -> bool {
        self.recv.as_ref().is_none_or(|r| r.streamed)
    }

    /// The request body still streams to Dart.
    pub(crate) fn streaming(&self) -> bool {
        self.recv.as_ref().is_some_and(|r| r.streamed)
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

/// The open streams of one connection, by stream id. The streams sit in a
/// slab, and the map holds only `id -> slot` (8 bytes an entry). With many
/// streams opened and closed, a map keeps up to twice the slots of its
/// most entries, so a map of whole streams held 256 stream slots for 100
/// open streams. The slab holds only as many as were open at once, and
/// reuses a free slot for the next stream (no allocation per stream).
#[derive(Default)]
pub(crate) struct Streams {
    pub(crate) ids: HashMap<u32, u32>,
    pub(crate) slab: Slab<(u32, Stream)>,
}

impl Streams {
    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub(crate) fn contains_key(&self, id: &u32) -> bool {
        self.ids.contains_key(id)
    }

    pub(crate) fn get(&self, id: &u32) -> Option<&Stream> {
        self.ids.get(id).map(|&k| &self.slab[k as usize].1)
    }

    pub(crate) fn get_mut(&mut self, id: &u32) -> Option<&mut Stream> {
        self.ids.get(id).map(|&k| &mut self.slab[k as usize].1)
    }

    /// Adds stream `id`. A stream with the same id is replaced.
    pub(crate) fn insert(&mut self, id: u32, s: Stream) {
        if let Some(&k) = self.ids.get(&id) {
            self.slab[k as usize].1 = s;
            return;
        }
        let k = self.slab.insert((id, s));
        self.ids.insert(id, k as u32);
    }

    pub(crate) fn remove(&mut self, id: &u32) -> Option<Stream> {
        let k = self.ids.remove(id)?;
        Some(self.slab.remove(k as usize).1)
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &Stream> {
        self.slab.iter().map(|(_, e)| &e.1)
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut Stream> {
        self.slab.iter_mut().map(|(_, e)| &mut e.1)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&u32, &Stream)> {
        self.slab.iter().map(|(_, e)| (&e.0, &e.1))
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = (&u32, &mut Stream)> {
        self.slab.iter_mut().map(|(_, e)| (&e.0, &mut e.1))
    }
}

impl std::ops::Index<&u32> for Streams {
    type Output = Stream;

    fn index(&self, id: &u32) -> &Stream {
        self.get(id).expect("an open stream")
    }
}
