//! One HTTP/2 stream on the server side.
//!
//! 2a: a stream is only kept while its request is whole (END_STREAM came)
//! and its answer is not all sent: the "half-closed (remote)" state of
//! RFC 9113 5.1. Open streams (a request body) come in 2b.

pub(crate) struct Stream {
    /// the request sequence number Dart knows (low 32 bits of the id)
    pub(crate) seq: u32,
    /// the send window of the client for this stream. It can go below 0
    /// when the client lowers SETTINGS_INITIAL_WINDOW_SIZE.
    pub(crate) window: i64,
    /// the request is HEAD: the answer has no body
    pub(crate) head: bool,
    /// Dart answered (maybe a gzip job still runs)
    pub(crate) answered: bool,
    /// a gzip job runs: the status and the header lines wait for it
    pub(crate) zip: Option<(u16, Vec<u8>)>,
    /// answer body bytes, `sent..` is not sent yet
    pub(crate) body: Vec<u8>,
    pub(crate) sent: usize,
}

impl Stream {
    pub(crate) fn new(seq: u32, window: i64, head: bool) -> Self {
        Stream {
            seq,
            window,
            head,
            answered: false,
            zip: None,
            body: Vec::new(),
            sent: 0,
        }
    }

    /// body bytes not sent yet
    pub(crate) fn left(&self) -> usize {
        self.body.len() - self.sent
    }
}
