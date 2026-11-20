//! HTTP/2. For now only HPACK; the connection code comes in the next PR.

// ponytail: nothing outside tests uses this yet. Drop this line when the
// connection code uses the decoder and the encoder.
#![allow(dead_code)]

pub(crate) mod hpack;
mod huffman;

#[cfg(test)]
mod tests;
