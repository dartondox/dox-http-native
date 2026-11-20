use super::hpack::{Decoded, Decoder, HpackError, encode_field, read_int, write_int};
use super::huffman::{self, CODES};

/// Bytes from hex text; spaces are skipped.
fn hex(s: &str) -> Vec<u8> {
    let s: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    s.chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}

type Fields = Vec<(String, String)>;

fn fields(list: &[(&str, &str)]) -> Fields {
    list.iter()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect()
}

fn decode(d: &mut Decoder, block: &[u8], max_list: usize) -> Result<Fields, HpackError> {
    let mut out = Vec::new();
    d.decode(block, max_list, |n, v| {
        out.push((
            String::from_utf8(n.to_vec()).unwrap(),
            String::from_utf8(v.to_vec()).unwrap(),
        ))
    })?;
    Ok(out)
}

/// Decodes `block` and checks the fields, the table size and the table
/// entries (newest first).
fn step(d: &mut Decoder, block: &str, want: &[(&str, &str)], size: usize, table: &[(&str, &str)]) {
    assert_eq!(decode(d, &hex(block), usize::MAX).unwrap(), fields(want));
    let (got_size, entries) = d.table();
    let entries: Fields = entries
        .iter()
        .map(|(n, v)| {
            (
                String::from_utf8(n.to_vec()).unwrap(),
                String::from_utf8(v.to_vec()).unwrap(),
            )
        })
        .collect();
    assert_eq!(entries, fields(table));
    assert_eq!(got_size, size);
}

// C.1 integers

#[test]
fn c1_integers() {
    for (value, prefix, bytes) in [
        (10, 5, vec![0x0a]),
        (1337, 5, vec![0x1f, 0x9a, 0x0a]),
        (42, 8, vec![0x2a]),
    ] {
        let mut out = Vec::new();
        write_int(value, prefix, 0, &mut out);
        assert_eq!(out, bytes);
        let mut pos = 0;
        assert_eq!(read_int(&bytes, &mut pos, prefix).unwrap(), value);
        assert_eq!(pos, bytes.len());
    }
}

// C.2 literal types

#[test]
fn c2_literals() {
    let mut d = Decoder::new(4096);
    step(
        &mut d,
        "400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572",
        &[("custom-key", "custom-header")],
        55,
        &[("custom-key", "custom-header")],
    );
    let mut d = Decoder::new(4096);
    step(
        &mut d,
        "040c 2f73 616d 706c 652f 7061 7468",
        &[(":path", "/sample/path")],
        0,
        &[],
    );
    let mut d = Decoder::new(4096);
    step(
        &mut d,
        "1008 7061 7373 776f 7264 0673 6563 7265 74",
        &[("password", "secret")],
        0,
        &[],
    );
    let mut d = Decoder::new(4096);
    step(&mut d, "82", &[(":method", "GET")], 0, &[]);
}

// C.3 and C.4: three requests on one connection, plain and Huffman

const REQ1: &[(&str, &str)] = &[
    (":method", "GET"),
    (":scheme", "http"),
    (":path", "/"),
    (":authority", "www.example.com"),
];
const REQ2: &[(&str, &str)] = &[
    (":method", "GET"),
    (":scheme", "http"),
    (":path", "/"),
    (":authority", "www.example.com"),
    ("cache-control", "no-cache"),
];
const REQ3: &[(&str, &str)] = &[
    (":method", "GET"),
    (":scheme", "https"),
    (":path", "/index.html"),
    (":authority", "www.example.com"),
    ("custom-key", "custom-value"),
];

fn requests(blocks: [&str; 3]) {
    let mut d = Decoder::new(4096);
    step(
        &mut d,
        blocks[0],
        REQ1,
        57,
        &[(":authority", "www.example.com")],
    );
    step(
        &mut d,
        blocks[1],
        REQ2,
        110,
        &[
            ("cache-control", "no-cache"),
            (":authority", "www.example.com"),
        ],
    );
    step(
        &mut d,
        blocks[2],
        REQ3,
        164,
        &[
            ("custom-key", "custom-value"),
            ("cache-control", "no-cache"),
            (":authority", "www.example.com"),
        ],
    );
}

#[test]
fn c3_requests() {
    requests([
        "8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d",
        "8286 84be 5808 6e6f 2d63 6163 6865",
        "8287 85bf 400a 6375 7374 6f6d 2d6b 6579 0c63 7573 746f 6d2d 7661 6c75 65",
    ]);
}

#[test]
fn c4_requests_huffman() {
    requests([
        "8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff",
        "8286 84be 5886 a8eb 1064 9cbf",
        "8287 85bf 4088 25a8 49e9 5ba9 7d7f 8925 a849 e95b b8e8 b4bf",
    ]);
}

// C.5 and C.6: three responses with a 256-byte table, so entries are evicted

const DATE1: &str = "Mon, 21 Oct 2013 20:13:21 GMT";
const DATE2: &str = "Mon, 21 Oct 2013 20:13:22 GMT";
const LOC: &str = "https://www.example.com";
const COOKIE: &str = "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1";

fn responses(blocks: [&str; 3]) {
    let mut d = Decoder::new(256);
    step(
        &mut d,
        blocks[0],
        &[
            (":status", "302"),
            ("cache-control", "private"),
            ("date", DATE1),
            ("location", LOC),
        ],
        222,
        &[
            ("location", LOC),
            ("date", DATE1),
            ("cache-control", "private"),
            (":status", "302"),
        ],
    );
    step(
        &mut d,
        blocks[1],
        &[
            (":status", "307"),
            ("cache-control", "private"),
            ("date", DATE1),
            ("location", LOC),
        ],
        222,
        &[
            (":status", "307"),
            ("location", LOC),
            ("date", DATE1),
            ("cache-control", "private"),
        ],
    );
    step(
        &mut d,
        blocks[2],
        &[
            (":status", "200"),
            ("cache-control", "private"),
            ("date", DATE2),
            ("location", LOC),
            ("content-encoding", "gzip"),
            ("set-cookie", COOKIE),
        ],
        215,
        &[
            ("set-cookie", COOKIE),
            ("content-encoding", "gzip"),
            ("date", DATE2),
        ],
    );
}

#[test]
fn c5_responses() {
    responses([
        "4803 3330 3258 0770 7269 7661 7465 611d 4d6f 6e2c 2032 3120 4f63 7420 3230 3133 \
         2032 303a 3133 3a32 3120 474d 546e 1768 7474 7073 3a2f 2f77 7777 2e65 7861 6d70 \
         6c65 2e63 6f6d",
        "4803 3330 37c1 c0bf",
        "88c1 611d 4d6f 6e2c 2032 3120 4f63 7420 3230 3133 2032 303a 3133 3a32 3220 474d \
         54c0 5a04 677a 6970 7738 666f 6f3d 4153 444a 4b48 514b 425a 584f 5157 454f 5049 \
         5541 5851 5745 4f49 553b 206d 6178 2d61 6765 3d33 3630 303b 2076 6572 7369 6f6e \
         3d31",
    ]);
}

#[test]
fn c6_responses_huffman() {
    responses([
        "4882 6402 5885 aec3 771a 4b61 96d0 7abe 9410 54d4 44a8 2005 9504 0b81 66e0 82a6 \
         2d1b ff6e 919d 29ad 1718 63c7 8f0b 97c8 e9ae 82ae 43d3",
        "4883 640e ffc1 c0bf",
        "88c1 6196 d07a be94 1054 d444 a820 0595 040b 8166 e084 a62d 1bff c05a 839b d9ab \
         77ad 94e7 821d d7f2 e6c7 b335 dfdf cd5b 3960 d5af 2708 7f36 72c1 ab27 0fb5 291f \
         9587 3160 65c0 03ed 4ee5 b106 3d50 07",
    ]);
}

// Huffman

/// A test-only encoder from the same table, with all-ones padding.
fn huffman_encode(src: &[u8]) -> Vec<u8> {
    let (mut acc, mut bits, mut out) = (0u64, 0u32, Vec::new());
    for &b in src {
        let (code, len) = CODES[b as usize];
        acc = (acc << len) | u64::from(code);
        bits += u32::from(len);
        while bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    if bits > 0 {
        out.push(((acc << (8 - bits)) | ((1 << (8 - bits)) - 1)) as u8);
    }
    out
}

fn huffman_decode(src: &[u8]) -> Result<Vec<u8>, HpackError> {
    let mut out = Vec::new();
    huffman::decode(src, &mut out, usize::MAX)?;
    Ok(out)
}

#[test]
fn huffman_every_symbol() {
    for b in 0..=255u8 {
        assert_eq!(
            huffman_decode(&huffman_encode(&[b])).unwrap(),
            [b],
            "byte {b}"
        );
    }
    let all: Vec<u8> = (0..=255u8).chain((0..=255u8).rev()).collect();
    assert_eq!(huffman_decode(&huffman_encode(&all)).unwrap(), all);
    // the RFC example "www.example.com"
    assert_eq!(
        huffman_encode(b"www.example.com"),
        hex("f1e3 c2e5 f23a 6ba0 ab90 f4ff")
    );
}

#[test]
fn huffman_bad_input() {
    // EOS (30 ones) inside the data
    assert_eq!(
        huffman_decode(&[0xff, 0xff, 0xff, 0xfc]),
        Err(HpackError::BadHuffman)
    );
    // 8 bits of padding: a whole byte of ones
    let mut a = huffman_encode(b"a"); // 5 bits + 3 bits padding
    a.push(0xff);
    assert_eq!(huffman_decode(&a), Err(HpackError::BadHuffman));
    // padding that is not all ones: 'a' is 00011, then 000
    assert_eq!(huffman_decode(&[0x18]), Err(HpackError::BadHuffman));
    assert_eq!(huffman_decode(&[0x1f]).unwrap(), b"a");
    // the room limit stops the output
    let mut out = Vec::new();
    assert_eq!(
        huffman::decode(&huffman_encode(b"abcdef"), &mut out, 3),
        Ok(6)
    );
    assert_eq!(out, b"abc");
}

// Encoder

#[test]
fn encoder_round_trip() {
    let list = fields(&[
        (":status", "200"),
        (":status", "418"),
        ("content-type", "application/grpc"),
        ("accept-encoding", "gzip, deflate"),
        ("x-custom", ""),
        ("grpc-status", "0"),
        ("set-cookie", &"v".repeat(300)),
    ]);
    let mut out = Vec::new();
    for (n, v) in &list {
        encode_field(n.as_bytes(), v.as_bytes(), &mut out);
    }
    // `:status: 200` is static index 8, one byte
    assert_eq!(out[0], 0x88);
    // `:status: 418` keeps the indexed name (8) without indexing
    assert_eq!(&out[1..6], &[0x08, 0x03, b'4', b'1', b'8']);
    let mut d = Decoder::new(4096);
    assert_eq!(decode(&mut d, &out, usize::MAX).unwrap(), list);
    // no dynamic table is used
    assert_eq!(d.table().0, 0);
}

// Errors

#[test]
fn bad_index() {
    let mut d = Decoder::new(4096);
    assert_eq!(
        decode(&mut d, &[0x80], usize::MAX),
        Err(HpackError::BadIndex)
    );
    // 62 with an empty dynamic table
    assert_eq!(
        decode(&mut d, &[0xbe], usize::MAX),
        Err(HpackError::BadIndex)
    );
    // a literal with name index 70
    assert_eq!(
        decode(&mut d, &[0x0f, 0x37, 0x00], usize::MAX),
        Err(HpackError::BadIndex)
    );
}

#[test]
fn size_updates() {
    let mut d = Decoder::new(4096);
    // over the limit
    assert_eq!(
        decode(&mut d, &hex("3fe2 1f"), usize::MAX),
        Err(HpackError::BadSizeUpdate)
    );
    // after a field
    let mut d = Decoder::new(4096);
    assert_eq!(
        decode(&mut d, &[0x82, 0x20], usize::MAX),
        Err(HpackError::BadSizeUpdate)
    );
    // lower to 0 then raise to 4096 at the start of a block is fine;
    // the 0 empties the table
    let mut d = Decoder::new(4096);
    decode(
        &mut d,
        &hex("418c f1e3 c2e5 f23a 6ba0 ab90 f4ff"),
        usize::MAX,
    )
    .unwrap();
    assert_eq!(d.table().0, 57);
    let mut block = vec![0x20];
    write_int(4096, 5, 0x20, &mut block);
    block.push(0x82);
    assert_eq!(
        decode(&mut d, &block, usize::MAX).unwrap(),
        fields(&[(":method", "GET")])
    );
    assert_eq!(d.table(), (0, vec![]));
    assert_eq!(d.max_size(), 4096);
}

#[test]
fn integer_overflow() {
    let mut pos = 0;
    // 2^32 does not fit
    let mut big = Vec::new();
    write_int(1 << 32, 7, 0x80, &mut big);
    assert_eq!(
        read_int(&big, &mut pos, 7),
        Err(HpackError::IntegerOverflow)
    );
    // too many continuation bytes, even with zero bits
    let mut pos = 0;
    let long = [0x7f, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00];
    assert_eq!(
        read_int(&long, &mut pos, 7),
        Err(HpackError::IntegerOverflow)
    );
    let mut d = Decoder::new(4096);
    assert_eq!(
        decode(&mut d, &big, usize::MAX),
        Err(HpackError::IntegerOverflow)
    );
    // u32::MAX itself is read
    let mut max = Vec::new();
    write_int(u32::MAX as usize, 7, 0, &mut max);
    let mut pos = 0;
    assert_eq!(read_int(&max, &mut pos, 7), Ok(u32::MAX as usize));
}

#[test]
fn truncated() {
    let full = hex("400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572");
    for cut in 1..full.len() {
        let mut d = Decoder::new(4096);
        assert_eq!(
            decode(&mut d, &full[..cut], usize::MAX),
            Err(HpackError::Truncated),
            "cut {cut}"
        );
    }
    // an integer that stops in its continuation bytes
    let mut d = Decoder::new(4096);
    assert_eq!(
        decode(&mut d, &[0xff, 0x80], usize::MAX),
        Err(HpackError::Truncated)
    );
}

/// Decodes with a cap and gives (fields, result).
fn capped(d: &mut Decoder, block: &[u8], cap: usize) -> (Fields, Result<Decoded, HpackError>) {
    let mut out = Vec::new();
    let res = d.decode(block, cap, |n, v| {
        out.push((
            String::from_utf8(n.to_vec()).unwrap(),
            String::from_utf8(v.to_vec()).unwrap(),
        ))
    });
    (out, res)
}

#[test]
fn list_over_cap() {
    let block = hex("8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d");
    // sizes: 42 + 43 + 38 + 57 = 180
    let mut d = Decoder::new(4096);
    assert_eq!(capped(&mut d, &block, 180).0.len(), 4);
    // over the cap: the fields before are given, the rest is read to the
    // end (the table stays in sync), and the result says so
    let mut d = Decoder::new(4096);
    let (got, res) = capped(&mut d, &block, 179);
    assert_eq!(res, Ok(Decoded::TooLarge));
    assert_eq!(got.len(), 3);
    assert_eq!(d.table().0, 57, "the last field is in the table");
    // a long Huffman string is counted, not kept
    let value = huffman_encode(&[b'a'; 1000]);
    let mut block = vec![0x00, 0x01, b'x'];
    write_int(value.len(), 7, 0x80, &mut block);
    block.extend_from_slice(&value);
    let mut d = Decoder::new(4096);
    let (got, res) = capped(&mut d, &block, 32 + 1 + 100);
    assert_eq!((got.len(), res), (0, Ok(Decoded::TooLarge)));
    // a plain string over the room is the same
    let mut d = Decoder::new(4096);
    let (got, res) = capped(&mut d, &[0x00, 0x01, b'x', 0x02, b'a', b'b'], 34);
    assert_eq!((got.len(), res), (0, Ok(Decoded::TooLarge)));
}

#[test]
fn over_cap_keeps_the_table_in_sync() {
    // a field that is over the cap but is added to the table, then a
    // field that is not
    let big = "v".repeat(300);
    let mut block = indexed_literal("a", &big[..100]);
    block.extend(indexed_literal("b", "2"));
    let mut d = Decoder::new(4096);
    let (got, res) = capped(&mut d, &block, 100);
    assert_eq!((got.len(), res), (0, Ok(Decoded::TooLarge)));
    let (size, entries) = d.table();
    assert_eq!(size, 133 + 34);
    assert_eq!(entries[0], (&b"b"[..], &b"2"[..]));
    // a string over the table size empties the table, even over the cap
    let mut long = vec![0x40, 1, b'c'];
    write_int(5000, 7, 0, &mut long);
    long.extend(vec![b'z'; 5000]);
    let (_, res) = capped(&mut d, &long, 100);
    assert_eq!(res, Ok(Decoded::TooLarge));
    assert_eq!(d.table().0, 0);
    // a real error after the cap is still an error
    assert_eq!(
        capped(&mut d, &[0x82, 0x82, 0x82, 0x80 | 100], 50).1,
        Err(HpackError::BadIndex)
    );
}

// dynamic table edge cases

/// A literal with incremental indexing and a new name, no Huffman.
fn indexed_literal(name: &str, value: &str) -> Vec<u8> {
    let mut out = vec![0x40, name.len() as u8];
    out.extend_from_slice(name.as_bytes());
    out.push(value.len() as u8);
    out.extend_from_slice(value.as_bytes());
    out
}

#[test]
fn entry_bigger_than_table_empties_it() {
    let mut d = Decoder::new(50);
    decode(&mut d, &indexed_literal("a", "b"), usize::MAX).unwrap();
    assert_eq!(d.table().0, 34);
    // 10 + 13 + 32 = 55, over the whole table size of 50
    let block = indexed_literal("custom-key", "custom-header");
    let got = decode(&mut d, &block, usize::MAX).unwrap();
    assert_eq!(got, fields(&[("custom-key", "custom-header")]));
    let (size, entries) = d.table();
    assert_eq!(size, 0);
    assert!(entries.is_empty());
}

#[test]
fn smaller_size_update_evicts_oldest() {
    let mut d = Decoder::new(4096);
    for n in ["n1", "n2", "n3"] {
        decode(&mut d, &indexed_literal(n, "v1"), usize::MAX).unwrap();
    }
    assert_eq!(d.table().0, 108);
    // size update to 80: 0x20 | 31, then 80 - 31 = 49
    decode(&mut d, &[0x3f, 0x31], usize::MAX).unwrap();
    assert_eq!(d.max_size(), 80);
    let (size, entries) = d.table();
    assert_eq!(size, 72);
    assert_eq!(
        entries,
        vec![(&b"n3"[..], &b"v1"[..]), (&b"n2"[..], &b"v1"[..])]
    );
}

#[test]
fn indexed_field_over_cap_is_list_too_large() {
    // static index 2 is :method GET: 7 + 3 + 32 = 42
    let mut d = Decoder::new(4096);
    assert_eq!(decode(&mut d, &[0x82], 42).unwrap().len(), 1);
    assert_eq!(capped(&mut d, &[0x82], 41).1, Ok(Decoded::TooLarge));
    // the second field pushes the list over: 42 + 42 = 84
    assert_eq!(capped(&mut d, &[0x82, 0x82], 83).1, Ok(Decoded::TooLarge));
    assert_eq!(decode(&mut d, &[0x82, 0x82], 84).unwrap().len(), 2);
    // dynamic index 62: 10 + 13 + 32 = 55
    decode(
        &mut d,
        &indexed_literal("custom-key", "custom-header"),
        usize::MAX,
    )
    .unwrap();
    assert_eq!(capped(&mut d, &[0xbe], 54).1, Ok(Decoded::TooLarge));
    assert_eq!(decode(&mut d, &[0xbe], 55).unwrap().len(), 1);
}

#[test]
fn name_from_entry_evicted_by_same_insert() {
    let mut d = Decoder::new(100);
    decode(&mut d, &indexed_literal("aa", "bb"), usize::MAX).unwrap();
    decode(&mut d, &indexed_literal("cc", "dd"), usize::MAX).unwrap();
    assert_eq!(d.table().0, 72);
    // name from index 63 (the oldest, "aa"); the new entry is 36 bytes, so
    // the table is 108 and "aa" is evicted
    let got = decode(&mut d, &[0x7f, 0x00, 0x02, b'e', b'e'], usize::MAX).unwrap();
    assert_eq!(got, fields(&[("aa", "ee")]));
    let (size, entries) = d.table();
    assert_eq!(size, 72);
    assert_eq!(
        entries,
        vec![(&b"aa"[..], &b"ee"[..]), (&b"cc"[..], &b"dd"[..])]
    );
}

// ---- frames and the connection --------------------------------------

mod conn {
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};

    use super::super::frame::{self, Error, Frame, Head};
    use super::super::hpack::{Decoder, encode_field, write_int};
    use super::super::{CONN_WINDOW, DEFAULT_WINDOW, H2, HOLD, State};
    use crate::SEND_WAIT;
    use crate::run::MAX_HEADERS;

    fn head(len: usize, kind: u8, flags: u8, id: u32) -> Head {
        Head {
            len,
            kind,
            flags,
            id,
        }
    }

    /// Writes a frame, reads it back.
    fn round(kind: u8, flags: u8, id: u32, payload: &[u8]) -> (Head, Vec<u8>) {
        let mut out = Vec::new();
        frame::write(&mut out, kind, flags, id, payload);
        let h = frame::read_head(&out).unwrap();
        assert_eq!(h, head(payload.len(), kind, flags, id));
        (h, out[frame::HEAD..].to_vec())
    }

    fn parse(kind: u8, flags: u8, id: u32, payload: &[u8]) -> Result<String, Error> {
        let (h, p) = round(kind, flags, id, payload);
        frame::parse(&h, &p).map(|f| format!("{f:?}"))
    }

    fn conn_err(code: u32) -> Error {
        Error {
            code,
            stream: false,
        }
    }

    #[test]
    fn frame_round_trips() {
        let ok = |kind, flags, id, p: &[u8], want: Frame| {
            assert_eq!(parse(kind, flags, id, p), Ok(format!("{want:?}")));
        };
        ok(
            frame::DATA,
            frame::END_STREAM | frame::PADDED,
            1,
            b"\x02abc\0\0",
            Frame::Data {
                end: true,
                data: b"abc",
                flow: 6,
            },
        );
        ok(
            frame::HEADERS,
            frame::END_HEADERS | frame::PRIORITY_FLAG,
            3,
            b"\x80\0\0\x01\x10xy",
            Frame::Headers {
                end_stream: false,
                end_headers: true,
                dep: Some(1),
                block: b"xy",
            },
        );
        ok(
            frame::PRIORITY,
            0,
            3,
            b"\0\0\0\x05\x10",
            Frame::Priority { dep: 5 },
        );
        ok(
            frame::RST_STREAM,
            0,
            3,
            &8u32.to_be_bytes(),
            Frame::RstStream { code: 8 },
        );
        ok(
            frame::SETTINGS,
            0,
            0,
            b"\0\x04\0\0\x01\0",
            Frame::Settings {
                ack: false,
                params: b"\0\x04\0\0\x01\0",
            },
        );
        ok(
            frame::PUSH_PROMISE,
            frame::END_HEADERS,
            1,
            b"\0\0\0\x02zz",
            Frame::PushPromise {
                end_headers: true,
                promised: 2,
                block: b"zz",
            },
        );
        ok(
            frame::PING,
            frame::ACK,
            0,
            b"12345678",
            Frame::Ping {
                ack: true,
                data: *b"12345678",
            },
        );
        ok(
            frame::GOAWAY,
            0,
            0,
            b"\0\0\0\x07\0\0\0\x01dbg",
            Frame::GoAway {
                last: 7,
                code: 1,
                debug: b"dbg",
            },
        );
        ok(
            frame::WINDOW_UPDATE,
            0,
            0,
            &100u32.to_be_bytes(),
            Frame::WindowUpdate { inc: 100 },
        );
        ok(
            frame::CONTINUATION,
            frame::END_HEADERS,
            1,
            b"q",
            Frame::Continuation {
                end_headers: true,
                block: b"q",
            },
        );
        ok(0xfa, 0, 0, b"anything", Frame::Unknown);
        let pairs: Vec<_> = frame::settings(b"\0\x03\0\0\0\x64\0\x05\0\0\x40\0").collect();
        assert_eq!(pairs, vec![(3, 100), (5, 16384)]);
    }

    #[test]
    fn bad_frames() {
        use frame::{
            ACK, CONTINUATION, DATA, FRAME_SIZE_ERROR, GOAWAY, HEADERS, PADDED, PING, PRIORITY,
            PROTOCOL_ERROR, RST_STREAM, SETTINGS, WINDOW_UPDATE,
        };
        let size = conn_err(FRAME_SIZE_ERROR);
        let proto = conn_err(PROTOCOL_ERROR);
        assert_eq!(parse(PING, 0, 0, b"1234567"), Err(size));
        assert_eq!(parse(RST_STREAM, 0, 1, b"123"), Err(size));
        assert_eq!(parse(WINDOW_UPDATE, 0, 1, b"12345"), Err(size));
        assert_eq!(parse(SETTINGS, 0, 0, b"12345"), Err(size));
        assert_eq!(parse(SETTINGS, ACK, 0, b"123456"), Err(size));
        assert_eq!(parse(GOAWAY, 0, 0, b"1234567"), Err(size));
        // PRIORITY with a wrong size hits only its stream
        assert_eq!(
            parse(PRIORITY, 0, 1, b"1234"),
            Err(Error {
                code: FRAME_SIZE_ERROR,
                stream: true
            })
        );
        // padding as long as the payload
        assert_eq!(parse(DATA, PADDED, 1, b"\x04abc"), Err(proto));
        assert_eq!(parse(HEADERS, PADDED, 1, b""), Err(size));
        // stream frames need an id, connection frames must not have one
        assert_eq!(parse(DATA, 0, 0, b"x"), Err(proto));
        assert_eq!(parse(HEADERS, 0, 0, b"x"), Err(proto));
        assert_eq!(parse(CONTINUATION, 0, 0, b"x"), Err(proto));
        assert_eq!(parse(SETTINGS, 0, 1, b""), Err(proto));
        assert_eq!(parse(PING, 0, 1, b"12345678"), Err(proto));
        assert_eq!(parse(GOAWAY, 0, 3, b"12345678"), Err(proto));
    }

    #[test]
    fn a_big_header_block_is_split_into_continuation() {
        let mut out = Vec::new();
        frame::write_headers(&mut out, 5, &[1u8; 40], true, 16);
        let fs = frames(&out);
        let kinds: Vec<_> = fs.iter().map(|(h, p)| (h.kind, h.flags, p.len())).collect();
        assert_eq!(
            kinds,
            vec![
                (frame::HEADERS, frame::END_STREAM, 16),
                (frame::CONTINUATION, 0, 16),
                (frame::CONTINUATION, frame::END_HEADERS, 8),
            ]
        );
    }

    // ---- a connection, driven frame by frame ----

    const DATE: &[u8] = b"date: Thu, 01 Jan 2026 00:00:00 GMT\r\n";

    /// All frames in `b`.
    fn frames(b: &[u8]) -> Vec<(Head, Vec<u8>)> {
        let mut out = Vec::new();
        let mut p = 0;
        while let Some(h) = frame::read_head(&b[p..]) {
            let end = p + frame::HEAD + h.len;
            out.push((h, b[p + frame::HEAD..end].to_vec()));
            p = end;
        }
        assert_eq!(p, b.len(), "a cut frame");
        out
    }

    struct Client {
        h2: Box<H2>,
        seq: u32,
        /// records Dart got (no body), and their bodies
        recs: Vec<Vec<u8>>,
        bodies: Vec<Vec<u8>>,
        /// the time of the reads
        now: Instant,
        /// bytes "written" so far, and the STREAM_SENT marks
        written: u64,
        marks: VecDeque<(u64, u64, u64)>,
    }

    /// The body limit of [Client::new] (and twice it for a multipart form).
    const MAX_BODY: usize = 10 << 20;

    impl Client {
        /// A connection after the preface and the client SETTINGS.
        fn new(max_streams: u32) -> Self {
            Self::with_max(max_streams, MAX_BODY)
        }

        fn with_max(max_streams: u32, max_body: usize) -> Self {
            let mut out = Vec::new();
            let h2 = H2::new(1, [0; 18], max_streams, (max_body, 2 * max_body), &mut out);
            let fs = frames(&out);
            assert_eq!(fs[0].0.kind, frame::SETTINGS);
            let ours: Vec<_> = frame::settings(&fs[0].1).collect();
            assert_eq!(
                ours,
                vec![
                    (3, max_streams),
                    (4, 1 << 20),
                    (5, 16384),
                    (6, 16384),
                    (2, 0)
                ]
            );
            assert_eq!(fs[1].0.kind, frame::WINDOW_UPDATE);
            assert_eq!(
                fs[1].1,
                ((CONN_WINDOW - DEFAULT_WINDOW) as u32).to_be_bytes()
            );
            let mut c = Client {
                h2,
                seq: 0,
                recs: Vec::new(),
                bodies: Vec::new(),
                now: Instant::now(),
                written: 0,
                marks: VecDeque::new(),
            };
            let acks = c.send(frame::SETTINGS, 0, 0, b"");
            assert_eq!(kinds(&acks), vec![(frame::SETTINGS, frame::ACK, 0)]);
            c
        }

        /// Feeds raw bytes, returns the frames the server wrote.
        fn feed(&mut self, b: &[u8]) -> Vec<(Head, Vec<u8>)> {
            let mut out = Vec::new();
            let mut pos = 0;
            while self
                .h2
                .step(b, &mut pos, &mut out, &mut self.seq, DATE, self.now)
            {
                if let Some((rec, body)) = self.h2.held.take() {
                    self.recs.push(rec);
                    self.bodies.push(body);
                }
            }
            frames(&out)
        }

        /// [H2::send] into `out`. `written` counts the bytes before
        /// `out[wpos..]`; after the call all of `out` counts as written.
        fn send_into(&mut self, out: &mut Vec<u8>, wpos: usize) {
            self.h2.send(out, wpos, self.written, &mut self.marks);
            self.written += (out.len() - wpos) as u64;
        }

        /// The STREAM_SENT bytes of `sid` that are written by now.
        fn sent(&mut self, sid: u64) -> u64 {
            let mut n = 0;
            while let Some(&(at, s, k)) = self.marks.front() {
                if at > self.written {
                    break;
                }
                self.marks.pop_front();
                assert_eq!(s, sid);
                n += k;
            }
            n
        }

        fn send(&mut self, kind: u8, flags: u8, id: u32, p: &[u8]) -> Vec<(Head, Vec<u8>)> {
            let mut b = Vec::new();
            frame::write(&mut b, kind, flags, id, p);
            self.feed(&b)
        }

        fn get(&mut self, id: u32, fields: &[(&str, &str)]) -> Vec<(Head, Vec<u8>)> {
            self.get_block(id, &block(fields))
        }

        fn get_block(&mut self, id: u32, block: &[u8]) -> Vec<(Head, Vec<u8>)> {
            let flags = frame::END_STREAM | frame::END_HEADERS;
            self.send(frame::HEADERS, flags, id, block)
        }

        /// Answer of Dart for request `seq`, then the frames that go out.
        fn answer(
            &mut self,
            seq: u32,
            status: u16,
            lines: &[u8],
            body: &[u8],
        ) -> Vec<(Head, Vec<u8>)> {
            let mut out = Vec::new();
            self.h2.answer(seq, status, lines, body, DATE, &mut out);
            self.send_into(&mut out, 0);
            frames(&out)
        }

        fn pump(&mut self) -> Vec<(Head, Vec<u8>)> {
            let mut out = Vec::new();
            self.send_into(&mut out, 0);
            frames(&out)
        }
    }

    const GET: &[(&str, &str)] = &[
        (":method", "GET"),
        (":scheme", "http"),
        (":path", "/a?b"),
        (":authority", "example.test"),
    ];

    /// A POST header block with this content-length.
    fn post(len: usize) -> Vec<u8> {
        let len = len.to_string();
        block(&[
            (":method", "POST"),
            (":scheme", "http"),
            (":path", "/"),
            ("content-length", &len),
        ])
    }

    fn block(fields: &[(&str, &str)]) -> Vec<u8> {
        let mut b = Vec::new();
        for (n, v) in fields {
            encode_field(n.as_bytes(), v.as_bytes(), &mut b);
        }
        b
    }

    fn kinds(fs: &[(Head, Vec<u8>)]) -> Vec<(u8, u8, u32)> {
        fs.iter().map(|(h, _)| (h.kind, h.flags, h.id)).collect()
    }

    fn rst(id: u32, code: u32) -> Vec<(u8, u8, u32)> {
        let _ = code;
        vec![(frame::RST_STREAM, 0, id)]
    }

    /// The error code of the one RST_STREAM or GOAWAY in `fs`.
    fn code(fs: &[(Head, Vec<u8>)]) -> u32 {
        let (h, p) = fs.last().expect("a frame");
        match h.kind {
            frame::RST_STREAM => u32::from_be_bytes(p[..4].try_into().unwrap()),
            frame::GOAWAY => u32::from_be_bytes(p[4..8].try_into().unwrap()),
            k => panic!("frame type {k}"),
        }
    }

    fn goaway(fs: &[(Head, Vec<u8>)], want: u32) {
        assert_eq!(fs.last().map(|f| f.0.kind), Some(frame::GOAWAY), "{fs:?}");
        assert_eq!(code(fs), want);
    }

    fn decode(p: &[u8]) -> Vec<(String, String)> {
        let mut out = Vec::new();
        Decoder::new(4096)
            .decode(p, usize::MAX, |n, v| {
                out.push((
                    String::from_utf8(n.to_vec()).unwrap(),
                    String::from_utf8(v.to_vec()).unwrap(),
                ))
            })
            .unwrap();
        out
    }

    /// (method, target, headers) of a record.
    fn record(r: &[u8]) -> (u32, String, String, Vec<(String, String)>) {
        let flags = u32::from_le_bytes(r[8..12].try_into().unwrap());
        let ml = u16::from_le_bytes(r[12..14].try_into().unwrap()) as usize;
        let tl = u16::from_le_bytes(r[14..16].try_into().unwrap()) as usize;
        let hl = u32::from_le_bytes(r[16..20].try_into().unwrap()) as usize;
        let mut p = crate::REQ_HEAD;
        let m = String::from_utf8(r[p..p + ml].to_vec()).unwrap();
        p += ml;
        let t = String::from_utf8(r[p..p + tl].to_vec()).unwrap();
        p += tl;
        let end = p + hl;
        assert_eq!(end, r.len());
        let mut hs = Vec::new();
        while p < end {
            let nl = u16::from_le_bytes(r[p..p + 2].try_into().unwrap()) as usize;
            let vl = u16::from_le_bytes(r[p + 2..p + 4].try_into().unwrap()) as usize;
            p += 4;
            let n = String::from_utf8(r[p..p + nl].to_vec()).unwrap();
            let v = String::from_utf8(r[p + nl..p + nl + vl].to_vec()).unwrap();
            hs.push((n, v));
            p += nl + vl;
        }
        (flags, m, t, hs)
    }

    #[test]
    fn the_first_frame_must_be_settings() {
        let mut out = Vec::new();
        let mut h2 = H2::new(0, [0; 18], 250, (1, 1), &mut out);
        out.clear();
        let mut b = Vec::new();
        frame::write(&mut b, frame::PING, 0, 0, b"12345678");
        let (mut pos, mut seq) = (0, 0);
        h2.step(&b, &mut pos, &mut out, &mut seq, DATE, Instant::now());
        goaway(&frames(&out), frame::PROTOCOL_ERROR);
        assert!(h2.over());
    }

    #[test]
    fn ping_settings_and_unknown_frames() {
        let mut c = Client::new(250);
        let fs = c.send(frame::PING, 0, 0, b"abcdefgh");
        assert_eq!(kinds(&fs), vec![(frame::PING, frame::ACK, 0)]);
        assert_eq!(fs[0].1, b"abcdefgh");
        assert!(c.send(frame::PING, frame::ACK, 0, b"abcdefgh").is_empty());
        assert!(c.send(0x20, 0, 0, b"?").is_empty(), "unknown frame");
        assert!(c.send(frame::PRIORITY, 0, 9, b"\0\0\0\x01\x10").is_empty());
        let fs = c.send(frame::PRIORITY, 0, 9, b"\0\0\0\x09\x10");
        assert_eq!(code(&fs), frame::PROTOCOL_ERROR, "depends on itself");
        // ENABLE_PUSH 2 is not allowed
        goaway(
            &c.send(frame::SETTINGS, 0, 0, b"\0\x02\0\0\0\x02"),
            frame::PROTOCOL_ERROR,
        );
    }

    #[test]
    fn a_frame_over_our_max_frame_size() {
        let mut c = Client::new(250);
        // only the head is there: the error comes before the payload
        let mut b = Vec::new();
        frame::write(&mut b, frame::DATA, 0, 1, &[0; 16385]);
        goaway(&c.feed(&b[..frame::HEAD]), frame::FRAME_SIZE_ERROR);
        assert!(c.h2.over());
    }

    #[test]
    fn a_get_becomes_a_record() {
        let mut c = Client::new(250);
        let mut fields = GET.to_vec();
        fields.extend([
            ("cookie", "a=1"),
            ("x-y", "z"),
            ("cookie", "b=2"),
            ("te", "trailers"),
        ]);
        assert!(c.get(1, &fields).is_empty());
        let (flags, m, t, hs) = record(&c.recs[0]);
        assert_eq!(flags, 8, "flag bit 3: HTTP/2");
        assert_eq!((m.as_str(), t.as_str()), ("GET", "/a?b"));
        let hs: Vec<_> = hs.iter().map(|(n, v)| format!("{n}: {v}")).collect();
        assert_eq!(
            hs,
            [
                "x-y: z",
                "te: trailers",
                "cookie: a=1; b=2",
                "host: example.test"
            ]
        );
        // the id: connection 1, seq 0
        assert_eq!(
            u64::from_le_bytes(c.recs[0][..8].try_into().unwrap()),
            1 << 32
        );
        // a host header wins over :authority; a raw byte in the path is escaped
        let mut b = Vec::new();
        for (n, v) in [
            (":method", &b"GET"[..]),
            (":scheme", b"http"),
            (":path", b"/\xc3\xa9"),
            (":authority", b"a"),
            ("host", b"h"),
        ] {
            encode_field(n.as_bytes(), v, &mut b);
        }
        c.send(
            frame::HEADERS,
            frame::END_STREAM | frame::END_HEADERS,
            3,
            &b,
        );
        let (_, _, t, hs) = record(&c.recs[1]);
        assert_eq!(t, "/%C3%A9");
        assert_eq!(hs, [("host".to_string(), "h".to_string())]);
    }

    #[test]
    fn headers_with_continuation() {
        let mut c = Client::new(250);
        let b = block(GET);
        assert!(
            c.send(frame::HEADERS, frame::END_STREAM, 1, &b[..3])
                .is_empty()
        );
        // nothing else may come in between
        let mut d = Client::new(250);
        d.send(frame::HEADERS, frame::END_STREAM, 1, &b[..3]);
        goaway(
            &d.send(frame::PING, 0, 0, b"12345678"),
            frame::PROTOCOL_ERROR,
        );
        assert!(c.send(frame::CONTINUATION, 0, 1, &b[3..5]).is_empty());
        assert!(
            c.send(frame::CONTINUATION, frame::END_HEADERS, 1, &b[5..])
                .is_empty()
        );
        assert_eq!(c.recs.len(), 1);
        // a CONTINUATION with no HEADERS
        goaway(
            &c.send(frame::CONTINUATION, frame::END_HEADERS, 3, &b),
            frame::PROTOCOL_ERROR,
        );
    }

    #[test]
    fn request_checks() {
        let bad: &[&[(&str, &str)]] = &[
            &[(":method", "GET"), (":scheme", "http")],
            &[(":method", "GET"), (":path", "/")],
            &[(":scheme", "http"), (":path", "/")],
            &[
                (":method", "GET"),
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                ("x", "1"),
                (":path", "/"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                (":status", "200"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                (":foo", "1"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                ("X-Up", "1"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                ("connection", "close"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                ("upgrade", "x"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                ("transfer-encoding", "chunked"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                ("te", "gzip"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                ("x", "a\r\nb"),
            ],
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                ("x", " a"),
            ],
            &[(":method", "GET"), (":scheme", "http"), (":path", "/a b")],
            &[(":method", "GET"), (":scheme", "http"), (":path", "")],
            // a body length, but END_STREAM is on HEADERS
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                ("content-length", "5"),
            ],
        ];
        for (i, fields) in bad.iter().enumerate() {
            let mut c = Client::new(250);
            let id = 1;
            let fs = c.get(id, fields);
            assert_eq!(kinds(&fs), rst(id, 0), "{fields:?}");
            assert_eq!(code(&fs), frame::PROTOCOL_ERROR, "case {i}");
            assert!(c.recs.is_empty());
            // the connection goes on
            assert!(c.get(3, GET).is_empty());
            assert_eq!(c.recs.len(), 1);
        }
        let mut c = Client::new(250);
        assert!(
            c.get(1, &[GET, &[("content-length", "0")]].concat())
                .is_empty()
        );
        // CONNECT gets 405, too many headers 431
        let fs = c.get(3, &[(":method", "CONNECT"), (":authority", "a:1")]);
        assert_eq!(
            kinds(&fs),
            vec![(frame::HEADERS, frame::END_STREAM | frame::END_HEADERS, 3)]
        );
        assert_eq!(decode(&fs[0].1)[0], (":status".into(), "405".into()));
        let many: Vec<(String, String)> = (0..=MAX_HEADERS)
            .map(|i| (format!("x{i}"), "v".into()))
            .collect();
        let many: Vec<(&str, &str)> = many.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
        let fs = c.get(5, &[GET, &many].concat());
        assert_eq!(decode(&fs[0].1)[0], (":status".into(), "431".into()));
        assert_eq!(c.recs.len(), 1);
    }

    #[test]
    fn an_hpack_error_closes_the_connection() {
        let mut c = Client::new(250);
        // index 70 is past the end of the table
        let fs = c.send(
            frame::HEADERS,
            frame::END_STREAM | frame::END_HEADERS,
            1,
            &[0x80 | 70],
        );
        goaway(&fs, frame::COMPRESSION_ERROR);
    }

    #[test]
    fn stream_states() {
        let mut c = Client::new(250);
        goaway(&c.get(2, GET), frame::PROTOCOL_ERROR); // even id
        let mut c = Client::new(250);
        c.get(5, GET);
        assert_eq!(c.h2.state(5), State::Open);
        assert_eq!(c.h2.state(3), State::Closed);
        assert_eq!(c.h2.state(7), State::Idle);
        // DATA after END_STREAM: a stream error, Dart hears it is gone
        let fs = c.send(frame::DATA, 0, 5, b"x");
        assert_eq!(code(&fs), frame::STREAM_CLOSED);
        assert_eq!(c.h2.gone, vec![0]);
        assert_eq!(c.h2.state(5), State::Closed);
        // DATA on a closed stream is ignored, on an idle one it is an error
        assert!(c.send(frame::DATA, 0, 5, b"x").is_empty());
        goaway(&c.send(frame::DATA, 0, 7, b"x"), frame::PROTOCOL_ERROR);
        // a lower id than one used before
        let mut c = Client::new(250);
        c.get(5, GET);
        goaway(&c.get(3, GET), frame::STREAM_CLOSED);
        // RST_STREAM on an idle stream, PUSH_PROMISE from a client
        let mut c = Client::new(250);
        goaway(
            &c.send(frame::RST_STREAM, 0, 1, &[0; 4]),
            frame::PROTOCOL_ERROR,
        );
        let mut c = Client::new(250);
        goaway(
            &c.send(frame::PUSH_PROMISE, frame::END_HEADERS, 1, &[0, 0, 0, 2]),
            frame::PROTOCOL_ERROR,
        );
    }

    #[test]
    fn a_client_reset_makes_the_request_gone() {
        let mut c = Client::new(250);
        c.get(1, GET);
        c.get(3, GET);
        assert!(
            c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes())
                .is_empty()
        );
        assert_eq!(c.h2.gone, vec![0]);
        assert_eq!(c.h2.open(), 1);
        // a late answer goes nowhere
        assert!(c.answer(0, 200, b"", b"late").is_empty());
        assert_eq!(
            kinds(&c.answer(1, 200, b"", b"")),
            vec![(frame::HEADERS, 5, 3)]
        );
        assert_eq!(c.h2.open(), 0);
    }

    #[test]
    fn answer_headers() {
        let mut c = Client::new(250);
        c.get(1, GET);
        let lines =
            b"Content-Type: text/plain\r\nconnection: close\r\nx-a: 1\r\ncontent-length: 99\r\n";
        let fs = c.answer(0, 200, lines, b"hello");
        assert_eq!(
            kinds(&fs),
            vec![
                (frame::HEADERS, frame::END_HEADERS, 1),
                (frame::DATA, frame::END_STREAM, 1)
            ]
        );
        let hs: Vec<_> = decode(&fs[0].1)
            .iter()
            .map(|(n, v)| format!("{n}: {v}"))
            .collect();
        assert_eq!(
            hs,
            [
                ":status: 200",
                "content-type: text/plain",
                "x-a: 1",
                "content-length: 5",
                "date: Thu, 01 Jan 2026 00:00:00 GMT"
            ]
        );
        assert_eq!(fs[1].1, b"hello");
        // HEAD, 204 and an empty body: END_STREAM on HEADERS, no DATA
        let mut c = Client::new(250);
        c.get(
            1,
            &[(":method", "HEAD"), (":scheme", "http"), (":path", "/")],
        );
        c.get(3, GET);
        c.get(5, GET);
        let end = frame::END_STREAM | frame::END_HEADERS;
        assert_eq!(
            kinds(&c.answer(0, 200, b"", b"body")),
            vec![(frame::HEADERS, end, 1)]
        );
        let fs = c.answer(1, 204, b"", b"");
        assert_eq!(kinds(&fs), vec![(frame::HEADERS, end, 3)]);
        assert!(!decode(&fs[0].1).iter().any(|(n, _)| n == "content-length"));
        assert_eq!(
            kinds(&c.answer(2, 200, b"", b"")),
            vec![(frame::HEADERS, end, 5)]
        );
        assert_eq!(c.h2.open(), 0);
    }

    #[test]
    fn send_windows() {
        let mut c = Client::new(250);
        c.get(1, GET);
        let body = vec![7u8; 100_000];
        let fs = c.answer(0, 200, b"", &body);
        // the default windows: 65535 bytes in frames of 16384
        let sent: usize = fs
            .iter()
            .filter(|f| f.0.kind == frame::DATA)
            .map(|f| f.1.len())
            .sum();
        assert_eq!(sent, 65_535);
        assert!(fs.iter().all(|f| f.1.len() <= 16_384));
        assert!(!c.h2.can_send());
        // the connection window alone is not enough
        c.send(frame::WINDOW_UPDATE, 0, 0, &100_000u32.to_be_bytes());
        assert!(!c.h2.can_send());
        c.send(frame::WINDOW_UPDATE, 0, 1, &10u32.to_be_bytes());
        let fs = c.pump();
        assert_eq!(fs.iter().map(|f| f.1.len()).sum::<usize>(), 10);
        // a bigger INITIAL_WINDOW_SIZE raises the open streams too
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\x10\0\0");
        let fs = c.pump();
        let last = fs.last().unwrap();
        assert_eq!(last.0.flags, frame::END_STREAM);
        assert_eq!(
            fs.iter().map(|f| f.1.len()).sum::<usize>(),
            100_000 - 65_545
        );
        assert_eq!(c.h2.open(), 0);
        // a 0 increment, and a window over 2^31 - 1
        let mut c = Client::new(250);
        c.get(1, GET);
        assert_eq!(
            code(&c.send(frame::WINDOW_UPDATE, 0, 1, &[0; 4])),
            frame::PROTOCOL_ERROR
        );
        c.get(3, GET);
        let fs = c.send(frame::WINDOW_UPDATE, 0, 3, &0x7fff_ffffu32.to_be_bytes());
        assert_eq!(code(&fs), frame::FLOW_CONTROL_ERROR);
        assert_eq!(fs[0].0.kind, frame::RST_STREAM);
        goaway(
            &c.send(frame::WINDOW_UPDATE, 0, 0, &[0; 4]),
            frame::PROTOCOL_ERROR,
        );
        let mut c = Client::new(250);
        goaway(
            &c.send(frame::WINDOW_UPDATE, 0, 0, &0x7fff_ffffu32.to_be_bytes()),
            frame::FLOW_CONTROL_ERROR,
        );
        let mut c = Client::new(250);
        goaway(
            &c.send(frame::SETTINGS, 0, 0, b"\0\x04\x80\0\0\0"),
            frame::FLOW_CONTROL_ERROR,
        );
    }

    #[test]
    fn a_bigger_max_frame_size_and_a_smaller_window() {
        let mut c = Client::new(250);
        // MAX_FRAME_SIZE 20000, and INITIAL_WINDOW_SIZE 0
        c.send(frame::SETTINGS, 0, 0, b"\0\x05\0\0\x4e\x20\0\x04\0\0\0\0");
        c.send(frame::WINDOW_UPDATE, 0, 0, &1_000_000u32.to_be_bytes());
        c.get(1, GET);
        let fs = c.answer(0, 200, b"", &[1u8; 50_000]);
        assert_eq!(
            kinds(&fs),
            vec![(frame::HEADERS, frame::END_HEADERS, 1)],
            "no window"
        );
        c.send(frame::WINDOW_UPDATE, 0, 1, &60_000u32.to_be_bytes());
        let lens: Vec<_> = c.pump().iter().map(|f| f.1.len()).collect();
        // frames stay at 16 KiB, so one frame never breaks HOLD
        assert_eq!(lens, [16_384, 16_384, 16_384, 848]);
        // a MAX_FRAME_SIZE out of range
        goaway(
            &c.send(frame::SETTINGS, 0, 0, b"\0\x05\0\0\x10\0"),
            frame::PROTOCOL_ERROR,
        );
    }

    #[test]
    fn streams_take_turns() {
        let mut c = Client::new(250);
        c.send(frame::WINDOW_UPDATE, 0, 0, &10_000_000u32.to_be_bytes());
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\x10\0\0");
        c.get(1, GET);
        c.get(3, GET);
        c.get(5, GET);
        let mut out = Vec::new();
        c.h2.answer(0, 200, b"", &[1; 40_000], DATE, &mut out);
        c.h2.answer(1, 200, b"", &[2; 40_000], DATE, &mut out);
        c.h2.answer(2, 200, b"", b"small", DATE, &mut out);
        c.send_into(&mut out, 0);
        // HOLD is full after 4 frames; once written, the rest goes
        let written = out.len();
        c.send_into(&mut out, written);
        let ids: Vec<_> = frames(&out)
            .iter()
            .filter(|f| f.0.kind == frame::DATA)
            .map(|f| f.0.id)
            .collect();
        assert_eq!(ids, [1, 3, 5, 1, 3, 1, 3]);
        // the write buffer holds at most HOLD answer bytes before a write
        let mut c = Client::new(250);
        c.send(frame::WINDOW_UPDATE, 0, 0, &10_000_000u32.to_be_bytes());
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\x10\0\0");
        c.get(1, GET);
        let fs = c.answer(0, 200, b"", &[1; 400_000]);
        let sent: usize = fs.iter().map(|f| f.1.len()).sum();
        assert!(sent < HOLD + 16_384 + 100, "{sent}");
        assert!(c.h2.can_send());
    }

    #[test]
    fn max_concurrent_streams() {
        let mut c = Client::new(2);
        c.get(1, GET);
        c.get(3, GET);
        let fs = c.get(5, GET);
        assert_eq!(kinds(&fs), rst(5, 0));
        assert_eq!(code(&fs), frame::REFUSED_STREAM);
        assert_eq!(c.recs.len(), 2);
        c.answer(0, 200, b"", b"");
        assert!(c.get(7, GET).is_empty(), "room again");
        assert_eq!(c.recs.len(), 3);
    }

    #[test]
    fn graceful_drain() {
        let mut c = Client::new(250);
        c.get(1, GET);
        c.get(3, GET);
        let mut out = Vec::new();
        c.h2.drain(&mut out);
        let fs = frames(&out);
        goaway(&fs, frame::NO_ERROR);
        assert_eq!(fs[0].1[..4], 3u32.to_be_bytes(), "the last stream id");
        // new streams are ignored, open ones finish
        assert!(c.get(5, GET).is_empty());
        assert_eq!(c.recs.len(), 2);
        assert!(!c.h2.over());
        c.answer(0, 200, b"", b"a");
        c.answer(1, 200, b"", b"b");
        assert!(c.h2.over());
        // a GOAWAY from the client works the same
        let mut c = Client::new(250);
        c.get(1, GET);
        c.send(frame::GOAWAY, 0, 0, &[0; 8]);
        assert!(!c.h2.over());
        c.answer(0, 200, b"", b"");
        assert!(c.h2.over());
    }

    /// A big peer MAX_FRAME_SIZE must not let one frame break HOLD.
    #[test]
    fn a_big_peer_frame_size_keeps_hold() {
        let mut c = Client::new(250);
        // MAX_FRAME_SIZE 1 MiB
        c.send(frame::SETTINGS, 0, 0, b"\0\x05\0\x10\0\0");
        c.send(frame::WINDOW_UPDATE, 0, 0, &10_000_000u32.to_be_bytes());
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\x10\0\0");
        c.get(1, GET);
        let fs = c.answer(0, 200, b"", &[1; 600_000]);
        assert!(fs.iter().all(|f| f.1.len() <= 16_384));
        let sent: usize = fs.iter().map(|f| f.1.len()).sum();
        assert!(sent < HOLD + 16_384 + 100, "{sent}");
    }

    /// A window that stays 0 with data waiting: `blocked`, so the
    /// connection gets the SEND_WAIT timer. Progress ends it.
    #[test]
    fn a_closed_window_is_blocked() {
        let mut c = Client::new(250);
        assert!(!c.h2.blocked());
        c.get(1, GET);
        c.answer(0, 200, b"", &vec![7u8; 100_000]);
        assert!(c.h2.blocked(), "the stream window is used up");
        c.send(frame::WINDOW_UPDATE, 0, 1, &100_000u32.to_be_bytes());
        assert!(c.h2.blocked(), "the connection window is used up");
        c.send(frame::WINDOW_UPDATE, 0, 0, &100_000u32.to_be_bytes());
        assert!(!c.h2.blocked());
        c.pump();
        assert!(!c.h2.blocked(), "all sent");
    }

    /// GET then RST cycles with a closed window must not grow `turns`.
    #[test]
    fn dropped_streams_leave_the_turns() {
        let mut c = Client::new(250);
        // the connection window is used up by the first answer
        c.get(1, GET);
        c.answer(0, 200, b"", &vec![7u8; 100_000]);
        assert!(!c.h2.can_send());
        for i in 0..50u32 {
            let id = 3 + 2 * i;
            c.get(id, GET);
            c.answer(i + 1, 200, b"", b"body");
            c.send(frame::RST_STREAM, 0, id, &8u32.to_be_bytes());
        }
        assert!(c.h2.turns.len() <= 1, "{}", c.h2.turns.len());
    }

    /// GOAWAY may not carry a higher last stream id than an earlier one
    /// (RFC 9113 6.8).
    #[test]
    fn goaway_last_id_never_rises() {
        let mut c = Client::new(250);
        c.get(1, GET);
        let mut out = Vec::new();
        c.h2.drain(&mut out);
        let fs = frames(&out);
        assert_eq!(fs[0].1[..4], 1u32.to_be_bytes());
        // a new stream after the drain is ignored, so it does not count
        assert!(c.get(3, GET).is_empty());
        let fs = c.send(frame::SETTINGS, 0, 0, b"\0\x05\0\0\x10\0");
        assert_eq!(fs[0].1[..4], 1u32.to_be_bytes());
        goaway(&fs, frame::PROTOCOL_ERROR);
        // a refused stream and a stream with an HPACK error: same
        let mut c = Client::new(1);
        c.get(1, GET);
        assert_eq!(code(&c.get(3, GET)), frame::REFUSED_STREAM);
        let fs = c.send(
            frame::HEADERS,
            frame::END_STREAM | frame::END_HEADERS,
            5,
            &[0x80 | 70],
        );
        goaway(&fs, frame::COMPRESSION_ERROR);
        assert_eq!(fs[0].1[..4], 1u32.to_be_bytes());
    }

    /// No frame is written after a GOAWAY with an error, not even DATA.
    #[test]
    fn no_data_after_a_goaway_error() {
        let mut c = Client::new(250);
        c.send(frame::WINDOW_UPDATE, 0, 0, &1_000_000u32.to_be_bytes());
        c.get(1, GET);
        let mut out = Vec::new();
        c.h2.answer(0, 200, b"", &[1; 100_000], DATE, &mut out);
        out.clear();
        goaway(
            &c.send(frame::PING, 0, 1, b"12345678"),
            frame::PROTOCOL_ERROR,
        );
        assert!(!c.h2.can_send() && !c.h2.blocked());
        c.send_into(&mut out, 0);
        assert!(out.is_empty());
    }

    /// A body over the limit gets 413 and RST_STREAM. Trailers that were
    /// already on their way are ignored, and the connection goes on.
    #[test]
    fn frames_after_our_reset_are_ignored() {
        let mut c = Client::with_max(250, 1);
        let fs = c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(5));
        assert_eq!(code(&fs), frame::NO_ERROR);
        let trailers = block(&[("x-t", "1")]);
        let flags = frame::END_STREAM | frame::END_HEADERS;
        assert!(c.send(frame::HEADERS, flags, 1, &trailers).is_empty());
        assert!(!c.h2.over());
        // the same with the header block in two parts
        assert!(c.send(frame::HEADERS, 0, 1, &trailers).is_empty());
        assert!(
            c.send(frame::CONTINUATION, frame::END_HEADERS, 1, b"")
                .is_empty()
        );
        // a refused stream too
        let mut c = Client::new(1);
        c.get(1, GET);
        assert_eq!(code(&c.get(3, GET)), frame::REFUSED_STREAM);
        assert!(c.send(frame::HEADERS, flags, 3, &trailers).is_empty());
        // the next stream works
        c.answer(0, 200, b"", b"");
        assert!(c.get(5, GET).is_empty());
        assert_eq!(c.recs.len(), 2);
        // a stream that ended normally: HEADERS on it is still an error
        let mut c = Client::new(250);
        c.get(1, GET);
        c.answer(0, 200, b"", b"");
        goaway(
            &c.send(frame::HEADERS, flags, 1, &trailers),
            frame::STREAM_CLOSED,
        );
    }

    /// The ids of up to `max_streams` resets are remembered.
    #[test]
    fn the_reset_memory_is_bounded() {
        let mut c = Client::with_max(250, 1);
        for i in 0..500u32 {
            // refused HEADERS are no work: some real work in between
            let b = if i % 50 == 49 { block(GET) } else { post(5) };
            c.send(frame::HEADERS, frame::END_HEADERS, 1 + 2 * i, &b);
        }
        assert_eq!(c.h2.open(), 10);
        assert_eq!(c.h2.resets.len(), 250);
    }

    /// A header list over the cap is a 431 for that stream only. The
    /// table stays in sync, so the next stream works.
    #[test]
    fn a_huge_header_list_gets_431_on_its_stream() {
        let mut c = Client::new(250);
        // x-big: 3000 bytes, added to the table as index 62
        let mut b = block(GET);
        b.extend([0x40, 5]);
        b.extend(b"x-big");
        write_int(3000, 7, 0, &mut b);
        b.extend(vec![b'a'; 3000]);
        assert!(c.get_block(1, &b).is_empty());
        // 7 x 3000 bytes, over 16 KiB, in a few bytes
        let mut b = block(GET);
        b.extend([0xbe; 7]);
        let fs = c.get_block(3, &b);
        assert_eq!(decode(&fs[0].1)[0], (":status".into(), "431".into()));
        assert!(!c.h2.over());
        assert_eq!(c.recs.len(), 1);
        // the table still works
        let mut b = block(GET);
        b.push(0xbe);
        assert!(c.get_block(5, &b).is_empty());
        assert_eq!(c.recs.len(), 2);
    }

    // ---- request bodies, receive windows, streamed answers ----

    /// DATA frames: (stream id, payload).
    fn data_frames(fs: &[(Head, Vec<u8>)]) -> Vec<(u32, Vec<u8>)> {
        fs.iter()
            .filter(|f| f.0.kind == frame::DATA)
            .map(|f| (f.0.id, f.1.clone()))
            .collect()
    }

    /// The increments of the WINDOW_UPDATE frames: (stream id, increment).
    fn updates(fs: &[(Head, Vec<u8>)]) -> Vec<(u32, u32)> {
        fs.iter()
            .filter(|f| f.0.kind == frame::WINDOW_UPDATE)
            .map(|f| (f.0.id, u32::from_be_bytes(f.1[..4].try_into().unwrap())))
            .collect()
    }

    /// A DATA payload with PADDED: `data`, then `pad` zero bytes.
    fn padded(data: &[u8], pad: u8) -> Vec<u8> {
        let mut p = vec![pad];
        p.extend_from_slice(data);
        p.extend(vec![0; usize::from(pad)]);
        p
    }

    #[test]
    fn a_post_body_becomes_the_record() {
        let mut c = Client::new(250);
        let mut fields = GET.to_vec();
        fields[0] = (":method", "POST");
        assert!(
            c.send(frame::HEADERS, frame::END_HEADERS, 1, &block(&fields))
                .is_empty()
        );
        assert!(c.recs.is_empty(), "Dart waits for the whole body");
        assert!(c.send(frame::DATA, 0, 1, b"hel").is_empty());
        let p = padded(b"lo", 3);
        let flags = frame::END_STREAM | frame::PADDED;
        assert!(c.send(frame::DATA, flags, 1, &p).is_empty());
        let (flags, m, t, _) = record(&c.recs[0]);
        assert_eq!((flags, m.as_str(), t.as_str()), (8, "POST", "/a?b"));
        assert_eq!(c.bodies[0], b"hello");
        // an empty DATA with END_STREAM ends a body too
        c.send(frame::HEADERS, frame::END_HEADERS, 3, &block(&fields));
        c.send(frame::DATA, frame::END_STREAM, 3, b"");
        assert_eq!(c.bodies[1], b"");
        // the answer works as for a GET
        let fs = c.answer(0, 200, b"", b"ok");
        assert_eq!(data_frames(&fs), [(1, b"ok".to_vec())]);
    }

    /// WINDOW_UPDATE once half of a window is used, padding included.
    /// None for a stream after END_STREAM.
    #[test]
    fn receive_windows_go_back_at_half() {
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(10 << 20));
        // 15 bytes of body + 1 pad length byte + 240 padding: 256 of flow
        let p = padded(&[1; 15], 240);
        let (mut flow, mut sent) = (0i64, Vec::new());
        while sent.is_empty() {
            flow += 256;
            sent = updates(&c.send(frame::DATA, frame::PADDED, 1, &p));
        }
        // the stream window went under half: all of it comes back
        assert!(
            flow > (1 << 20) / 2 && flow - 256 <= (1 << 20) / 2,
            "{flow}"
        );
        assert_eq!(sent, [(1, flow as u32)]);
        // the connection window (16 MiB) goes back at half too
        let big = vec![0u8; 16_384];
        let mut conn = Vec::new();
        while conn.is_empty() {
            flow += big.len() as i64;
            let fs = c.send(frame::DATA, 0, 1, &big);
            conn = updates(&fs).into_iter().filter(|u| u.0 == 0).collect();
        }
        assert!(flow > CONN_WINDOW / 2, "{flow}");
        assert_eq!(conn, [(0, flow as u32)]);
        // the last DATA goes past half: no stream update after END_STREAM
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(33 * 16_384));
        let mut fs = Vec::new();
        for i in 0..33 {
            let end = if i == 32 { frame::END_STREAM } else { 0 };
            fs.extend(c.send(frame::DATA, end, 1, &big));
        }
        assert!(updates(&fs).is_empty(), "{:?}", updates(&fs));
        assert_eq!(c.bodies[0].len(), 33 * 16_384);
    }

    /// DATA over a window: RST_STREAM for the stream window, GOAWAY for
    /// the connection window. Padding counts.
    #[test]
    fn data_over_a_window_is_a_flow_control_error() {
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(100));
        c.h2.streams
            .get_mut(&1)
            .unwrap()
            .recv
            .as_mut()
            .unwrap()
            .window = 10;
        // all of it may come; then the window goes back
        assert_eq!(
            updates(&c.send(frame::DATA, 0, 1, &[1; 10])),
            [(1, 1 << 20)]
        );
        c.h2.streams
            .get_mut(&1)
            .unwrap()
            .recv
            .as_mut()
            .unwrap()
            .window = 10;
        let fs = c.send(frame::DATA, frame::PADDED, 1, &padded(&[1; 5], 5));
        assert_eq!(kinds(&fs), rst(1, 0));
        assert_eq!(code(&fs), frame::FLOW_CONTROL_ERROR);
        assert!(!c.h2.over());
        // the connection window
        c.send(frame::HEADERS, frame::END_HEADERS, 3, &post(100));
        c.h2.recv = 10;
        let fs = c.send(frame::DATA, frame::PADDED, 3, &padded(&[1; 5], 5));
        goaway(&fs, frame::FLOW_CONTROL_ERROR);
        assert!(c.h2.over());
    }

    /// A body over the limit gets 413 on its stream. Other streams and the
    /// connection go on. A multipart form gets the bigger limit.
    #[test]
    fn a_body_over_the_limit_gets_413_on_its_stream() {
        let mut c = Client::with_max(250, 100);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &block(GET));
        c.get(3, GET);
        // content-length over the limit: 413 before the body comes
        let fs = c.send(frame::HEADERS, frame::END_HEADERS, 5, &post(101));
        assert_eq!(
            kinds(&fs),
            vec![(frame::HEADERS, 5, 5), (frame::RST_STREAM, 0, 5)]
        );
        assert_eq!(decode(&fs[0].1)[0], (":status".into(), "413".into()));
        assert_eq!(code(&fs), frame::NO_ERROR, "the client keeps the 413");
        // no content-length: 413 once the bytes go over
        assert!(c.send(frame::DATA, 0, 1, &[1; 60]).is_empty());
        let fs = c.send(frame::DATA, 0, 1, &[1; 41]);
        assert_eq!(decode(&fs[0].1)[0], (":status".into(), "413".into()));
        assert_eq!(code(&fs), frame::NO_ERROR);
        // its DATA on the way is ignored; stream 3 still gets its answer
        assert!(
            c.send(frame::DATA, frame::END_STREAM, 1, &[1; 60])
                .is_empty()
        );
        assert_eq!(c.recs.len(), 1);
        assert_eq!(c.h2.open(), 1);
        assert_eq!(
            data_frames(&c.answer(1, 200, b"", b"x")),
            [(3, b"x".to_vec())]
        );
        assert!(c.h2.gone.is_empty(), "Dart never knew stream 1 or 5");
        // a multipart form may have 200
        let form = block(&[
            (":method", "POST"),
            (":scheme", "http"),
            (":path", "/"),
            ("content-type", "Multipart/Form-Data; boundary=x"),
            ("content-length", "150"),
        ]);
        c.send(frame::HEADERS, frame::END_HEADERS, 7, &form);
        assert!(
            c.send(frame::DATA, frame::END_STREAM, 7, &[2; 150])
                .is_empty()
        );
        assert_eq!(c.bodies[1].len(), 150);
        assert!(!c.h2.over());
    }

    /// The body timeout: 408 on the late stream only.
    #[test]
    fn a_slow_body_gets_408_on_its_stream() {
        let mut c = Client::new(250);
        let t0 = c.now;
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(10));
        c.get(3, GET);
        assert_eq!(c.h2.body_since(), Some(t0));
        // bytes move the wait
        c.now = t0 + Duration::from_secs(5);
        c.send(frame::DATA, 0, 1, b"abc");
        assert_eq!(c.h2.body_since(), Some(c.now));
        let timeout = Duration::from_secs(10);
        let mut out = Vec::new();
        c.h2.expire(
            t0 + Duration::from_secs(14),
            timeout,
            SEND_WAIT,
            DATE,
            &mut out,
        );
        assert!(out.is_empty());
        c.h2.expire(
            t0 + Duration::from_secs(15),
            timeout,
            SEND_WAIT,
            DATE,
            &mut out,
        );
        let fs = frames(&out);
        assert_eq!(decode(&fs[0].1)[0], (":status".into(), "408".into()));
        assert_eq!(kinds(&fs)[1], (frame::RST_STREAM, 0, 1));
        assert_eq!(c.h2.body_since(), None);
        assert_eq!(c.h2.open(), 1, "stream 3 goes on");
        assert!(c.h2.gone.is_empty());
    }

    /// content-length must match the DATA bytes (RFC 9113 8.1.1).
    #[test]
    fn content_length_must_match_the_data() {
        let cases: &[&[(u8, &[u8])]] = &[
            // more than declared
            &[(0, b"abcdef")],
            // fewer, then END_STREAM
            &[(0, b"ab"), (frame::END_STREAM, b"c")],
        ];
        for datas in cases {
            let mut c = Client::new(250);
            c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(5));
            let mut fs = Vec::new();
            for (flags, d) in *datas {
                fs = c.send(frame::DATA, *flags, 1, d);
            }
            assert_eq!(code(&fs), frame::PROTOCOL_ERROR);
            assert!(c.recs.is_empty() && !c.h2.over());
        }
        // fewer, then trailers
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(5));
        c.send(frame::DATA, 0, 1, b"ab");
        let flags = frame::END_STREAM | frame::END_HEADERS;
        let fs = c.send(frame::HEADERS, flags, 1, &block(&[("x-t", "1")]));
        assert_eq!(code(&fs), frame::PROTOCOL_ERROR);
        // two different lengths, or not a number
        for cl in [
            &[("content-length", "5"), ("content-length", "6")][..],
            &[("content-length", "5x")],
        ] {
            let mut c = Client::new(250);
            let fields = [
                &[(":method", "POST"), (":scheme", "http"), (":path", "/")][..],
                cl,
            ]
            .concat();
            let fs = c.send(frame::HEADERS, frame::END_HEADERS, 1, &block(&fields));
            assert_eq!(code(&fs), frame::PROTOCOL_ERROR, "{cl:?}");
        }
    }

    /// Request trailers end the body and are dropped. HPACK stays in sync.
    #[test]
    fn request_trailers() {
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(3));
        c.send(frame::DATA, 0, 1, b"abc");
        // x-t: 1, added to the HPACK table (index 62)
        let mut t = vec![0x40, 3];
        t.extend(b"x-t");
        t.extend([1, b'1']);
        let flags = frame::END_STREAM | frame::END_HEADERS;
        assert!(c.send(frame::HEADERS, flags, 1, &t).is_empty());
        let (_, _, _, hs) = record(&c.recs[0]);
        assert!(!hs.iter().any(|(n, _)| n == "x-t"), "trailers are dropped");
        assert_eq!(c.bodies[0], b"abc");
        // the table has the trailer
        let mut b = block(GET);
        b.push(0xbe);
        c.get_block(3, &b);
        let (_, _, _, hs) = record(&c.recs[1]);
        assert!(hs.contains(&("x-t".into(), "1".into())));
        // a pseudo header in trailers; trailers in two frames
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &block(GET));
        let fs = c.send(frame::HEADERS, flags, 1, &block(&[(":path", "/")]));
        assert_eq!(code(&fs), frame::PROTOCOL_ERROR);
        c.send(frame::HEADERS, frame::END_HEADERS, 3, &block(GET));
        let tr = block(&[("x-t", "1")]);
        assert!(
            c.send(frame::HEADERS, frame::END_STREAM, 3, &tr[..2])
                .is_empty()
        );
        assert!(c.recs.is_empty());
        let fs = c.send(frame::CONTINUATION, frame::END_HEADERS, 3, &tr[2..]);
        assert!(fs.is_empty());
        assert_eq!(c.recs.len(), 1);
        // HEADERS with no END_STREAM after DATA: malformed
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &block(GET));
        c.send(frame::DATA, 0, 1, b"abc");
        let fs = c.send(frame::HEADERS, frame::END_HEADERS, 1, &tr);
        assert_eq!(code(&fs), frame::PROTOCOL_ERROR);
        assert!(!c.h2.over());
    }

    /// DATA on a stream we reset: ignored, but it counts against the
    /// connection window. On a stream that ended: STREAM_CLOSED. On an
    /// idle one: PROTOCOL_ERROR.
    #[test]
    fn data_on_reset_closed_and_idle_streams() {
        let mut c = Client::with_max(250, 1);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(5));
        let big = vec![0u8; 16_384];
        let mut back = 0u64;
        for k in 0..600u32 {
            if k % 50 == 49 {
                // DATA on a reset stream is no work: some work in between
                c.get(3 + 2 * (k / 50), GET);
            }
            for (id, n) in updates(&c.send(frame::DATA, 0, 1, &big)) {
                assert_eq!(id, 0);
                back += u64::from(n);
            }
        }
        assert!(back >= 8 << 20, "{back}");
        assert!(!c.h2.over());
        // a stream that ended normally
        let mut c = Client::new(250);
        c.get(1, GET);
        c.answer(0, 200, b"", b"");
        goaway(&c.send(frame::DATA, 0, 1, b"x"), frame::STREAM_CLOSED);
        // an idle stream
        let mut c = Client::new(250);
        goaway(&c.send(frame::DATA, 0, 1, b"x"), frame::PROTOCOL_ERROR);
    }

    /// Writes Dart's kind 6 for `seq`, then what [H2::send] adds.
    fn start(c: &mut Client, seq: u32, lines: &[u8], first: &[u8]) -> (bool, Vec<(Head, Vec<u8>)>) {
        let mut out = Vec::new();
        let ok = c.h2.start(seq, 200, lines, SID, first, DATE, &mut out);
        c.send_into(&mut out, 0);
        (ok, frames(&out))
    }

    /// Dart's kind 7 or 8 for `seq`, then what [H2::send] adds.
    fn part(c: &mut Client, seq: u32, data: &[u8], end: bool, ok: bool) -> Vec<(Head, Vec<u8>)> {
        let mut out = Vec::new();
        c.h2.part(seq, data, end, ok, b"", &mut out);
        c.send_into(&mut out, 0);
        frames(&out)
    }

    const SID: u64 = 77;

    /// A streamed answer under a small window: the bytes wait, STREAM_SENT
    /// counts only what went out, END_STREAM comes after the last byte.
    #[test]
    fn a_streamed_answer_under_a_small_window() {
        let mut c = Client::new(250);
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\0\0\x0a"); // window 10
        c.get(1, GET);
        let (ok, fs) = start(&mut c, 0, b"content-type: text/plain\r\n", b"abcdef");
        assert!(ok);
        assert_eq!(kinds(&fs)[0], (frame::HEADERS, frame::END_HEADERS, 1));
        assert!(!decode(&fs[0].1).iter().any(|(n, _)| n == "content-length"));
        assert_eq!(data_frames(&fs), [(1, b"abcdef".to_vec())]);
        assert_eq!(c.sent(SID), 6);
        assert!(!c.h2.blocked(), "nothing waits");
        let fs = part(&mut c, 0, b"ghijkl", false, true);
        assert_eq!(data_frames(&fs), [(1, b"ghij".to_vec())]);
        assert_eq!(c.sent(SID), 4);
        assert!(c.h2.blocked(), "two bytes wait for the window");
        // the end waits behind them
        assert!(part(&mut c, 0, b"", true, true).is_empty());
        c.send(frame::WINDOW_UPDATE, 0, 1, &100u32.to_be_bytes());
        let fs = c.pump();
        assert_eq!(kinds(&fs), [(frame::DATA, frame::END_STREAM, 1)]);
        assert_eq!(fs[0].1, b"kl");
        assert_eq!(c.sent(SID), 2);
        assert_eq!(c.h2.open(), 0);
        assert!(c.h2.gone_streams.is_empty(), "it ended well");
        // all sent before the end: an empty DATA ends it
        c.get(3, GET);
        let (_, fs) = start(&mut c, 1, b"", b"ab");
        assert_eq!(data_frames(&fs), [(3, b"ab".to_vec())]);
        let fs = part(&mut c, 1, b"", true, true);
        assert_eq!(kinds(&fs), [(frame::DATA, frame::END_STREAM, 3)]);
        assert!(fs[0].1.is_empty());
    }

    /// Dart's content-length goes out and must match the parts.
    #[test]
    fn a_streamed_answer_with_a_length() {
        let mut c = Client::new(250);
        c.get(1, GET);
        let (_, fs) = start(&mut c, 0, b"Content-Length: 4\r\n", b"ab");
        assert!(decode(&fs[0].1).contains(&("content-length".into(), "4".into())));
        let fs = part(&mut c, 0, b"cd", true, true);
        assert_eq!(kinds(&fs), [(frame::DATA, frame::END_STREAM, 1)]);
        // more bytes than said, or fewer at the end: RST
        for (first, more, end) in [(&b"abc"[..], &b"de"[..], false), (b"ab", b"", true)] {
            let mut c = Client::new(250);
            c.get(1, GET);
            start(&mut c, 0, b"content-length: 4\r\n", first);
            let fs = part(&mut c, 0, more, end, true);
            assert_eq!(kinds(&fs), rst(1, 0));
            assert_eq!(code(&fs), frame::INTERNAL_ERROR);
            assert_eq!(c.h2.gone_streams, [SID]);
        }
    }

    /// A failed kind 8 resets the stream; the connection goes on.
    #[test]
    fn a_failed_end_resets_the_stream() {
        let mut c = Client::new(250);
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\0\0\x00"); // window 0
        c.get(1, GET);
        start(&mut c, 0, b"", b"waits");
        let fs = part(&mut c, 0, b"", true, false);
        assert_eq!(kinds(&fs), rst(1, 0));
        assert_eq!(code(&fs), frame::INTERNAL_ERROR);
        assert_eq!(c.h2.open(), 0);
        assert!(!c.h2.blocked() && !c.h2.over());
        assert!(c.get(3, GET).is_empty());
    }

    /// A client RST during a streamed answer: the stream is gone once,
    /// later parts go nowhere.
    #[test]
    fn a_client_reset_ends_a_streamed_answer() {
        let mut c = Client::new(250);
        c.get(1, GET);
        start(&mut c, 0, b"", b"abc");
        c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
        assert_eq!(c.h2.gone_streams, [SID]);
        assert!(c.h2.gone.is_empty(), "Dart answered it");
        assert!(part(&mut c, 0, b"more", false, true).is_empty());
        assert!(part(&mut c, 0, b"", true, true).is_empty());
        assert_eq!(c.h2.streamed().collect::<Vec<_>>(), [SID]);
        // HEAD, or a stream that is gone: no stream, Dart stops
        let (ok, fs) = start(&mut c, 0, b"", b"x");
        assert!(!ok && fs.is_empty());
        c.get(
            3,
            &[(":method", "HEAD"), (":scheme", "http"), (":path", "/")],
        );
        let (ok, fs) = start(&mut c, 1, b"", b"x");
        assert!(!ok);
        assert_eq!(
            kinds(&fs),
            [(frame::HEADERS, frame::END_STREAM | frame::END_HEADERS, 3)]
        );
    }

    /// More resets than 64 in one round trip: the DATA that was already
    /// on its way for the oldest one is ignored.
    /// After a 413 in the middle of an upload, a whole window of small
    /// DATA frames may still be in flight. They are not "empty" frames,
    /// so the connection is not cut.
    #[test]
    fn in_flight_data_after_a_413_does_not_cut() {
        let mut c = Client::with_max(250, 100);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &block(GET));
        let fs = c.send(frame::DATA, 0, 1, &[1; 101]);
        assert_eq!(decode(&fs[0].1)[0], (":status".into(), "413".into()));
        for _ in 0..500 {
            assert!(c.send(frame::DATA, 0, 1, &[1; 4]).is_empty());
        }
        assert!(!c.h2.over());
        assert!(c.get(3, GET).is_empty());
        assert_eq!(c.recs.len(), 1);
    }

    #[test]
    fn late_data_after_many_resets_is_ignored() {
        let mut c = Client::with_max(250, 1);
        for i in 0..200u32 {
            // refused HEADERS are no work: some real work in between
            let b = if i % 50 == 49 { block(GET) } else { post(5) };
            c.send(frame::HEADERS, frame::END_HEADERS, 1 + 2 * i, &b);
        }
        assert!(c.send(frame::DATA, 0, 1, b"x").is_empty());
        assert!(!c.h2.over());
    }

    /// A stream the client reset: the client sends nothing on it after
    /// its own reset, so DATA (or HEADERS) on it is a stream error
    /// STREAM_CLOSED (RFC 9113 5.1, h2spec 5.1/8 and 5.1/9). The
    /// connection stays up, and the HPACK table stays in sync.
    #[test]
    fn data_after_a_client_reset_is_a_stream_error() {
        for kind in [frame::DATA, frame::HEADERS] {
            let mut c = Client::new(250);
            c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(5));
            c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
            let (p, flags) = if kind == frame::DATA {
                (b"abc".to_vec(), 0)
            } else {
                (block(GET), frame::END_HEADERS)
            };
            let fs = c.send(kind, flags, 1, &p);
            assert_eq!(kinds(&fs), [(frame::RST_STREAM, 0, 1)]);
            assert_eq!(code(&fs), frame::STREAM_CLOSED);
            assert!(!c.h2.over());
            // the connection works on
            assert!(c.get(3, GET).is_empty());
            assert_eq!(c.recs.len(), 1);
        }
        // DATA still counts in the connection window
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(5));
        c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
        let before = c.h2.recv;
        c.send(frame::DATA, 0, 1, b"abc");
        assert_eq!(c.h2.recv, before - 3);
    }

    /// The memory of client resets is bounded, and RSTs on closed or
    /// idle ids add nothing to it.
    #[test]
    fn the_client_reset_memory_is_bounded() {
        let mut c = Client::with_max(250, MAX_BODY);
        for i in 0..300u32 {
            c.get(1 + 2 * i, GET);
            c.send(frame::RST_STREAM, 0, 1 + 2 * i, &8u32.to_be_bytes());
        }
        assert_eq!(c.h2.client_resets.len(), 250);
        // closed ids: nothing is added
        for _ in 0..50 {
            c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
        }
        assert_eq!(c.h2.client_resets.len(), 250);
        assert_eq!(*c.h2.client_resets.back().unwrap(), 599);
    }

    /// All bodies of one connection together stay under the bound
    /// (BODY_STREAMS_CAP times the body limit): the stream that goes over
    /// it is refused (RST_STREAM REFUSED_STREAM, no 413), and the bytes
    /// are given back when a stream goes.
    #[test]
    fn the_bodies_of_a_connection_have_a_bound() {
        let mut c = Client::with_max(250, 1000); // the bound is 4 * 2000
        let ids: Vec<u32> = (0..9).map(|i| 1 + 2 * i).collect();
        for &id in &ids {
            c.send(frame::HEADERS, frame::END_HEADERS, id, &post(1000));
        }
        for &id in &ids[..8] {
            assert!(c.send(frame::DATA, 0, id, &[0; 900]).is_empty());
        }
        let fs = c.send(frame::DATA, 0, 17, &[0; 900]);
        assert_eq!(kinds(&fs), [(frame::RST_STREAM, 0, 17)]);
        assert_eq!(code(&fs), frame::REFUSED_STREAM);
        assert_eq!(c.h2.buffered, 7200);
        assert!(!c.h2.over());
        // late DATA of the refused stream is ignored
        assert!(c.send(frame::DATA, 0, 17, &[0; 10]).is_empty());
        // a stream that ends hands its body over: the bytes are free
        assert!(
            c.send(frame::DATA, frame::END_STREAM, 1, &[0; 100])
                .is_empty()
        );
        assert_eq!(c.h2.buffered, 6300);
        for &id in &ids[1..8] {
            c.send(frame::RST_STREAM, 0, id, &8u32.to_be_bytes());
        }
        assert_eq!(c.h2.buffered, 0);
        // a retry fits now
        c.send(frame::HEADERS, frame::END_HEADERS, 19, &post(1000));
        assert!(c.send(frame::DATA, 0, 19, &[0; 900]).is_empty());
    }

    /// A streamed answer whose stream window stays 0 for SEND_WAIT is
    /// cancelled, while other streams go on. A WINDOW_UPDATE ends the wait.
    #[test]
    fn a_stream_window_of_0_cancels_the_stream() {
        let mut c = Client::new(250);
        let t0 = c.now;
        c.get(1, GET);
        c.get(3, GET);
        start(&mut c, 0, b"", &vec![1u8; 100_000]);
        c.h2.stall_mark(t0);
        assert_eq!(c.h2.stall_since(), Some(t0));
        let mut out = Vec::new();
        let wait = SEND_WAIT;
        c.h2.expire(
            t0 + wait - Duration::from_secs(1),
            wait,
            wait,
            DATE,
            &mut out,
        );
        assert!(out.is_empty());
        c.h2.expire(t0 + wait, wait, wait, DATE, &mut out);
        let fs = frames(&out);
        assert_eq!(kinds(&fs), [(frame::RST_STREAM, 0, 1)]);
        assert_eq!(code(&fs), frame::CANCEL);
        assert_eq!(c.h2.gone_streams, [SID]);
        assert_eq!(c.h2.stall_since(), None);
        assert_eq!(c.h2.open(), 1, "stream 3 goes on");
        // a WINDOW_UPDATE before the time: no cancel, and no timer left
        let mut c = Client::new(250);
        c.get(1, GET);
        start(&mut c, 0, b"", &vec![1u8; 100_000]);
        c.h2.stall_mark(t0);
        c.send(frame::WINDOW_UPDATE, 0, 1, &1000u32.to_be_bytes());
        let mut out = Vec::new();
        c.h2.expire(t0 + wait, wait, wait, DATE, &mut out);
        assert!(out.is_empty());
        assert_eq!(c.h2.stall_since(), None);
    }

    /// A stream that drained and then stalls again gets a fresh clock.
    #[test]
    fn a_stall_after_a_drain_starts_a_fresh_clock() {
        let mut c = Client::new(250);
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\0\0\x0a"); // window 10
        let t0 = c.now;
        c.get(1, GET);
        start(&mut c, 0, b"", &[1u8; 20]);
        c.h2.stall_mark(t0);
        assert_eq!(c.h2.stall_since(), Some(t0));
        // the window opens: the rest goes out and the stream waits for Dart
        c.send(frame::WINDOW_UPDATE, 0, 1, &10u32.to_be_bytes());
        let mut out = Vec::new();
        c.send_into(&mut out, 0);
        // more bytes, a window of 10 again: a new stall
        c.send(frame::WINDOW_UPDATE, 0, 1, &10u32.to_be_bytes());
        part(&mut c, 0, &[2u8; 20], false, true);
        let t1 = t0 + Duration::from_secs(20);
        c.h2.stall_mark(t1);
        assert_eq!(c.h2.stall_since(), Some(t1));
    }

    /// Small WINDOW_UPDATEs are progress: the stream lives past SEND_WAIT.
    #[test]
    fn small_window_updates_keep_a_slow_stream_alive() {
        let mut c = Client::new(250);
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\0\0\x0a"); // window 10
        let t0 = c.now;
        c.get(1, GET);
        start(&mut c, 0, b"", &vec![1u8; 100_000]);
        c.h2.stall_mark(t0);
        let t1 = t0 + SEND_WAIT - Duration::from_secs(1);
        // exactly the gap: window 0 again at the next mark
        c.send(frame::WINDOW_UPDATE, 0, 1, &10u32.to_be_bytes());
        let mut out = Vec::new();
        c.send_into(&mut out, 0);
        c.h2.stall_mark(t1);
        let mut out = Vec::new();
        c.h2.expire(t0 + SEND_WAIT, SEND_WAIT, SEND_WAIT, DATE, &mut out);
        assert!(out.is_empty(), "bytes moved, so no cancel");
        assert_eq!(c.h2.stall_since(), Some(t1));
    }

    // ---- the attacks of plan 4.10: each one gets GOAWAY or its guard ----

    /// The time of the client, a whole window later: the rate counts start
    /// again.
    fn next_second(c: &mut Client) {
        c.now += Duration::from_secs(1);
    }

    /// Rapid Reset (CVE-2023-44487): HEADERS then RST_STREAM, again and
    /// again. The bucket holds 1000 (a burst), then 100 come back each
    /// second.
    #[test]
    fn attack_rapid_reset() {
        let mut c = Client::new(250);
        let rst = 8u32.to_be_bytes();
        for i in 0..1000u32 {
            c.get(1 + 2 * i, GET);
            assert!(c.send(frame::RST_STREAM, 0, 1 + 2 * i, &rst).is_empty());
        }
        c.get(2001, GET);
        goaway(
            &c.send(frame::RST_STREAM, 0, 2001, &rst),
            frame::ENHANCE_YOUR_CALM,
        );
        assert!(c.h2.over());
        // a flood that goes on at the sustained rate: 100 a second is fine
        let mut c = Client::new(250);
        for i in 0..1000u32 {
            c.get(1 + 2 * i, GET);
            c.send(frame::RST_STREAM, 0, 1 + 2 * i, &rst);
        }
        for s in 0..5u32 {
            next_second(&mut c);
            for i in 0..100u32 {
                let id = 2001 + 2 * (s * 100 + i);
                c.get(id, GET);
                assert!(c.send(frame::RST_STREAM, 0, id, &rst).is_empty());
            }
        }
        // 101 in one second is over the rate
        next_second(&mut c);
        for i in 0..100u32 {
            let id = 3001 + 2 * i;
            c.get(id, GET);
            c.send(frame::RST_STREAM, 0, id, &rst);
        }
        c.get(4001, GET);
        goaway(
            &c.send(frame::RST_STREAM, 0, 4001, &rst),
            frame::ENHANCE_YOUR_CALM,
        );
    }

    /// Sends `answered` streams that Dart answers with `status` and `body`,
    /// then one stream the client resets at once. `n` counts the streams.
    fn pair(
        c: &mut Client,
        n: &mut u32,
        answered: u32,
        status: u16,
        body: &[u8],
    ) -> Vec<(Head, Vec<u8>)> {
        for _ in 0..answered {
            c.get(1 + 2 * *n, GET);
            c.answer(*n, status, b"", body);
            *n += 1;
        }
        let id = 1 + 2 * *n;
        *n += 1;
        c.get(id, GET);
        c.send(frame::RST_STREAM, 0, id, &8u32.to_be_bytes())
    }

    /// After many answers, a flood is still cut within one burst.
    fn flood_is_cut(c: &mut Client, n: &mut u32) {
        let mut cut = None;
        for i in 0..=1100u32 {
            let id = 1 + 2 * *n;
            *n += 1;
            c.get(id, GET);
            let fs = c.send(frame::RST_STREAM, 0, id, &8u32.to_be_bytes());
            if !fs.is_empty() {
                goaway(&fs, frame::ENHANCE_YOUR_CALM);
                cut = Some(i);
                break;
            }
        }
        assert!(cut.is_some_and(|i| i >= 990), "{cut:?}");
    }

    /// A busy gRPC client: all calls on one connection, and some go over
    /// their deadline (a reset). Each answer with a body gives half a token
    /// back: with 3 answers per reset it is not cut. Only time refilled the
    /// bucket before: this cut it after 1000 resets.
    #[test]
    fn answered_streams_give_reset_tokens_back() {
        let mut c = Client::new(250);
        let mut n = 0;
        for _ in 0..1200 {
            assert!(pair(&mut c, &mut n, 3, 200, b"ok").is_empty());
        }
        assert!(!c.h2.over());
        // the tokens do not pile up over the burst: a flood after many
        // answers is still cut within one burst
        flood_is_cut(&mut c, &mut n);
    }

    /// One cheap answer (no body) per reset gives nothing back, so the
    /// attacker is cut within one burst.
    #[test]
    fn cheap_answers_give_no_reset_tokens() {
        for status in [204u16, 304, 200] {
            let mut c = Client::new(250);
            let mut n = 0;
            let mut cut = None;
            for i in 0..1100u32 {
                let fs = pair(&mut c, &mut n, 1, status, b"");
                if !fs.is_empty() {
                    goaway(&fs, frame::ENHANCE_YOUR_CALM);
                    cut = Some(i);
                    break;
                }
            }
            assert!(
                cut.is_some_and(|i| (990..1100).contains(&i)),
                "{status} {cut:?}"
            );
        }
    }

    /// One answer with a body per reset (net -0.5 a pair) is cut after
    /// about 2000 pairs. With a whole token back it was never cut.
    #[test]
    fn one_answer_per_reset_is_cut() {
        let mut c = Client::new(250);
        let mut n = 0;
        let mut cut = None;
        for i in 0..2500u32 {
            let fs = pair(&mut c, &mut n, 1, 200, b"ok");
            if !fs.is_empty() {
                goaway(&fs, frame::ENHANCE_YOUR_CALM);
                cut = Some(i);
                break;
            }
        }
        assert!(cut.is_some_and(|i| (1900..2100).contains(&i)), "{cut:?}");
    }

    /// A stream the client reset before Dart answered gets no refund.
    #[test]
    fn answer_of_a_reset_stream_gives_nothing() {
        let mut c = Client::new(250);
        c.get(1, GET);
        c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
        assert_eq!(c.h2.reset_bucket.tokens, 999.0);
        c.answer(0, 200, b"", b"ok");
        assert_eq!(c.h2.reset_bucket.tokens, 999.0);
    }

    /// HEADERS on a stream we reset are decoded and dropped: each takes a
    /// token. Work in between (a new stream) does not stop the cost.
    #[test]
    fn attack_headers_on_a_reset_stream() {
        let mut c = Client::with_max(250, 100);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &block(GET));
        let fs = c.send(frame::DATA, 0, 1, &[1; 101]);
        assert_eq!(decode(&fs[0].1)[0], (":status".into(), "413".into()));
        // one late trailers HEADERS is fine
        assert!(
            c.send(
                frame::HEADERS,
                frame::END_HEADERS | frame::END_STREAM,
                1,
                &block(&[("x-t", "1")])
            )
            .is_empty()
        );
        let mut sent = 0u32;
        let mut id = 3;
        'outer: for _ in 0..100 {
            for _ in 0..99 {
                sent += 1;
                if c.h2.over() {
                    break 'outer;
                }
                c.send(frame::HEADERS, frame::END_HEADERS, 1, &block(GET));
            }
            // work: a new stream, so the empty count starts again
            c.get(id, GET);
            id += 2;
        }
        assert!(c.h2.over());
        assert!(sent < 1300, "{sent}");
    }

    /// After our own GOAWAY a busy client still has many requests in
    /// flight: they are ignored, and the connection is not cut.
    #[test]
    fn many_headers_after_our_goaway_do_not_cut() {
        let mut c = Client::new(250);
        c.get(1, GET);
        let mut out = Vec::new();
        c.h2.drain(&mut out);
        for i in 0..300u32 {
            assert!(c.get(3 + 2 * i, GET).is_empty());
        }
        assert!(!c.h2.over());
        let fs = c.answer(0, 200, b"", b"a");
        assert!(!fs.is_empty());
        assert!(c.h2.over());
    }

    /// A client or proxy that cancels all its calls at once is not cut.
    #[test]
    fn a_mass_cancel_is_fine() {
        let mut c = Client::new(250);
        for i in 0..250u32 {
            assert!(c.get(1 + 2 * i, GET).is_empty());
        }
        assert_eq!(c.h2.open(), 250);
        for i in 0..250u32 {
            let fs = c.send(frame::RST_STREAM, 0, 1 + 2 * i, &8u32.to_be_bytes());
            assert!(fs.is_empty());
        }
        assert!(!c.h2.over());
        assert_eq!(c.h2.open(), 0);
    }

    /// "Made you reset" (CVE-2025-8671): each request is followed by a
    /// bad frame, so the server resets it. Those resets count too.
    #[test]
    fn attack_made_you_reset() {
        let mut c = Client::new(250);
        for i in 0..1000u32 {
            c.get(1 + 2 * i, GET);
            // a WINDOW_UPDATE of 0 on a stream is a stream error
            let fs = c.send(frame::WINDOW_UPDATE, 0, 1 + 2 * i, &[0; 4]);
            assert_eq!(code(&fs), frame::PROTOCOL_ERROR);
        }
        assert_eq!(c.recs.len(), 1000);
        c.get(2001, GET);
        goaway(
            &c.send(frame::WINDOW_UPDATE, 0, 2001, &[0; 4]),
            frame::ENHANCE_YOUR_CALM,
        );
        // frames on a stream the client reset: STREAM_CLOSED, also counted
        let mut c = Client::new(250);
        c.get(1, GET);
        c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
        for k in 0..999u32 {
            if k % 50 == 49 {
                c.get(3 + 2 * (k / 50), GET); // work: the empty count starts again
            }
            assert_eq!(code(&c.send(frame::DATA, 0, 1, b"x")), frame::STREAM_CLOSED);
        }
        goaway(&c.send(frame::DATA, 0, 1, b"x"), frame::ENHANCE_YOUR_CALM);
    }

    /// Refused HEADERS cost an HPACK decode: they take a token too. (Some
    /// real work in between keeps the empty count from stopping them.)
    #[test]
    fn attack_refused_headers() {
        let mut c = Client::with_max(1, MAX_BODY);
        // one open stream: each new HEADERS is REFUSED_STREAM
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(100_000));
        let mut n = 0u32;
        let fs = loop {
            n += 1;
            if n.is_multiple_of(50) {
                c.send(frame::DATA, 0, 1, b"x");
            }
            let fs = c.send(frame::HEADERS, frame::END_HEADERS, 1 + 2 * n, &block(GET));
            if code(&fs) != frame::REFUSED_STREAM {
                break fs;
            }
            assert!(n < 5000);
        };
        goaway(&fs, frame::ENHANCE_YOUR_CALM);
        assert_eq!(n, 1001);
        // with no work in between, the empty count stops them first
        let mut c = Client::with_max(1, MAX_BODY);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(100_000));
        for n in 1..=100u32 {
            let fs = c.send(frame::HEADERS, frame::END_HEADERS, 1 + 2 * n, &block(GET));
            assert_eq!(code(&fs), frame::REFUSED_STREAM);
        }
        let fs = c.send(frame::HEADERS, frame::END_HEADERS, 203, &block(GET));
        goaway(&fs, frame::ENHANCE_YOUR_CALM);
    }

    /// RST_STREAM on closed ids counts toward the limit, and does not push
    /// the ids we reset out of the reset memory.
    #[test]
    fn attack_rst_stream_on_closed_ids() {
        let mut c = Client::with_max(250, 1);
        // stream 1 gets 413 and our reset: its late DATA must be ignored
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(5));
        c.get(3, GET);
        c.answer(0, 200, b"", b"");
        assert_eq!(c.h2.resets.len(), 1);
        // the 413 took one token: 999 are left (an empty answer gives
        // nothing back)
        for _ in 0..999 {
            assert!(
                c.send(frame::RST_STREAM, 0, 3, &8u32.to_be_bytes())
                    .is_empty()
            );
        }
        assert_eq!(c.h2.resets.len(), 1, "client resets are not remembered");
        assert!(c.h2.client_resets.is_empty());
        assert!(c.send(frame::DATA, 0, 1, b"late").is_empty());
        goaway(
            &c.send(frame::RST_STREAM, 0, 3, &8u32.to_be_bytes()),
            frame::ENHANCE_YOUR_CALM,
        );
    }

    /// SETTINGS flood: at most 10 whose ACK is not written yet, and at
    /// most 100 a second.
    #[test]
    fn attack_settings_flood() {
        // the client does not read: the 11th unacked SETTINGS
        let mut c = Client::new(250);
        c.h2.flushed();
        for _ in 0..10 {
            assert_eq!(c.send(frame::SETTINGS, 0, 0, b"").len(), 1);
        }
        goaway(
            &c.send(frame::SETTINGS, 0, 0, b""),
            frame::ENHANCE_YOUR_CALM,
        );
        // the client reads, but sends more than 100 a second
        let mut c = Client::new(250);
        next_second(&mut c);
        for _ in 0..100 {
            c.h2.flushed();
            assert_eq!(c.send(frame::SETTINGS, 0, 0, b"").len(), 1);
        }
        c.h2.flushed();
        goaway(
            &c.send(frame::SETTINGS, 0, 0, b""),
            frame::ENHANCE_YOUR_CALM,
        );
    }

    /// PING flood: at most 100 a second, and at most 100 ACKs that are not
    /// written yet.
    #[test]
    fn attack_ping_flood() {
        let mut c = Client::new(250);
        for _ in 0..100 {
            c.h2.flushed();
            assert_eq!(c.send(frame::PING, 0, 0, b"12345678").len(), 1);
        }
        goaway(
            &c.send(frame::PING, 0, 0, b"12345678"),
            frame::ENHANCE_YOUR_CALM,
        );
        // slow PINGs, but the client never reads the ACKs
        let mut c = Client::new(250);
        for i in 0..100 {
            if i % 50 == 0 {
                next_second(&mut c);
            }
            assert_eq!(c.send(frame::PING, 0, 0, b"12345678").len(), 1);
        }
        next_second(&mut c);
        goaway(
            &c.send(frame::PING, 0, 0, b"12345678"),
            frame::ENHANCE_YOUR_CALM,
        );
    }

    /// grpc-go sends a PING each time DATA comes to it (its window
    /// estimate). DATA we send starts the PING count again, so a big
    /// download with many PINGs is not cut.
    #[test]
    fn pings_between_our_data_are_fine() {
        let mut c = Client::new(250);
        c.send(frame::WINDOW_UPDATE, 0, 0, &100_000_000u32.to_be_bytes());
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\x7f\xff\xff\xff");
        c.get(1, GET);
        c.answer(0, 200, b"", &vec![1u8; 1300 * 16_384]);
        for _ in 0..300 {
            c.h2.flushed();
            assert_eq!(c.send(frame::PING, 0, 0, b"12345678").len(), 1);
            let mut out = Vec::new();
            c.h2.send(&mut out, 0, 0, &mut VecDeque::new());
            assert!(!out.is_empty());
        }
        assert!(!c.h2.over());
    }

    /// Frames with no work: over 100 in a row get ENHANCE_YOUR_CALM.
    /// HEADERS or DATA with bytes between start the count again.
    #[test]
    fn attack_empty_frames() {
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(5));
        // empty DATA, PRIORITY and unknown types, mixed
        for i in 0..100 {
            let fs = match i % 3 {
                0 => c.send(frame::DATA, 0, 1, b""),
                1 => c.send(frame::PRIORITY, 0, 9, b"\0\0\0\x01\x10"),
                _ => c.send(0x20, 0, 0, b"?"),
            };
            assert!(fs.is_empty(), "{i}: {fs:?}");
        }
        // one frame with work: the count starts again
        c.send(frame::DATA, 0, 1, b"ab");
        for _ in 0..100 {
            assert!(c.send(frame::DATA, 0, 1, b"").is_empty());
        }
        goaway(&c.send(frame::DATA, 0, 1, b""), frame::ENHANCE_YOUR_CALM);
        // empty CONTINUATION frames: the block does not grow, so only
        // this count stops them
        let mut c = Client::new(250);
        c.send(frame::HEADERS, 0, 1, &block(GET));
        // the HEADERS counts as one
        for _ in 0..99 {
            assert!(c.send(frame::CONTINUATION, 0, 1, b"").is_empty());
        }
        goaway(
            &c.send(frame::CONTINUATION, 0, 1, b""),
            frame::ENHANCE_YOUR_CALM,
        );
    }

    /// A refused HEADERS, or an empty DATA with END_STREAM on a reset
    /// stream, is no work: it does not start the empty count again.
    #[test]
    fn refused_frames_do_not_reset_the_empty_count() {
        let mut c = Client::with_max(1, MAX_BODY);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(5));
        let mut id = 3;
        let mut last = Vec::new();
        'outer: for _ in 0..10 {
            for _ in 0..99 {
                last = c.send(frame::DATA, 0, 1, b"");
                if !last.is_empty() {
                    break 'outer;
                }
            }
            // refused over max_streams: still no work
            last = c.send(frame::HEADERS, frame::END_HEADERS, id, &block(GET));
            id += 2;
            if code(&last) != frame::REFUSED_STREAM {
                break;
            }
        }
        goaway(&last, frame::ENHANCE_YOUR_CALM);
        // empty DATA with END_STREAM on a stream the client reset
        let mut c = Client::new(250);
        c.get(1, GET);
        c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
        for _ in 0..10 {
            for _ in 0..99 {
                c.send(frame::PRIORITY, 0, 9, b"\0\0\0\x01\x10");
            }
            c.send(frame::DATA, frame::END_STREAM, 1, b"");
            if c.h2.over() {
                break;
            }
        }
        assert!(c.h2.over());
    }

    /// CONTINUATION flood (CVE-2024-27316 family): the header block is
    /// capped at MAX_HEAD bytes before it is decoded.
    #[test]
    fn attack_continuation_flood() {
        let mut c = Client::new(250);
        c.send(frame::HEADERS, 0, 1, &block(GET));
        let chunk = [b'a'; 1000];
        let mut fs = Vec::new();
        for _ in 0..20 {
            fs = c.send(frame::CONTINUATION, 0, 1, &chunk);
            if !fs.is_empty() {
                break;
            }
        }
        goaway(&fs, frame::ENHANCE_YOUR_CALM);
        assert!(c.recs.is_empty());
    }

    /// HPACK bomb: a table size over ours is an error, and a small block
    /// that decodes to a huge list gets 431 (see
    /// `a_huge_header_list_gets_431_on_its_stream`).
    #[test]
    fn attack_hpack_table_too_big() {
        let mut c = Client::new(250);
        let mut b = Vec::new();
        write_int(1 << 20, 5, 0x20, &mut b); // a table of 1 MiB
        b.extend(block(GET));
        goaway(&c.get_block(1, &b), frame::COMPRESSION_ERROR);
    }

    /// A normal busy client stays under every limit: many streams, some
    /// resets, PINGs, SETTINGS, PRIORITY and WINDOW_UPDATEs, for a few
    /// seconds. It is never cut.
    #[test]
    fn a_busy_normal_client_is_not_cut() {
        let mut c = Client::new(250);
        let mut id = 1;
        for _ in 0..3 {
            next_second(&mut c);
            for i in 0..200u32 {
                c.h2.flushed();
                assert!(c.get(id, GET).is_empty());
                if i % 3 == 0 {
                    c.send(frame::RST_STREAM, 0, id, &8u32.to_be_bytes());
                }
                if i % 4 == 0 {
                    c.send(frame::PING, 0, 0, b"12345678");
                    c.send(frame::PRIORITY, 0, id + 100, b"\0\0\0\x01\x10");
                    c.send(frame::WINDOW_UPDATE, 0, 0, &1000u32.to_be_bytes());
                }
                if i % 50 == 0 {
                    c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\x01\0\0");
                }
                id += 2;
            }
            for seq in 0..c.seq {
                c.answer(seq, 200, b"", b"ok");
            }
            // package:http2 gives the window back for each DATA it got,
            // also on streams that ended: many in a row
            for back in (1..id).step_by(2) {
                c.send(frame::WINDOW_UPDATE, 0, back, &2u32.to_be_bytes());
            }
            assert!(!c.h2.over());
        }
        assert_eq!(c.recs.len(), 600);
        assert_eq!(c.h2.open(), 0);
    }

    // ---- streamed request bodies and trailers ----

    /// A gRPC POST header block (no END_STREAM).
    fn grpc(ct: &str) -> Vec<u8> {
        block(&[
            (":method", "POST"),
            (":scheme", "http"),
            (":path", "/a.B/C"),
            ("content-type", ct),
            ("te", "trailers"),
        ])
    }

    /// The body records for Dart so far: (seq, kind, status, bytes).
    fn to_dart(c: &mut Client) -> Vec<(u32, u32, u16, Vec<u8>)> {
        std::mem::take(&mut c.h2.to_dart)
    }

    fn ack(c: &mut Client, seq: u32, n: u64) -> Vec<(Head, Vec<u8>)> {
        let mut out = Vec::new();
        c.h2.ack(seq, n, &mut out);
        frames(&out)
    }

    /// Dart's kind 8 with trailer lines, then what [H2::send] adds.
    fn end_with(c: &mut Client, seq: u32, trailers: &[u8]) -> Vec<(Head, Vec<u8>)> {
        let mut out = Vec::new();
        c.h2.part(seq, b"", true, true, trailers, &mut out);
        c.send_into(&mut out, 0);
        frames(&out)
    }

    /// `content-type: application/grpc*` (any case) streams: the record
    /// comes with the HEADERS, flag bit 4, no body. Others are buffered.
    /// HEADERS with END_STREAM has no body: a normal record.
    #[test]
    fn grpc_content_type_streams_the_body() {
        let mut c = Client::new(250);
        for (id, ct) in [(1, "application/grpc"), (3, "Application/GRPC+proto")] {
            assert!(
                c.send(frame::HEADERS, frame::END_HEADERS, id, &grpc(ct))
                    .is_empty()
            );
        }
        assert_eq!(c.recs.len(), 2);
        for r in &c.recs {
            let (flags, m, t, _) = record(r);
            assert_eq!((flags, m.as_str(), t.as_str()), (8 | 16, "POST", "/a.B/C"));
        }
        assert!(c.bodies.iter().all(|b| b.is_empty()));
        // not gRPC: Dart waits for the whole body
        for (id, ct) in [
            (5, "application/json"),
            (7, "application/gr"),
            (11, "application/grpc-web"),
            (13, "application/grpc-web+proto"),
            (15, "application/grpcfoo"),
        ] {
            c.send(frame::HEADERS, frame::END_HEADERS, id, &grpc(ct));
        }
        assert_eq!(c.recs.len(), 2);
        c.send(frame::DATA, frame::END_STREAM, 5, b"{}");
        assert_eq!(record(&c.recs[2]).0, 8);
        assert_eq!(c.bodies[2], b"{}");

        // gRPC with END_STREAM on the HEADERS: no body, no bit 4
        let flags = frame::END_HEADERS | frame::END_STREAM;
        c.send(frame::HEADERS, flags, 19, &grpc("application/grpc"));
        assert_eq!(record(&c.recs[3]).0, 8);
        assert!(to_dart(&mut c).is_empty());
        assert_eq!(c.h2.streaming().count(), 2);
        // `;` and `+` after the type still count
        let ct = "application/grpc; charset=utf-8";
        c.send(frame::HEADERS, frame::END_HEADERS, 21, &grpc(ct));
        assert_eq!(record(c.recs.last().unwrap()).0, 8 | 16);
        assert_eq!(c.h2.streaming().count(), 3);
    }

    /// Parts go to Dart in order. The stream window comes back only after
    /// BODY_ACK, in batches. The stream window bounds what waits for Dart.
    #[test]
    fn a_streamed_body_waits_for_acks() {
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        assert!(c.send(frame::DATA, 0, 1, b"ab").is_empty());
        let p = padded(b"cde", 4);
        assert!(c.send(frame::DATA, frame::PADDED, 1, &p).is_empty());
        let got = to_dart(&mut c);
        // the parts that wait for Dart are joined
        assert_eq!(got, [(0, crate::BODY_PART, 0, b"abcde".to_vec())]);
        // a whole window with no ack: no stream WINDOW_UPDATE at all
        let big = vec![7u8; 16_384];
        let mut sent = 2 + 3 + 5;
        while sent + big.len() <= 1 << 20 {
            let fs = c.send(frame::DATA, 0, 1, &big);
            assert!(updates(&fs).iter().all(|u| u.0 == 0), "{:?}", updates(&fs));
            sent += big.len();
        }
        let rest = (1 << 20) - sent;
        c.send(frame::DATA, 0, 1, &big[..rest]);
        let parts: usize = to_dart(&mut c).iter().map(|m| m.3.len()).sum();
        // all but the padding went to Dart
        assert_eq!(parts + 5 + 5, 1 << 20);
        // one byte more is over the window
        let fs = c.send(frame::DATA, 0, 1, b"x");
        assert_eq!(code(&fs), frame::FLOW_CONTROL_ERROR);
        assert!(!c.h2.over());

        // acks: small ones wait, half the window sends one update
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        for _ in 0..40 {
            c.send(frame::DATA, 0, 1, &big);
        }
        assert!(ack(&mut c, 0, 100).is_empty());
        assert!(ack(&mut c, 0, (super::super::ACK_BATCH) - 101).is_empty());
        assert_eq!(updates(&ack(&mut c, 0, 1)), [(1, (1 << 19) as u32)]);
        // an ack over what came is cut to it: the window never grows past
        // ours (and the rest is under a batch)
        assert!(ack(&mut c, 0, u64::MAX).is_empty());
        let r = c.h2.streams[&1].recv.as_ref().unwrap();
        assert_eq!(r.acked, 40 * 16_384 - (1 << 19));
        assert_eq!(r.window + r.acked as i64, 1 << 20);
        // an unknown seq does nothing
        assert!(ack(&mut c, 9, 1 << 20).is_empty());
    }

    /// Padding comes back without an ack: Dart never sees it.
    #[test]
    fn padding_of_a_streamed_body_comes_back() {
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        let p = padded(b"a", 255);
        let mut back = Vec::new();
        for _ in 0..3000 {
            back.extend(updates(&c.send(frame::DATA, frame::PADDED, 1, &p)));
        }
        assert!(
            back.iter().any(|u| u.0 == 1),
            "the stream got its padding back"
        );
        assert!(!c.h2.over());
    }

    /// A client RST: BODY_END with a non-zero status, once, and the
    /// request is gone (Dart has not answered).
    #[test]
    fn a_client_reset_ends_a_streamed_body_once() {
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        c.send(frame::DATA, 0, 1, b"ab");
        c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
        // a frame after the RST is a stream error: no second end
        c.send(frame::DATA, 0, 1, b"cd");
        c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
        assert_eq!(
            to_dart(&mut c),
            [
                (0, crate::BODY_PART, 0, b"ab".to_vec()),
                (0, crate::BODY_END, 1, Vec::new()),
            ]
        );
        assert_eq!(c.h2.gone, [0]);
        assert_eq!(c.h2.streaming().count(), 0);
        assert!(ack(&mut c, 0, 1 << 20).is_empty());
        // Dart answered: only BODY_END, no REQUEST_GONE
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            3,
            &grpc("application/grpc"),
        );
        let (ok, _) = start(&mut c, 1, b"", b"");
        assert!(ok);
        c.h2.gone.clear();
        c.send(frame::RST_STREAM, 0, 3, &8u32.to_be_bytes());
        assert_eq!(to_dart(&mut c), [(1, crate::BODY_END, 1, Vec::new())]);
        assert!(c.h2.gone.is_empty());
        assert_eq!(c.h2.gone_streams, [SID]);
    }

    /// END_STREAM on DATA, or trailers (dropped): BODY_END 0. HPACK stays in
    /// sync.
    #[test]
    fn a_streamed_body_ends_clean() {
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        c.send(frame::DATA, frame::END_STREAM, 1, b"ab");
        assert_eq!(
            to_dart(&mut c),
            [
                (0, crate::BODY_PART, 0, b"ab".to_vec()),
                (0, crate::BODY_END, 0, Vec::new()),
            ]
        );
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            3,
            &grpc("application/grpc"),
        );
        c.send(frame::DATA, 0, 3, b"cd");
        let flags = frame::END_STREAM | frame::END_HEADERS;
        let t = block(&[("x-t", "1")]);
        assert!(c.send(frame::HEADERS, flags, 3, &t).is_empty());
        assert_eq!(
            to_dart(&mut c),
            [
                (1, crate::BODY_PART, 0, b"cd".to_vec()),
                (1, crate::BODY_END, 0, Vec::new()),
            ]
        );
        // the answers go out as usual, the streams are not reset
        let fs = c.answer(1, 200, b"", b"ok");
        assert_eq!(data_frames(&fs), [(3, b"ok".to_vec())]);
        assert!(!fs.iter().any(|f| f.0.kind == frame::RST_STREAM));
        assert!(c.h2.gone.is_empty() && !c.h2.over());
        // a pseudo header in trailers is still malformed
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            5,
            &grpc("application/grpc"),
        );
        let fs = c.send(frame::HEADERS, flags, 5, &block(&[(":path", "/")]));
        assert_eq!(code(&fs), frame::PROTOCOL_ERROR);
        assert_eq!(to_dart(&mut c), [(2, crate::BODY_END, 1, Vec::new())]);
    }

    /// A streamed body has no body timeout and no size limits: a gRPC
    /// stream may be quiet for long and send much.
    #[test]
    fn a_streamed_body_has_no_timeout_and_no_limits() {
        let mut c = Client::with_max(250, 100);
        let t0 = c.now;
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        assert_eq!(c.h2.body_since(), None);
        let mut out = Vec::new();
        let later = t0 + Duration::from_secs(3600);
        c.h2.expire(later, Duration::from_secs(10), SEND_WAIT, DATE, &mut out);
        assert!(out.is_empty());
        assert_eq!(c.h2.open(), 1);
        // more than the body limit and the connection cap (4 * 200)
        for _ in 0..10 {
            let fs = c.send(frame::DATA, 0, 1, &[1; 1000]);
            assert!(updates(&fs).is_empty());
        }
        let parts: usize = to_dart(&mut c).iter().map(|m| m.3.len()).sum();
        assert_eq!(parts, 10_000);
        assert_eq!(c.h2.buffered, 0);
        // a buffered upload next to it still fits under the cap
        c.send(frame::HEADERS, frame::END_HEADERS, 3, &post(100));
        c.send(frame::DATA, frame::END_STREAM, 3, &[2; 100]);
        assert_eq!(c.bodies.last().unwrap().len(), 100);
        // a content-length still must match
        let mut fields = vec![("content-length", "3")];
        fields.extend([(":method", "POST"), (":scheme", "http"), (":path", "/")]);
        fields.rotate_left(1);
        fields.push(("content-type", "application/grpc"));
        c.send(frame::HEADERS, frame::END_HEADERS, 5, &block(&fields));
        let fs = c.send(frame::DATA, 0, 5, b"abcd");
        assert_eq!(code(&fs), frame::PROTOCOL_ERROR);
    }

    /// Dart answers while the body still comes (eg. a gRPC error): the
    /// answer ends the stream, RST_STREAM NO_ERROR stops the client, and
    /// its DATA on the way is ignored.
    #[test]
    fn an_answer_before_the_body_ends() {
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        c.send(frame::DATA, 0, 1, b"ab");
        to_dart(&mut c);
        let fs = c.answer(0, 200, b"grpc-status: 12\r\n", b"");
        assert_eq!(
            kinds(&fs),
            [
                (frame::HEADERS, frame::END_STREAM | frame::END_HEADERS, 1),
                (frame::RST_STREAM, 0, 1)
            ]
        );
        assert_eq!(code(&fs), frame::NO_ERROR);
        // status 2: we answered first, not a failure
        assert_eq!(to_dart(&mut c), [(0, crate::BODY_END, 2, Vec::new())]);
        assert!(c.h2.gone.is_empty(), "it was answered");
        assert!(c.send(frame::DATA, 0, 1, b"cd").is_empty());
        assert!(!c.h2.over());
    }

    /// Trailers on kind 8: the last DATA has no END_STREAM, a HEADERS with
    /// the trailers ends the stream. Names are lowercase, connection
    /// headers and lines with no name are dropped. A failed end ignores them.
    #[test]
    fn trailers_after_the_data() {
        let mut c = Client::new(250);
        c.send(frame::SETTINGS, 0, 0, b"\0\x04\0\0\0\x04"); // window 4
        c.get(1, GET);
        start(&mut c, 0, b"content-type: application/grpc\r\n", b"abcdef");
        let lines = b"Grpc-Status: 0\r\ngrpc-message: ok\r\nConnection: x\r\n:status: 1\r\n";
        // two bytes wait: the trailers wait behind them
        assert!(end_with(&mut c, 0, lines).is_empty());
        c.send(frame::WINDOW_UPDATE, 0, 1, &100u32.to_be_bytes());
        let fs = c.pump();
        let flags = frame::END_STREAM | frame::END_HEADERS;
        assert_eq!(
            kinds(&fs),
            [(frame::DATA, 0, 1), (frame::HEADERS, flags, 1)]
        );
        assert_eq!(fs[0].1, b"ef");
        assert_eq!(
            decode(&fs[1].1),
            [
                ("grpc-status".into(), "0".into()),
                ("grpc-message".into(), "ok".into())
            ]
        );
        assert_eq!(c.h2.open(), 0);
        // all bytes out before the end: the trailers come at once
        c.get(3, GET);
        start(&mut c, 1, b"", b"ab");
        let fs = end_with(&mut c, 1, b"grpc-status: 0\r\n");
        assert_eq!(kinds(&fs), [(frame::HEADERS, flags, 3)]);
        // lines that are all dropped: an empty DATA ends it, as before
        c.get(5, GET);
        start(&mut c, 2, b"", b"ab");
        let fs = end_with(&mut c, 2, b"connection: close\r\n");
        assert_eq!(kinds(&fs), [(frame::DATA, frame::END_STREAM, 5)]);
        // a failed end: RST, no trailers
        c.get(7, GET);
        start(&mut c, 3, b"", b"ab");
        let mut out = Vec::new();
        c.h2.part(3, b"", true, false, b"grpc-status: 0\r\n", &mut out);
        assert_eq!(kinds(&frames(&out)), rst(7, 0));
    }

    /// Trailers-only the streamed way: kind 6 with no parts, then kind 8
    /// with trailers. HEADERS (no END_STREAM), then the trailers HEADERS
    /// with END_STREAM: valid gRPC.
    #[test]
    fn a_stream_with_no_parts_and_trailers() {
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        c.send(frame::DATA, frame::END_STREAM, 1, b"x");
        let (ok, fs) = start(&mut c, 0, b"content-type: application/grpc\r\n", b"");
        assert!(ok);
        assert_eq!(kinds(&fs), [(frame::HEADERS, frame::END_HEADERS, 1)]);
        let fs = end_with(&mut c, 0, b"grpc-status: 5\r\n");
        let flags = frame::END_STREAM | frame::END_HEADERS;
        assert_eq!(kinds(&fs), [(frame::HEADERS, flags, 1)]);
        assert_eq!(decode(&fs[0].1), [("grpc-status".into(), "5".into())]);
        assert_eq!(c.h2.open(), 0);
        assert!(c.h2.gone_streams.is_empty());
    }

    /// Many small DATA frames with acks in between are work, not a flood.
    /// Empty DATA on a streamed body still counts as empty.
    #[test]
    fn a_chatty_streamed_body_is_not_cut() {
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        for i in 0..5000u32 {
            assert!(!c.h2.over());
            c.send(frame::DATA, 0, 1, &i.to_be_bytes());
            ack(&mut c, 0, 4);
        }
        let parts = to_dart(&mut c);
        assert_eq!(parts.len(), 1, "joined");
        assert_eq!(parts[0].3.len(), 5000 * 4);
        for _ in 0..200 {
            c.send(frame::DATA, 0, 1, b"");
        }
        assert!(c.h2.over(), "empty DATA is still a flood");
    }

    /// One body byte per DATA frame, a million of them on two streams that
    /// take turns: the parts are joined, so Dart gets few records and the
    /// bytes stay in order.
    #[test]
    fn tiny_data_frames_are_joined() {
        let mut c = Client::new(250);
        for id in [1, 3] {
            c.send(
                frame::HEADERS,
                frame::END_HEADERS,
                id,
                &grpc("application/grpc"),
            );
        }
        let (mut want, mut have) = ([Vec::new(), Vec::new()], [Vec::new(), Vec::new()]);
        let mut records = 0;
        for i in 0..(1u32 << 19) {
            for (n, id) in [1u32, 3].into_iter().enumerate() {
                let b = (i % 251) as u8;
                want[n].push(b);
                c.send(frame::DATA, 0, id, &[b]);
            }
            // Rust drains `to_dart` once in a while
            if i % 1000 == 999 {
                assert!(c.h2.to_dart.len() <= 2, "{}", c.h2.to_dart.len());
                for (seq, kind, _, data) in to_dart(&mut c) {
                    assert_eq!(kind, crate::BODY_PART);
                    have[seq as usize].extend_from_slice(&data);
                    records += 1;
                }
            }
        }
        for (seq, _, _, data) in to_dart(&mut c) {
            have[seq as usize].extend_from_slice(&data);
        }
        assert!(have == want, "the bytes are in order");
        assert!(records <= 2 * 525, "{records}");
    }

    /// The connection window of streamed bytes comes back on the ack, not
    /// on arrival: the unacked bytes never pass CONN_WINDOW. A mix with a
    /// buffered upload does not stall.
    #[test]
    fn streamed_bytes_hold_the_connection_window_until_acked() {
        let mut c = Client::new(250);
        let big = vec![3u8; 16_384];
        let mut conn_updates = 0;
        // 16 streams fill the connection window, 1 MiB each, no acks
        for k in 0..16u32 {
            let id = 2 * k + 1;
            c.send(
                frame::HEADERS,
                frame::END_HEADERS,
                id,
                &grpc("application/grpc"),
            );
            for _ in 0..64 {
                let fs = c.send(frame::DATA, 0, id, &big);
                conn_updates += updates(&fs).iter().filter(|u| u.0 == 0).count();
            }
        }
        assert_eq!(conn_updates, 0, "nothing is given back before an ack");
        assert_eq!(c.h2.unacked, CONN_WINDOW);
        assert_eq!(c.h2.recv, 0);
        // one byte more is over the connection window
        let fs = c.send(frame::DATA, 0, 1, b"x");
        goaway(&fs, frame::FLOW_CONTROL_ERROR);

        // an ack gives connection credit back
        let mut c = Client::new(250);
        for k in 0..16u32 {
            let id = 2 * k + 1;
            c.send(
                frame::HEADERS,
                frame::END_HEADERS,
                id,
                &grpc("application/grpc"),
            );
            for _ in 0..64 {
                c.send(frame::DATA, 0, id, &big);
            }
        }
        let fs = ack(&mut c, 0, 1 << 20);
        let conn: Vec<_> = updates(&fs).into_iter().filter(|u| u.0 == 0).collect();
        assert_eq!(conn, [(0, 1 << 20)]);
        assert_eq!(c.h2.unacked, CONN_WINDOW - (1 << 20));
        // an over-ack gives back at most what is owed
        let fs = ack(&mut c, 0, u64::MAX);
        assert!(updates(&fs).is_empty());
        assert_eq!(c.h2.unacked, CONN_WINDOW - (1 << 20));

        // a dropped stream gives back what was never acked
        let mut conn = Vec::new();
        for k in 1..9u32 {
            let fs = c.send(frame::RST_STREAM, 0, 2 * k + 1, &8u32.to_be_bytes());
            conn.extend(updates(&fs).into_iter().filter(|u| u.0 == 0));
        }
        // given back in a batch once half the window is due
        assert_eq!(conn, [(0, 8 << 20)]);
        assert_eq!(c.h2.unacked, CONN_WINDOW - (9 << 20));
        assert_eq!(c.h2.recv, 9 << 20);

        // buffered bytes still come back on arrival, next to streamed ones
        let mut c = Client::new(250);
        for k in 0..15u32 {
            let id = 2 * k + 1;
            c.send(
                frame::HEADERS,
                frame::END_HEADERS,
                id,
                &grpc("application/grpc"),
            );
            for _ in 0..64 {
                c.send(frame::DATA, 0, id, &big);
            }
        }
        assert_eq!(c.h2.unacked, 15 << 20);
        // 1 MiB left: buffered uploads of 64 KiB each, 40 of them (2.5 MiB
        // in all) must go through, because their bytes come back
        let mut seen = 0;
        for k in 0..40u32 {
            let id = 100 + 2 * k;
            c.send(frame::HEADERS, frame::END_HEADERS, id, &post(65_536));
            for i in 0..4 {
                let end = if i == 3 { frame::END_STREAM } else { 0 };
                let fs = c.send(frame::DATA, end, id, &big);
                assert!(
                    !fs.iter().any(|f| f.0.kind == frame::GOAWAY),
                    "stalled at {k}"
                );
                seen += c.bodies.len();
            }
        }
        assert!(seen > 0);
        assert!(c.h2.recv + c.h2.unacked <= CONN_WINDOW);
    }

    /// An ack after the body ended (END_STREAM) still gives connection
    /// credit back.
    #[test]
    fn an_ack_after_the_end_gives_credit() {
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        c.send(frame::DATA, frame::END_STREAM, 1, &[1u8; 1000]);
        assert_eq!(c.h2.unacked, 1000);
        ack(&mut c, 0, 1000);
        assert_eq!(c.h2.unacked, 0);
    }

    /// An ack for a buffered stream does nothing.
    #[test]
    fn an_ack_for_a_buffered_stream_is_ignored() {
        let mut c = Client::new(250);
        c.send(frame::HEADERS, frame::END_HEADERS, 1, &post(1000));
        c.send(frame::DATA, 0, 1, &[1u8; 100]);
        assert!(ack(&mut c, 0, 1 << 20).is_empty());
        let r = c.h2.streams[&1].recv.as_ref().unwrap();
        assert_eq!(r.acked, 0);
        assert_eq!(r.window, (1 << 20) - 100);
    }

    /// A client reset (not our answer) ends the body with status 1.
    #[test]
    fn a_streamed_body_end_without_answer_is_status_1() {
        let mut c = Client::new(250);
        c.send(
            frame::HEADERS,
            frame::END_HEADERS,
            1,
            &grpc("application/grpc"),
        );
        c.send(frame::DATA, 0, 1, b"ab");
        c.send(frame::RST_STREAM, 0, 1, &8u32.to_be_bytes());
        let ends: Vec<u16> = to_dart(&mut c)
            .iter()
            .filter(|m| m.1 == crate::BODY_END)
            .map(|m| m.2)
            .collect();
        assert_eq!(ends, [1]);
    }
}
