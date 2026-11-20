use super::hpack::{Decoder, HpackError, encode_field, read_int, write_int};
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
        Err(HpackError::ListTooLarge)
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

#[test]
fn list_over_cap() {
    let block = hex("8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d");
    // sizes: 42 + 43 + 38 + 57 = 180
    let mut d = Decoder::new(4096);
    assert_eq!(decode(&mut d, &block, 180).unwrap().len(), 4);
    let mut d = Decoder::new(4096);
    let mut seen = 0;
    let res = d.decode(&block, 179, |_, _| seen += 1);
    assert_eq!(res, Err(HpackError::ListTooLarge));
    assert_eq!(seen, 3);
    // a Huffman string stops as soon as it is over the room
    let value = huffman_encode(&[b'a'; 1000]);
    let mut block = vec![0x00, 0x01, b'x'];
    write_int(value.len(), 7, 0x80, &mut block);
    block.extend_from_slice(&value);
    let mut d = Decoder::new(4096);
    assert_eq!(
        decode(&mut d, &block, 32 + 1 + 100),
        Err(HpackError::ListTooLarge)
    );
    // a plain string over the room is refused too
    let mut d = Decoder::new(4096);
    assert_eq!(
        decode(&mut d, &[0x00, 0x01, b'x', 0x02, b'a', b'b'], 34),
        Err(HpackError::ListTooLarge)
    );
}
