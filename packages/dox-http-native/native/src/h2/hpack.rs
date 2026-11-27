//! HPACK (RFC 7541): the header compression of HTTP/2.
//!
//! - `Decoder` reads the header blocks of one connection. It keeps the
//!   dynamic table the client builds, so one connection has one `Decoder`.
//! - `encode_field` writes our answer headers. It needs no state.

use super::huffman;
use std::collections::VecDeque;

/// Why a header block could not be decoded. Each one is a connection
/// error (COMPRESSION_ERROR), so the caller closes the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HpackError {
    /// The block ends inside a field.
    Truncated,
    /// An integer is over 2^32 - 1, or has too many bytes.
    IntegerOverflow,
    /// Index 0, or an index past the end of the table.
    BadIndex,
    /// A Huffman string with EOS in it, or with bad padding.
    BadHuffman,
    /// A table size update over our limit, or after the first field.
    BadSizeUpdate,
}

/// How a header block that decoded fine ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decoded {
    /// All fields were passed to `emit`.
    Whole,
    /// The list went over `max_list`. Fields after that point were not
    /// passed to `emit`. The dynamic table is in sync.
    TooLarge,
}

/// Each field counts its name, its value and 32 bytes (RFC 7541 4.1).
/// The same count is used for SETTINGS_MAX_HEADER_LIST_SIZE.
const FIELD_OVERHEAD: usize = 32;

/// The static table (RFC 7541 Appendix A). Index 1 is the first entry.
const STATIC: [(&[u8], &[u8]); 61] = [
    (b":authority", b""),
    (b":method", b"GET"),
    (b":method", b"POST"),
    (b":path", b"/"),
    (b":path", b"/index.html"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"200"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"304"),
    (b":status", b"400"),
    (b":status", b"404"),
    (b":status", b"500"),
    (b"accept-charset", b""),
    (b"accept-encoding", b"gzip, deflate"),
    (b"accept-language", b""),
    (b"accept-ranges", b""),
    (b"accept", b""),
    (b"access-control-allow-origin", b""),
    (b"age", b""),
    (b"allow", b""),
    (b"authorization", b""),
    (b"cache-control", b""),
    (b"content-disposition", b""),
    (b"content-encoding", b""),
    (b"content-language", b""),
    (b"content-length", b""),
    (b"content-location", b""),
    (b"content-range", b""),
    (b"content-type", b""),
    (b"cookie", b""),
    (b"date", b""),
    (b"etag", b""),
    (b"expect", b""),
    (b"expires", b""),
    (b"from", b""),
    (b"host", b""),
    (b"if-match", b""),
    (b"if-modified-since", b""),
    (b"if-none-match", b""),
    (b"if-range", b""),
    (b"if-unmodified-since", b""),
    (b"last-modified", b""),
    (b"link", b""),
    (b"location", b""),
    (b"max-forwards", b""),
    (b"proxy-authenticate", b""),
    (b"proxy-authorization", b""),
    (b"range", b""),
    (b"referer", b""),
    (b"refresh", b""),
    (b"retry-after", b""),
    (b"server", b""),
    (b"set-cookie", b""),
    (b"strict-transport-security", b""),
    (b"transfer-encoding", b""),
    (b"user-agent", b""),
    (b"vary", b""),
    (b"via", b""),
    (b"www-authenticate", b""),
];

/// A name and a value.
type Field<'a> = (&'a [u8], &'a [u8]);

/// One dynamic table entry: name and value in one allocation.
struct Entry {
    bytes: Box<[u8]>,
    name_len: usize,
}

impl Entry {
    fn new(name: &[u8], value: &[u8]) -> Self {
        Entry {
            bytes: [name, value].concat().into_boxed_slice(),
            name_len: name.len(),
        }
    }

    fn name(&self) -> &[u8] {
        &self.bytes[..self.name_len]
    }

    fn value(&self) -> &[u8] {
        &self.bytes[self.name_len..]
    }

    fn size(&self) -> usize {
        self.bytes.len() + FIELD_OVERHEAD
    }
}

pub(crate) struct Decoder {
    /// Newest entry first: index 62 is `table[0]`.
    table: VecDeque<Entry>,
    /// The size of all entries (RFC 7541 4.1).
    size: usize,
    /// The table size the client set with a size update. Starts at `limit`.
    max: usize,
    /// Our SETTINGS_HEADER_TABLE_SIZE. A size update may not go over it.
    limit: usize,
    /// Huffman output, kept for the next fields.
    name_buf: Vec<u8>,
    value_buf: Vec<u8>,
}

impl Decoder {
    /// `limit` is the SETTINGS_HEADER_TABLE_SIZE we advertise (4096).
    ///
    /// ponytail: the limit is fixed for the connection. If we ever lower it
    /// with a new SETTINGS, the next block must start with a size update
    /// (RFC 7541 4.2). Check that here then.
    pub(crate) fn new(limit: usize) -> Self {
        Decoder {
            table: VecDeque::new(),
            size: 0,
            max: limit,
            limit,
            name_buf: Vec::new(),
            value_buf: Vec::new(),
        }
    }

    /// Decodes one whole header block (HEADERS plus its CONTINUATION
    /// frames) and calls `emit(name, value)` for each field, in order.
    ///
    /// `max_list` caps the decoded list: each field counts name + value +
    /// 32 bytes. Over the cap, `emit` is not called any more, but the
    /// block is still read to its end, so the dynamic table stays in sync
    /// with the client's. The result is then `Decoded::TooLarge`, and the
    /// caller may answer just that request (431). Nothing grows past the
    /// cap or the table size: a long string is counted, not copied.
    /// Real errors (`HpackError`) leave the table out of sync: the caller
    /// must close the connection.
    pub(crate) fn decode(
        &mut self,
        block: &[u8],
        max_list: usize,
        mut emit: impl FnMut(&[u8], &[u8]),
    ) -> Result<Decoded, HpackError> {
        let mut pos = 0;
        let mut list = 0usize;
        let mut fields = false;
        let mut over = false;
        while pos < block.len() {
            let b = block[pos];
            if b & 0xe0 == 0x20 {
                // a dynamic table size update: only before the first
                // field (RFC 7541 4.2), several in a row are fine
                if fields {
                    return Err(HpackError::BadSizeUpdate);
                }
                let size = read_int(block, &mut pos, 5)?;
                if size > self.limit {
                    return Err(HpackError::BadSizeUpdate);
                }
                self.max = size;
                self.evict();
                continue;
            }
            fields = true;
            // The strings may be as long as the list has room, or as the
            // table can keep. A longer one is counted, not copied.
            let list_room = if over { 0 } else { max_list - list };
            let room = list_room.max(self.limit);
            let (name, nlen, value, vlen, indexing);
            if b & 0x80 != 0 {
                // an indexed field
                let index = read_int(block, &mut pos, 7)?;
                (name, value) = lookup(&self.table, index)?;
                (nlen, vlen, indexing) = (name.len(), value.len(), false);
            } else {
                // a literal: with incremental indexing (01), without
                // indexing (0000) or never indexed (0001)
                indexing = b & 0x40 != 0;
                let index = read_int(block, &mut pos, if indexing { 6 } else { 4 })?;
                (name, nlen) = if index == 0 {
                    read_str(block, &mut pos, &mut self.name_buf, room)?
                } else {
                    let n = lookup(&self.table, index)?.0;
                    (n, n.len())
                };
                (value, vlen) = read_str(
                    block,
                    &mut pos,
                    &mut self.value_buf,
                    room.saturating_sub(nlen),
                )?;
            }
            let size = nlen + vlen + FIELD_OVERHEAD;
            if !over && size <= list_room {
                list += size;
                emit(name, value);
            } else {
                over = true;
            }
            if indexing {
                // copy first: adding the entry can evict the entry the
                // name came from. A too big entry is never copied (its
                // strings may be cut): it only empties the table.
                let entry = (size <= self.max).then(|| Entry::new(name, value));
                self.insert(entry);
            }
        }
        Ok(if over {
            Decoded::TooLarge
        } else {
            Decoded::Whole
        })
    }

    /// Adds an entry as index 62 and evicts the oldest ones until the
    /// table fits. `None` is an entry bigger than the whole table: it
    /// empties the table and is not added (RFC 7541 4.4).
    fn insert(&mut self, entry: Option<Entry>) {
        let Some(entry) = entry else {
            self.table.clear();
            self.size = 0;
            return;
        };
        self.size += entry.size();
        self.table.push_front(entry);
        self.evict();
    }

    fn evict(&mut self) {
        while self.size > self.max {
            match self.table.pop_back() {
                Some(old) => self.size -= old.size(),
                None => break,
            }
        }
    }

    /// The table size and its entries, newest first. For tests and fuzzing.
    #[cfg(any(test, fuzzing))]
    pub(crate) fn table(&self) -> (usize, Vec<Field<'_>>) {
        let entries = self.table.iter().map(|e| (e.name(), e.value())).collect();
        (self.size, entries)
    }

    #[cfg(any(test, fuzzing))]
    pub(crate) fn max_size(&self) -> usize {
        self.max
    }
}

/// The field at `index`: 1 to 61 is the static table, 62 and up the
/// dynamic table, newest first.
fn lookup(table: &VecDeque<Entry>, index: usize) -> Result<Field<'_>, HpackError> {
    if index == 0 {
        return Err(HpackError::BadIndex);
    }
    if index <= STATIC.len() {
        return Ok(STATIC[index - 1]);
    }
    match table.get(index - STATIC.len() - 1) {
        Some(e) => Ok((e.name(), e.value())),
        None => Err(HpackError::BadIndex),
    }
}

/// Reads an integer with an N-bit prefix (RFC 7541 5.1). The flag bits
/// above the prefix are ignored. Values over 2^32 - 1 are an error, so
/// nothing wraps.
pub(super) fn read_int(block: &[u8], pos: &mut usize, prefix: u32) -> Result<usize, HpackError> {
    let mask = (1u64 << prefix) - 1;
    let first = *block.get(*pos).ok_or(HpackError::Truncated)?;
    *pos += 1;
    let mut value = u64::from(first) & mask;
    if value < mask {
        return Ok(value as usize);
    }
    let mut shift = 0;
    loop {
        let b = *block.get(*pos).ok_or(HpackError::Truncated)?;
        *pos += 1;
        value += u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        // 5 bytes after the prefix hold 35 bits; more is never needed
        if shift > 28 {
            return Err(HpackError::IntegerOverflow);
        }
    }
    usize::try_from(value)
        .ok()
        .filter(|v| *v <= u32::MAX as usize)
        .ok_or(HpackError::IntegerOverflow)
}

/// Reads a string literal (RFC 7541 5.2) and gives (bytes, full length).
/// A plain string is a slice of `block`, so it costs no copy. A Huffman
/// string is decoded into `buf`. Only the first `room` bytes are kept: for
/// a longer string the bytes are cut and the full length says so.
fn read_str<'a>(
    block: &'a [u8],
    pos: &mut usize,
    buf: &'a mut Vec<u8>,
    room: usize,
) -> Result<(&'a [u8], usize), HpackError> {
    let huffman = *block.get(*pos).ok_or(HpackError::Truncated)? & 0x80 != 0;
    let len = read_int(block, pos, 7)?;
    let raw = block[*pos..].get(..len).ok_or(HpackError::Truncated)?;
    *pos += len;
    if huffman {
        buf.clear();
        let full = huffman::decode(raw, buf, room)?;
        Ok((buf, full))
    } else {
        Ok((&raw[..len.min(room)], len))
    }
}

/// Writes one header field into `out`. `name` must be lowercase (HTTP/2
/// rule), the caller makes sure of that.
///
/// - name and value in the static table: indexed (1 byte)
/// - only the name in it: literal without indexing, with the indexed name
/// - else: literal without indexing, with a new name
///
/// ponytail: no Huffman and no dynamic table. This is valid and safe. Add
/// them when benchmarks show that header bytes matter.
pub(crate) fn encode_field(name: &[u8], value: &[u8], out: &mut Vec<u8>) {
    let mut name_index = 0;
    for (i, (n, v)) in STATIC.iter().enumerate() {
        if *n == name {
            if *v == value {
                write_int(i + 1, 7, 0x80, out);
                return;
            }
            if name_index == 0 {
                name_index = i + 1;
            }
        }
    }
    write_int(name_index, 4, 0x00, out);
    if name_index == 0 {
        write_str(name, out);
    }
    write_str(value, out);
}

/// A literal with incremental indexing: the client adds the field to its
/// dynamic table, and later blocks name it with one index byte.
pub(crate) fn encode_insert(name: &[u8], value: &[u8], out: &mut Vec<u8>) {
    let index = STATIC
        .iter()
        .position(|(n, _)| *n == name)
        .map_or(0, |i| i + 1);
    write_int(index, 6, 0x40, out);
    if index == 0 {
        write_str(name, out);
    }
    write_str(value, out);
}

/// Writes an integer with an N-bit prefix and the `flags` bits above it.
pub(super) fn write_int(mut value: usize, prefix: u32, flags: u8, out: &mut Vec<u8>) {
    let mask = (1usize << prefix) - 1;
    if value < mask {
        out.push(flags | value as u8);
        return;
    }
    out.push(flags | mask as u8);
    value -= mask;
    while value >= 0x80 {
        out.push(0x80 | (value & 0x7f) as u8);
        value >>= 7;
    }
    out.push(value as u8);
}

/// A plain (not Huffman) string literal.
pub(super) fn write_str(s: &[u8], out: &mut Vec<u8>) {
    write_int(s.len(), 7, 0x00, out);
    out.extend_from_slice(s);
}

/// The `hpack` fuzz target (see `run::fuzz`). Splits `data` into header
/// blocks and decodes them with one `Decoder`. It checks that the list cap
/// and the table size hold, and that each decoded block encodes and
/// decodes again to the same fields.
#[cfg(fuzzing)]
pub(crate) fn fuzz(data: &[u8]) {
    let Some((&cap, mut rest)) = data.split_first() else {
        return;
    };
    let max_list = (cap as usize + 1) * 32;
    let mut dec = Decoder::new(4096);
    // each block: one length byte, then that many bytes
    while let Some((&len, tail)) = rest.split_first() {
        let len = (len as usize).min(tail.len());
        let (block, next) = tail.split_at(len);
        rest = next;
        let mut fields = Vec::new();
        let mut size = 0;
        let res = dec.decode(block, max_list, |n, v| {
            size += n.len() + v.len() + FIELD_OVERHEAD;
            fields.push((n.to_vec(), v.to_vec()));
        });
        assert!(size <= max_list, "the list went over the cap");
        let (table_size, entries) = dec.table();
        assert!(table_size <= dec.max_size() && dec.max_size() <= 4096);
        let sum: usize = entries.iter().map(|(n, v)| n.len() + v.len() + 32).sum();
        assert_eq!(sum, table_size);
        if res.is_err() {
            // the connection closes, the rest is never read
            return;
        }
        let mut out = Vec::new();
        for (n, v) in &fields {
            encode_field(n, v, &mut out);
        }
        let mut again = Vec::new();
        Decoder::new(4096)
            .decode(&out, usize::MAX, |n, v| {
                again.push((n.to_vec(), v.to_vec()))
            })
            .expect("our own encoding must decode");
        assert_eq!(fields, again);
    }
}
