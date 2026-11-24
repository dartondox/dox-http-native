//! HTTP/2 frames (RFC 9113 section 4 and 6): read and write all 10 types.

/// Every frame starts with this many bytes: length (24 bits), type,
/// flags, stream id (31 bits).
pub(crate) const HEAD: usize = 9;

// frame types
pub(crate) const DATA: u8 = 0;
pub(crate) const HEADERS: u8 = 1;
pub(crate) const PRIORITY: u8 = 2;
pub(crate) const RST_STREAM: u8 = 3;
pub(crate) const SETTINGS: u8 = 4;
pub(crate) const PUSH_PROMISE: u8 = 5;
pub(crate) const PING: u8 = 6;
pub(crate) const GOAWAY: u8 = 7;
pub(crate) const WINDOW_UPDATE: u8 = 8;
pub(crate) const CONTINUATION: u8 = 9;

// flags
pub(crate) const END_STREAM: u8 = 0x1;
pub(crate) const ACK: u8 = 0x1;
pub(crate) const END_HEADERS: u8 = 0x4;
pub(crate) const PADDED: u8 = 0x8;
pub(crate) const PRIORITY_FLAG: u8 = 0x20;

// error codes (RFC 9113 section 7)
pub(crate) const NO_ERROR: u32 = 0x0;
pub(crate) const PROTOCOL_ERROR: u32 = 0x1;
pub(crate) const INTERNAL_ERROR: u32 = 0x2;
pub(crate) const FLOW_CONTROL_ERROR: u32 = 0x3;
pub(crate) const STREAM_CLOSED: u32 = 0x5;
pub(crate) const FRAME_SIZE_ERROR: u32 = 0x6;
pub(crate) const REFUSED_STREAM: u32 = 0x7;
pub(crate) const CANCEL: u32 = 0x8;
pub(crate) const COMPRESSION_ERROR: u32 = 0x9;
pub(crate) const ENHANCE_YOUR_CALM: u32 = 0xb;

/// The largest window (RFC 9113 6.9.1). More is a FLOW_CONTROL_ERROR.
pub(crate) const MAX_WINDOW: i64 = (1 << 31) - 1;

/// A frame head.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Head {
    pub(crate) len: usize,
    pub(crate) kind: u8,
    pub(crate) flags: u8,
    pub(crate) id: u32,
}

/// Reads a frame head. None when fewer than [HEAD] bytes are there. The
/// reserved bit of the stream id is ignored (RFC 9113 4.1).
pub(crate) fn read_head(b: &[u8]) -> Option<Head> {
    let b = b.get(..HEAD)?;
    Some(Head {
        len: usize::from(b[0]) << 16 | usize::from(b[1]) << 8 | usize::from(b[2]),
        kind: b[3],
        flags: b[4],
        id: u32_at(b, 5) & 0x7fff_ffff,
    })
}

fn u32_at(b: &[u8], p: usize) -> u32 {
    u32::from_be_bytes(b[p..p + 4].try_into().unwrap())
}

/// One parsed frame. Slices point into the payload.
#[derive(Debug, PartialEq)]
pub(crate) enum Frame<'a> {
    /// `flow` is the whole payload length (with padding): it counts
    /// against the flow control windows (RFC 9113 6.9.1).
    Data {
        end: bool,
        data: &'a [u8],
        flow: usize,
    },
    Headers {
        end_stream: bool,
        end_headers: bool,
        /// the stream it depends on, when the PRIORITY flag is set
        dep: Option<u32>,
        block: &'a [u8],
    },
    Priority {
        dep: u32,
    },
    RstStream {
        code: u32,
    },
    /// `params` holds 6 byte pairs: u16 id, u32 value. See [settings].
    Settings {
        ack: bool,
        params: &'a [u8],
    },
    PushPromise {
        end_headers: bool,
        promised: u32,
        block: &'a [u8],
    },
    Ping {
        ack: bool,
        data: [u8; 8],
    },
    GoAway {
        last: u32,
        code: u32,
        debug: &'a [u8],
    },
    WindowUpdate {
        inc: u32,
    },
    Continuation {
        end_headers: bool,
        block: &'a [u8],
    },
    /// an unknown type: ignored (RFC 9113 4.1)
    Unknown,
}

/// Why a frame is bad: the error code, and whether only the stream is
/// hit (a stream error) or the whole connection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Error {
    pub(crate) code: u32,
    pub(crate) stream: bool,
}

const fn conn(code: u32) -> Error {
    Error {
        code,
        stream: false,
    }
}

/// Parses the payload of one frame. The caller checked the length
/// against our MAX_FRAME_SIZE. This checks the fixed sizes, the padding
/// and the stream id 0 rules of each type.
pub(crate) fn parse<'a>(h: &Head, p: &'a [u8]) -> Result<Frame<'a>, Error> {
    // frames of the connection have id 0, frames of a stream do not
    let on_conn = matches!(h.kind, SETTINGS | PING | GOAWAY);
    let any_id = matches!(h.kind, WINDOW_UPDATE) || h.kind > CONTINUATION;
    if !any_id && on_conn != (h.id == 0) {
        return Err(conn(PROTOCOL_ERROR));
    }
    let size = |n: usize| {
        if p.len() == n {
            Ok(())
        } else {
            Err(conn(FRAME_SIZE_ERROR))
        }
    };
    Ok(match h.kind {
        DATA => Frame::Data {
            end: h.flags & END_STREAM != 0,
            data: unpad(h, p)?,
            flow: p.len(),
        },
        HEADERS => {
            let mut b = unpad(h, p)?;
            let mut dep = None;
            if h.flags & PRIORITY_FLAG != 0 {
                if b.len() < 5 {
                    return Err(conn(FRAME_SIZE_ERROR));
                }
                dep = Some(u32_at(b, 0) & 0x7fff_ffff);
                b = &b[5..];
            }
            Frame::Headers {
                end_stream: h.flags & END_STREAM != 0,
                end_headers: h.flags & END_HEADERS != 0,
                dep,
                block: b,
            }
        }
        PRIORITY => {
            // a wrong size hits only the stream (RFC 9113 6.3)
            if p.len() != 5 {
                return Err(Error {
                    code: FRAME_SIZE_ERROR,
                    stream: true,
                });
            }
            Frame::Priority {
                dep: u32_at(p, 0) & 0x7fff_ffff,
            }
        }
        RST_STREAM => {
            size(4)?;
            Frame::RstStream { code: u32_at(p, 0) }
        }
        SETTINGS => {
            let ack = h.flags & ACK != 0;
            if (ack && !p.is_empty()) || !p.len().is_multiple_of(6) {
                return Err(conn(FRAME_SIZE_ERROR));
            }
            Frame::Settings { ack, params: p }
        }
        PUSH_PROMISE => {
            let b = unpad(h, p)?;
            if b.len() < 4 {
                return Err(conn(FRAME_SIZE_ERROR));
            }
            Frame::PushPromise {
                end_headers: h.flags & END_HEADERS != 0,
                promised: u32_at(b, 0) & 0x7fff_ffff,
                block: &b[4..],
            }
        }
        PING => {
            size(8)?;
            Frame::Ping {
                ack: h.flags & ACK != 0,
                data: p.try_into().unwrap(),
            }
        }
        GOAWAY => {
            if p.len() < 8 {
                return Err(conn(FRAME_SIZE_ERROR));
            }
            Frame::GoAway {
                last: u32_at(p, 0) & 0x7fff_ffff,
                code: u32_at(p, 4),
                debug: &p[8..],
            }
        }
        WINDOW_UPDATE => {
            size(4)?;
            Frame::WindowUpdate {
                inc: u32_at(p, 0) & 0x7fff_ffff,
            }
        }
        CONTINUATION => Frame::Continuation {
            end_headers: h.flags & END_HEADERS != 0,
            block: p,
        },
        _ => Frame::Unknown,
    })
}

/// The payload without its padding (PADDED flag). Padding as long as the
/// payload or longer is a PROTOCOL_ERROR (RFC 9113 6.1).
fn unpad<'a>(h: &Head, p: &'a [u8]) -> Result<&'a [u8], Error> {
    if h.flags & PADDED == 0 {
        return Ok(p);
    }
    let (&pad, rest) = p.split_first().ok_or(conn(FRAME_SIZE_ERROR))?;
    rest.len()
        .checked_sub(usize::from(pad))
        .map(|end| &rest[..end])
        .ok_or(conn(PROTOCOL_ERROR))
}

/// The (id, value) pairs of a SETTINGS payload.
pub(crate) fn settings(params: &[u8]) -> impl Iterator<Item = (u16, u32)> + '_ {
    params
        .as_chunks::<6>()
        .0
        .iter()
        .map(|c| (u16::from_be_bytes([c[0], c[1]]), u32_at(c, 2)))
}

/// Writes one frame.
pub(crate) fn write(out: &mut Vec<u8>, kind: u8, flags: u8, id: u32, payload: &[u8]) {
    let n = payload.len();
    out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8, n as u8, kind, flags]);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(payload);
}

pub(crate) fn write_settings(out: &mut Vec<u8>, params: &[(u16, u32)]) {
    let mut p = Vec::with_capacity(params.len() * 6);
    for (id, v) in params {
        p.extend_from_slice(&id.to_be_bytes());
        p.extend_from_slice(&v.to_be_bytes());
    }
    write(out, SETTINGS, 0, 0, &p);
}

pub(crate) fn write_rst(out: &mut Vec<u8>, id: u32, code: u32) {
    write(out, RST_STREAM, 0, id, &code.to_be_bytes());
}

pub(crate) fn write_goaway(out: &mut Vec<u8>, last: u32, code: u32) {
    let mut p = [0u8; 8];
    p[..4].copy_from_slice(&last.to_be_bytes());
    p[4..].copy_from_slice(&code.to_be_bytes());
    write(out, GOAWAY, 0, 0, &p);
}

pub(crate) fn write_window(out: &mut Vec<u8>, id: u32, inc: u32) {
    write(out, WINDOW_UPDATE, 0, id, &inc.to_be_bytes());
}

/// A header block as HEADERS plus CONTINUATION frames of at most `max`
/// bytes each.
pub(crate) fn write_headers(
    out: &mut Vec<u8>,
    id: u32,
    block: &[u8],
    end_stream: bool,
    max: usize,
) {
    let mut parts = block.chunks(max).peekable();
    let mut kind = HEADERS;
    let mut flags = if end_stream { END_STREAM } else { 0 };
    loop {
        let part = parts.next().unwrap_or(&[]);
        let last = parts.peek().is_none();
        write(
            out,
            kind,
            flags | if last { END_HEADERS } else { 0 },
            id,
            part,
        );
        if last {
            break;
        }
        kind = CONTINUATION;
        flags = 0;
    }
}
