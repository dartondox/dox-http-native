//! Native HTTP/1.1 server for Dart (dox_http_native).
//!
//! Rust only owns the sockets and parses HTTP (httparse). All app code
//! stays in Dart. One Dart isolate starts one server.
//!
//! One thread per isolate: the socket work runs on the thread of the Dart
//! isolate itself, like Bun. Dart calls `dox_run` (a non-leaf FFI call) in
//! a loop. One call writes the answers Dart gives, then accepts, reads and
//! parses new requests into a buffer of Dart. There is no hand off between
//! a Rust thread and the Dart thread under load. A small helper thread
//! only watches the sockets while Dart is idle, and wakes Dart with one
//! byte on its port. See `run.rs` for the loop, the helper and the limits.
//!
//! A connection belongs to the server (isolate) whose socket accepted it,
//! so its requests stay in order on one isolate. Linux spreads connections
//! over SO_REUSEPORT sockets, one per isolate. macOS gives all of them to
//! one such socket, so there all isolates share one socket (see [bind]).
//!
//! Request record (little endian, 44 byte head): u64 id, u32 flags
//! (bit 0: HTTP/1.0, bit 1: can upgrade to a WebSocket, bit 2: the body is
//! external, see below. Bits 8 to 15: a message kind, see below), u16
//! method_len, u16 target_len, u32 headers_len, u32 body_len, u16
//! remote_port, u16 reserved, 16 bytes remote ip (IPv6, IPv4 is mapped).
//! Then method, target, headers, body. Each header is u16 name_len, u16
//! value_len, name (lowercase), value. With the lengths Dart finds one
//! header without reading every byte.
//!
//! A body of [EXTERNAL_MIN] bytes or more is not copied into the buffer of
//! Dart. Its record has flag bit 2 and a 24 byte body: u64 address, u64
//! token, u64 length. Dart wraps the bytes as a typed list with the
//! finalizer `dox_free_body(token)`, so Rust frees them when Dart is done.
//!
//! Response record (20 byte head): u64 id, u16 status, u16 kind, u32
//! headers_len, u32 body_len. Then header lines, body. Kind 0 is an HTTP
//! answer. Kind bit 8 (0x100) on it: Rust gzips the body, on a helper
//! thread (Dart sets `content-encoding`). Kind bit 9 (0x200): the header
//! lines hold `connection: close`. Kinds 1 to 5 are for WebSockets, see
//! below.
//!
//! Streamed body: kind 6 is an HTTP answer whose body comes in parts. Its
//! body is u64 stream id (Dart picks it), then the first part. Then kind 7
//! records (id: the stream id, body: a part) and one kind 8 (status 0: the
//! end, else: failed, the connection is cut). Rust tells Dart how many
//! bytes reached the socket: a record with kind 4 ([STREAM_SENT]), the
//! stream id and a u64 body. Dart keeps at most a window of bytes not sent
//! yet, so memory stays small and the client gets the first bytes early.
//! Kind 5 ([STREAM_GONE]): the client is gone, stop sending.
//!
//! WebSocket: a request that can upgrade (GET, HTTP/1.1, `Connection:
//! upgrade`, `Upgrade: websocket`, version 13, a key) gets flag bit 1. Dart
//! runs routing, middleware and the Origin check like for any request.
//! - Dart says no with a normal answer (eg. 403). There is no upgrade.
//! - Dart says yes with kind 1. The body is u64 ws id (Dart picks it) and
//!   u64 max message size. Its header lines (eg. `sec-websocket-protocol`)
//!   go on the `101` answer. Rust adds the upgrade headers.
//!
//! Then Rust speaks the WebSocket protocol (the sync `tungstenite` crate on
//! the non-blocking socket): ping and pong, the close handshake, the UTF-8
//! check of text and the size limit (a bigger message or frame closes with
//! 1009). It never agrees to compression (permessage-deflate). Messages to
//! Dart are records with a kind in flags bits 8 to 15: 1 text, 2 binary, 3
//! closed. Their id is the ws id and their body the payload. For 3 the
//! remote_port field is the close code (1005: no code, 1006: no close
//! frame) and the body the reason. Rust sends exactly one kind 3 record per
//! ws, when it ended. Messages from Dart are response records: kind 2
//! text, 3 binary, 4 close (status: the close code, 0 for none. body: the
//! reason). Kind 5: Dart handled this many bytes (the id field) of client
//! messages.
//!
//! Limits, so a slow side can not make memory grow forever:
//! - Dart to client: each socket has a queue of messages that Rust did not
//!   send yet. Over 4 times the max message size (at least 1 MiB) the socket
//!   closes with 1008 (policy violation). The client reads too slowly.
//! - Client to Dart: while Dart has more than [WS_UNACKED] bytes of client
//!   messages not acknowledged (kind 5), Rust stops reading WebSocket
//!   frames. No data is dropped. HTTP is not paused.
//! - A send to the client may make no progress for [SEND_WAIT], a close
//!   handshake may take [CLOSE_WAIT]. Then the socket is dropped (1006).
//!   The same send limit holds for HTTP answers.
//! - One `dox_run` call reads at most 256 KB of one WebSocket. While more
//!   than 256 KB (eg. pongs) waits to be written, Rust reads no frames of it.
//! - HTTP: while 1 MB of answers waits to be written, Rust reads no new
//!   requests of that connection. See `run.rs` for the other limits.
mod run;

use std::ffi::{CStr, c_char, c_int, c_void};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener as StdListener};
#[cfg(not(any(target_os = "linux", target_os = "android")))]
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};

/// Bump when the FFI functions or the record layout change.
const ABI_VERSION: i32 = 7;
/// Max size of the request line plus headers. Bigger gets 431.
const MAX_HEAD: usize = 16 * 1024;
const RES_HEAD: usize = 20;
/// Request record head: 28 bytes, then 16 bytes remote ip.
const REQ_HEAD: usize = 44;
/// Bytes a too big body may still send before the connection is closed.
const DRAIN_MAX: usize = 64 * 1024 * 1024;
/// Most memory we reserve up front for a body. `Content-Length` comes from
/// the client, so a bigger body grows the buffer only as bytes arrive.
const MAX_RESERVE: usize = 64 * 1024;
/// WebSocket message kinds to Dart (flags bits 8 to 15 of a record)
const WS_TEXT: u32 = 1;
const WS_BINARY: u32 = 2;
const WS_CLOSED: u32 = 3;
/// Stream messages to Dart (flags bits 8 to 15): bytes sent, client gone
const STREAM_SENT: u32 = 4;
const STREAM_GONE: u32 = 5;
/// A body this big or bigger goes to Dart without a copy (external typed
/// data). A smaller one is copied: for a few bytes a copy is cheaper than
/// a finalizer.
const EXTERNAL_MIN: usize = 64 * 1024;
/// How long a closing WebSocket waits for the close frame of the peer.
/// Same as dart:io.
const CLOSE_WAIT: Duration = Duration::from_secs(5);
/// How long a send to a WebSocket client may make no progress. After that
/// the client counts as gone: the socket is dropped and Dart gets 1006.
const SEND_WAIT: Duration = Duration::from_secs(30);
/// The queue from Dart to one WebSocket holds at least this many bytes.
const MIN_QUEUE: usize = 1 << 20;
/// Client message bytes Dart may have unacknowledged before Rust stops
/// reading WebSocket frames, for all sockets of the server together.
const WS_UNACKED: usize = 8 << 20;

// ---- Dart API DL -------------------------------------------------------
// `NativeApi.initializeApiDLData` points to this table. We only need
// `Dart_PostCObject`, so we read it here instead of compiling dart_api_dl.c.

#[repr(C)]
struct ApiEntry {
    name: *const c_char,
    function: *const c_void,
}

#[repr(C)]
struct DartApi {
    major: c_int,
    minor: c_int,
    functions: *const ApiEntry,
}

/// `Dart_CObject` for a `kTypedData` (Uint8) value. 64-bit layout.
#[repr(C)]
struct DartCObject {
    ty: i32,
    _pad: i32,
    td_type: i32,
    _pad2: i32,
    length: isize,
    values: *const u8,
    _rest: [u64; 2],
}

type PostFn = unsafe extern "C" fn(i64, *mut DartCObject) -> bool;
static POST: OnceLock<PostFn> = OnceLock::new();

/// Posts bytes to a Dart port. Dart copies them. False when the port is
/// closed (the isolate is gone).
fn post(port: i64, data: &[u8]) -> bool {
    let Some(f) = POST.get() else { return false };
    let mut obj = DartCObject {
        ty: 7, // Dart_CObject_kTypedData
        _pad: 0,
        td_type: 2, // Dart_TypedData_kUint8
        _pad2: 0,
        length: data.len() as isize,
        values: data.as_ptr(),
        _rest: [0; 2],
    };
    unsafe { f(port, &mut obj) }
}

// ---- listening sockets -------------------------------------------------

/// The address to listen on: (family, high 64 bits, low 64 bits, port).
/// family 0: all interfaces, IPv6 and IPv4 (dual stack, like dart:io
/// `anyIPv6`). 4: the IPv4 address in the low 32 bits. 6: an IPv6 address.
type Key = (i32, u64, u64, u16);

fn socket_addr(key: Key) -> Option<SocketAddr> {
    let (family, hi, lo, port) = key;
    match family {
        4 => Some(SocketAddr::from((Ipv4Addr::from(lo as u32), port))),
        6 => Some(SocketAddr::from((
            Ipv6Addr::from((u128::from(hi) << 64) | u128::from(lo)),
            port,
        ))),
        _ => None,
    }
}

fn bind_new(key: Key) -> std::io::Result<StdListener> {
    let port = key.3;
    let (socket, addr) = match socket_addr(key) {
        Some(addr) => {
            let s = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
            if addr.is_ipv6() {
                let _ = s.set_only_v6(false); // `::` also takes IPv4, like dart:io
            }
            (s, addr)
        }
        // dual stack like dart:io `anyIPv6`. IPv4 only when there is no IPv6.
        None => match Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP)) {
            Ok(s) if s.set_only_v6(false).is_ok() => (s, SocketAddr::from(([0u16; 8], port))),
            _ => (
                Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?,
                SocketAddr::from(([0, 0, 0, 0], port)),
            ),
        },
    };
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    // Linux spreads connections over SO_REUSEPORT sockets: one per isolate.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    socket.set_reuse_port(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Listening sockets of this process by address, with a user count.
/// macOS does not spread connections over SO_REUSEPORT sockets (one gets
/// all), so there every isolate gets a copy (dup) of one socket and they
/// all accept from the same queue. Windows runs one isolate (see DoxServer).
#[cfg(not(any(target_os = "linux", target_os = "android")))]
static LISTENERS: Mutex<Vec<(Key, StdListener, usize)>> = Mutex::new(Vec::new());

/// Binds `key`. Returns the listener and the key with the real port.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn bind(key: Key) -> std::io::Result<(StdListener, Key)> {
    let listener = bind_new(key)?;
    let port = listener.local_addr()?.port();
    Ok((listener, (key.0, key.1, key.2, port)))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn unbind(_key: Key) {}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn bind(key: Key) -> std::io::Result<(StdListener, Key)> {
    let mut all = LISTENERS.lock().unwrap();
    if key.3 != 0
        && let Some(entry) = all.iter_mut().find(|e| e.0 == key)
    {
        entry.2 += 1;
        return Ok((entry.1.try_clone()?, key));
    }
    let listener = bind_new(key)?;
    let real = (key.0, key.1, key.2, listener.local_addr()?.port());
    all.push((real, listener.try_clone()?, 1));
    Ok((listener, real))
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn unbind(key: Key) {
    let mut all = LISTENERS.lock().unwrap();
    if let Some(i) = all.iter().position(|e| e.0 == key) {
        all[i].2 -= 1;
        if all[i].2 == 0 {
            all.swap_remove(i); // closes the last copy: the port is free
        }
    }
}

/// The peer as 16 bytes ip (IPv4 mapped) and a port.
fn peer_bytes(addr: SocketAddr) -> [u8; 18] {
    let ip = match addr.ip() {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    };
    let mut out = [0u8; 18];
    out[..16].copy_from_slice(&ip.octets());
    out[16..].copy_from_slice(&addr.port().to_le_bytes());
    out
}

/// Gzips `data` at the default level, like `dart:io` `gzip.encode`.
fn gzip(data: &[u8]) -> Vec<u8> {
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;
    let mut z = GzEncoder::new(Vec::with_capacity(data.len() / 2), Compression::default());
    // writes to a Vec do not fail
    let _ = z.write_all(data);
    z.finish().unwrap_or_default()
}

fn rd16(b: &[u8], p: usize) -> u16 {
    u16::from_le_bytes([b[p], b[p + 1]])
}

fn rd32(b: &[u8], p: usize) -> usize {
    u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize
}

// ---- FFI ---------------------------------------------------------------

/// ABI version of this library. Dart checks it at load.
#[unsafe(no_mangle)]
pub extern "C" fn dox_version() -> i32 {
    ABI_VERSION
}

/// Reads `Dart_PostCObject` from `NativeApi.initializeApiDLData`.
/// Returns 0 on success.
///
/// # Safety
/// `data` must be `NativeApi.initializeApiDLData`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_init(data: *mut c_void) -> i32 {
    if data.is_null() {
        return -1;
    }
    let api = unsafe { &*(data as *const DartApi) };
    if api.major != 2 {
        return -2; // DART_API_DL_MAJOR_VERSION
    }
    let mut entry = api.functions;
    unsafe {
        while !(*entry).name.is_null() {
            if CStr::from_ptr((*entry).name).to_bytes() == b"Dart_PostCObject" {
                let f: PostFn = std::mem::transmute((*entry).function);
                let _ = POST.set(f);
                return 0;
            }
            entry = entry.add(1);
        }
    }
    -3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_one_host_only() {
        let lo = u64::from(u32::from(Ipv4Addr::LOCALHOST));
        let (listener, key) = bind((4, 0, lo, 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(key.3, addr.port());
        unbind(key);
    }

    #[test]
    fn peer_is_mapped_ipv6() {
        let p = peer_bytes("1.2.3.4:513".parse().unwrap());
        assert_eq!(&p[10..16], &[0xff, 0xff, 1, 2, 3, 4]);
        assert_eq!(u16::from_le_bytes([p[16], p[17]]), 513);
    }

    #[test]
    fn gzip_round_trip() {
        use std::io::Read;
        let data = b"hello hello hello hello".repeat(50);
        let zipped = gzip(&data);
        assert!(zipped.len() < data.len());
        let mut back = Vec::new();
        flate2::read::GzDecoder::new(&zipped[..])
            .read_to_end(&mut back)
            .unwrap();
        assert_eq!(back, data);
    }
}
