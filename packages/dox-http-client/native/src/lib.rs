//! Native HTTP client for Dart (dox_http_client): HTTP/1.1 and HTTP/2.
//!
//! Rust owns the sockets and parses the answers. It runs on the thread of
//! the Dart isolate. See `client.rs` for the records and the connections.
//!
//! Every exported symbol starts with `dox_client_`. Thus one process can
//! load this library and the library of dox_http_native together.
mod client;

use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::OnceLock;

/// Bump when the FFI functions or the record layout change.
const ABI_VERSION: i32 = 4;

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

fn rd32(b: &[u8], p: usize) -> usize {
    u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize
}

/// 0 when the bytes are not there.
fn rd64(b: &[u8], p: usize) -> u64 {
    b.get(p..p + 8)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map_or(0, u64::from_le_bytes)
}

// ---- FFI ---------------------------------------------------------------

/// ABI version of this library. Dart checks it at load.
#[unsafe(no_mangle)]
pub extern "C" fn dox_client_version() -> i32 {
    ABI_VERSION
}

/// Reads `Dart_PostCObject` from `NativeApi.initializeApiDLData`.
/// Returns 0 on success.
///
/// # Safety
/// `data` must be `NativeApi.initializeApiDLData`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_client_init(data: *mut c_void) -> i32 {
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

/// Native memory for Dart (non-leaf calls can not use Dart heap bytes).
#[unsafe(no_mangle)]
pub extern "C" fn dox_client_alloc(size: i64) -> *mut c_void {
    let b: Box<[u8]> = vec![0u8; size.max(1) as usize].into_boxed_slice();
    Box::into_raw(b) as *mut u8 as *mut c_void
}

/// Frees memory from `dox_client_alloc`.
///
/// # Safety
/// `ptr` and `size` must come from one `dox_client_alloc` call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_client_free(ptr: *mut c_void, size: i64) {
    let slice = std::ptr::slice_from_raw_parts_mut(ptr as *mut u8, size.max(1) as usize);
    drop(unsafe { Box::from_raw(slice) });
}

/// Entry points for the fuzz targets in `fuzz/`. Only in `cargo fuzz`
/// builds (`--cfg fuzzing`), not in the library.
#[cfg(fuzzing)]
#[doc(hidden)]
pub mod fuzz {
    /// The answer parser of the client: see `client::fuzz`.
    pub fn client_response(data: &[u8]) {
        crate::client::fuzz(data)
    }
}
