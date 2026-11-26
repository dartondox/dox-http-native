#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| dox_http_native::fuzz::tls_handshake(data));
