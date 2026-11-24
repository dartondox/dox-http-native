//! AES-256-GCM for encrypted cookies and `AESEncryptor` of dox_core.
//! The `aes-gcm` crate uses the AES and carry-less multiply instructions of
//! the CPU, so one cookie takes well under a microsecond.
//!
//! Layout of a sealed value: nonce (12 bytes) + ciphertext + tag (16 bytes).
//! It is the same layout as the old pure Dart code, so old values still open.

use aes_gcm::aead::{self, AeadInOut};
use aes_gcm::{Aes256Gcm, KeyInit};

type Nonce = aead::Nonce<Aes256Gcm>;
type Tag = aead::Tag<Aes256Gcm>;

pub const NONCE_LEN: usize = 12;
pub const TAG_LEN: usize = 16;
pub const KEY_LEN: usize = 32;

/// `out` gets nonce + ciphertext + tag, so it is `plain.len() + 28` bytes.
pub fn seal_with_nonce(
    key: &[u8; KEY_LEN],
    nonce: &[u8; NONCE_LEN],
    aad: &[u8],
    plain: &[u8],
    out: &mut [u8],
) {
    let cipher = Aes256Gcm::new(key.into());
    out[..NONCE_LEN].copy_from_slice(nonce);
    let body = &mut out[NONCE_LEN..NONCE_LEN + plain.len()];
    body.copy_from_slice(plain);
    let tag = cipher
        .encrypt_inout_detached(&Nonce::from(*nonce), aad, body.into())
        .expect("a cookie is far below the GCM length limit");
    out[NONCE_LEN + plain.len()..].copy_from_slice(&tag);
}

/// The plain text of `data` (nonce + ciphertext + tag) in `out`, which is
/// `data.len() - 28` bytes. False when the data was changed, or the key or
/// `aad` is wrong.
pub fn open(key: &[u8; KEY_LEN], aad: &[u8], data: &[u8], out: &mut [u8]) -> bool {
    if data.len() < NONCE_LEN + TAG_LEN {
        return false;
    }
    let cipher = Aes256Gcm::new(key.into());
    let (nonce, rest) = data.split_at(NONCE_LEN);
    let (body, tag) = rest.split_at(rest.len() - TAG_LEN);
    let (Ok(nonce), Ok(tag)) = (<&Nonce>::try_from(nonce), <&Tag>::try_from(tag)) else {
        return false;
    };
    out.copy_from_slice(body);
    cipher
        .decrypt_inout_detached(nonce, aad, out.into(), tag)
        .is_ok()
}

/// a slice from a Dart pointer. A zero length may come with any pointer.
unsafe fn bytes<'a>(ptr: *const u8, len: i64) -> Option<&'a [u8]> {
    if len < 0 || (len > 0 && ptr.is_null()) {
        return None;
    }
    if len == 0 {
        return Some(&[]);
    }
    Some(unsafe { std::slice::from_raw_parts(ptr, len as usize) })
}

/// Encrypts `plain` with a new random nonce. `out` must hold
/// `plain_len + 28` bytes. Returns 0, -1 for bad arguments, -2 when the
/// system random source fails.
///
/// # Safety
/// `key` points to 32 bytes. `aad`, `plain` and `out` point to their lengths.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_aes_gcm_seal(
    key: *const u8,
    aad: *const u8,
    aad_len: i64,
    plain: *const u8,
    plain_len: i64,
    out: *mut u8,
) -> i32 {
    let (Some(key), Some(aad), Some(plain)) = (
        unsafe { bytes(key, KEY_LEN as i64) },
        unsafe { bytes(aad, aad_len) },
        unsafe { bytes(plain, plain_len) },
    ) else {
        return -1;
    };
    if out.is_null() {
        return -1;
    }
    let mut nonce = [0u8; NONCE_LEN];
    if getrandom::fill(&mut nonce).is_err() {
        return -2;
    }
    let out = unsafe { std::slice::from_raw_parts_mut(out, plain.len() + NONCE_LEN + TAG_LEN) };
    seal_with_nonce(key.try_into().unwrap(), &nonce, aad, plain, out);
    0
}

/// Decrypts `data` (nonce + ciphertext + tag) into `out`, which must hold
/// `data_len - 28` bytes. Returns the plain length, or -1 when the data was
/// changed, cut, or made with another key or `aad`.
///
/// # Safety
/// `key` points to 32 bytes. `aad`, `data` and `out` point to their lengths.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dox_aes_gcm_open(
    key: *const u8,
    aad: *const u8,
    aad_len: i64,
    data: *const u8,
    data_len: i64,
    out: *mut u8,
) -> i64 {
    let (Some(key), Some(aad), Some(data)) = (
        unsafe { bytes(key, KEY_LEN as i64) },
        unsafe { bytes(aad, aad_len) },
        unsafe { bytes(data, data_len) },
    ) else {
        return -1;
    };
    if data.len() < NONCE_LEN + TAG_LEN {
        return -1;
    }
    let len = data.len() - NONCE_LEN - TAG_LEN;
    if len > 0 && out.is_null() {
        return -1;
    }
    let mut empty = [0u8; 0];
    let out = if len == 0 {
        &mut empty[..]
    } else {
        unsafe { std::slice::from_raw_parts_mut(out, len) }
    };
    if open(key.try_into().unwrap(), aad, data, out) {
        len as i64
    } else {
        // do not leave half decrypted bytes behind
        out.fill(0);
        -1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Test case 16 of "The Galois/Counter Mode of Operation (GCM)"
    /// (McGrew and Viega): AES-256, 96-bit IV, with AAD.
    #[test]
    fn gcm_spec_test_case_16() {
        let key: [u8; 32] = hex("feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308")
            .try_into()
            .unwrap();
        let nonce: [u8; 12] = hex("cafebabefacedbaddecaf888").try_into().unwrap();
        let plain = hex(
            "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a72\
             1c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39",
        );
        let aad = hex("feedfacedeadbeeffeedfacedeadbeefabaddad2");
        let cipher = hex(
            "522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa\
             8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662",
        );
        let tag = hex("76fc6ece0f4e1768cddf8853bb2d551b");

        let mut out = vec![0u8; plain.len() + 28];
        seal_with_nonce(&key, &nonce, &aad, &plain, &mut out);
        assert_eq!(&out[..12], &nonce[..]);
        assert_eq!(&out[12..12 + plain.len()], &cipher[..]);
        assert_eq!(&out[12 + plain.len()..], &tag[..]);

        let mut back = vec![0u8; plain.len()];
        assert!(open(&key, &aad, &out, &mut back));
        assert_eq!(back, plain);
    }

    #[test]
    fn a_changed_byte_or_wrong_aad_fails() {
        let key = [7u8; 32];
        let mut sealed = vec![0u8; 5 + 28];
        unsafe {
            assert_eq!(
                dox_aes_gcm_seal(
                    key.as_ptr(),
                    b"name".as_ptr(),
                    4,
                    b"hello".as_ptr(),
                    5,
                    sealed.as_mut_ptr()
                ),
                0
            );
        }
        let mut out = [0u8; 5];
        let open_with = |data: &[u8], aad: &[u8], out: &mut [u8]| unsafe {
            dox_aes_gcm_open(
                key.as_ptr(),
                aad.as_ptr(),
                aad.len() as i64,
                data.as_ptr(),
                data.len() as i64,
                out.as_mut_ptr(),
            )
        };
        assert_eq!(open_with(&sealed, b"name", &mut out), 5);
        assert_eq!(&out, b"hello");
        assert_eq!(open_with(&sealed, b"other", &mut out), -1);
        assert_eq!(out, [0u8; 5]);
        sealed[14] ^= 1;
        assert_eq!(open_with(&sealed, b"name", &mut out), -1);
    }

    #[test]
    fn two_seals_use_two_nonces() {
        let key = [1u8; 32];
        let mut a = [0u8; 28];
        let mut b = [0u8; 28];
        unsafe {
            dox_aes_gcm_seal(
                key.as_ptr(),
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                a.as_mut_ptr(),
            );
            dox_aes_gcm_seal(
                key.as_ptr(),
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                b.as_mut_ptr(),
            );
        }
        assert_ne!(a[..12], b[..12]);
    }

    #[test]
    fn short_or_bad_input_is_refused() {
        let key = [1u8; 32];
        let mut out = [0u8; 4];
        unsafe {
            assert_eq!(
                dox_aes_gcm_open(
                    key.as_ptr(),
                    std::ptr::null(),
                    0,
                    [0u8; 27].as_ptr(),
                    27,
                    out.as_mut_ptr()
                ),
                -1
            );
            assert_eq!(
                dox_aes_gcm_open(
                    key.as_ptr(),
                    std::ptr::null(),
                    -1,
                    [0u8; 28].as_ptr(),
                    28,
                    out.as_mut_ptr()
                ),
                -1
            );
            assert_eq!(
                dox_aes_gcm_seal(
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                    0,
                    out.as_mut_ptr()
                ),
                -1
            );
        }
    }
}
