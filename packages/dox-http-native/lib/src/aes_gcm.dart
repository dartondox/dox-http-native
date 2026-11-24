import 'dart:ffi';
import 'dart:typed_data';

import 'bindings.dart';

/// AES-256-GCM in Rust, with the AES and carry-less multiply instructions
/// of the CPU. dox_core uses it for encrypted cookies and `AESEncryptor`.
///
/// A sealed value is nonce (12 bytes) + ciphertext + tag (16 bytes).
/// Each [seal] makes a new random nonce.
abstract final class AesGcm {
  static const int keyLength = 32;
  static const int overhead = 28;

  /// [plain] encrypted with [key] (32 bytes). [aad] binds it to a context,
  /// like a cookie name: [open] needs the same [aad].
  static Uint8List seal(Uint8List key, Uint8List plain, Uint8List aad) {
    _checkKey(key);
    Uint8List out = Uint8List(plain.length + overhead);
    int result = doxAesGcmSeal(
      key.address,
      aad.address,
      aad.length,
      plain.address,
      plain.length,
      out.address,
    );
    if (result == -2) {
      throw StateError('The system random source failed');
    }
    if (result != 0) throw ArgumentError('AesGcm.seal: bad arguments');
    return out;
  }

  /// The plain bytes of [data], or null when it was changed, cut, or made
  /// with another [key] or [aad].
  static Uint8List? open(Uint8List key, Uint8List data, Uint8List aad) {
    _checkKey(key);
    if (data.length < overhead) return null;
    Uint8List out = Uint8List(data.length - overhead);
    int length = doxAesGcmOpen(
      key.address,
      aad.address,
      aad.length,
      data.address,
      data.length,
      out.address,
    );
    return length < 0 ? null : out;
  }

  static void _checkKey(Uint8List key) {
    if (key.length != keyLength) {
      throw ArgumentError.value(key.length, 'key', 'must be 32 bytes');
    }
  }
}
