import 'dart:convert';
import 'dart:typed_data';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

Uint8List hex(String s) => Uint8List.fromList(<int>[
  for (int i = 0; i < s.length; i += 2)
    int.parse(s.substring(i, i + 2), radix: 16),
]);

void main() {
  Uint8List key = Uint8List(32)..fillRange(0, 32, 7);
  Uint8List aad = utf8.encode('dox_session');

  test('seal then open gives the plain bytes back', () {
    Uint8List plain = utf8.encode('{"user":42}');
    Uint8List sealed = AesGcm.seal(key, plain, aad);
    expect(sealed.length, plain.length + 28);
    expect(AesGcm.open(key, sealed, aad), plain);
  });

  test('an empty plain text works', () {
    Uint8List sealed = AesGcm.seal(key, Uint8List(0), Uint8List(0));
    expect(sealed.length, 28);
    expect(AesGcm.open(key, sealed, Uint8List(0)), isEmpty);
  });

  test('a changed byte, another aad or another key gives null', () {
    Uint8List sealed = AesGcm.seal(key, utf8.encode('hello'), aad);
    expect(AesGcm.open(key, sealed, utf8.encode('other')), isNull);
    expect(AesGcm.open(Uint8List(32), sealed, aad), isNull);
    Uint8List changed = Uint8List.fromList(sealed)..[13] ^= 1;
    expect(AesGcm.open(key, changed, aad), isNull);
    expect(AesGcm.open(key, sealed.sublist(0, 27), aad), isNull);
  });

  test('each seal has a new nonce', () {
    Uint8List a = AesGcm.seal(key, utf8.encode('x'), aad);
    Uint8List b = AesGcm.seal(key, utf8.encode('x'), aad);
    expect(a.sublist(0, 12), isNot(b.sublist(0, 12)));
  });

  test('opens a value made by another AES-GCM (GCM spec test case 16)', () {
    Uint8List specKey = hex(
      'feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308',
    );
    Uint8List sealed = hex(
      'cafebabefacedbaddecaf888'
      '522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa'
      '8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662'
      '76fc6ece0f4e1768cddf8853bb2d551b',
    );
    Uint8List specAad = hex('feedfacedeadbeeffeedfacedeadbeefabaddad2');
    expect(
      AesGcm.open(specKey, sealed, specAad),
      hex(
        'd9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a72'
        '1c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39',
      ),
    );
  });

  test('a key that is not 32 bytes throws', () {
    expect(
      () => AesGcm.seal(Uint8List(16), Uint8List(1), aad),
      throwsArgumentError,
    );
  });
}
