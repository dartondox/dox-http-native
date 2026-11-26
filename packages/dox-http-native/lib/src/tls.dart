/// HTTPS for [NativeHttpServer.bind]: the certificate chain and its
/// private key, as PEM files. Rust (rustls) reads them at `bind`, and again
/// on `NativeHttpServer.reloadCertificate`.
///
/// ```
/// NativeHttpServer server = await NativeHttpServer.bind(
///   443,
///   http2: true,
///   tls: TlsOptions(certificatePath: 'cert.pem', keyPath: 'key.pem'),
/// );
/// ```
library;

import 'dart:convert';
import 'dart:ffi';
import 'dart:io';

import 'package:dox_http_native/src/bindings.dart';
import 'package:dox_http_native/src/server.dart';

/// The certificate and key files of a TLS (HTTPS) port.
final class TlsOptions {
  const TlsOptions({required this.certificatePath, required this.keyPath});

  /// A PEM file with the certificate first, then the intermediate
  /// certificates (eg. `fullchain.pem` of certbot).
  final String certificatePath;

  /// A PEM file with the private key of the certificate (PKCS #8, PKCS #1
  /// or SEC1), eg. `privkey.pem` of certbot.
  final String keyPath;

  /// Reads the two files as [NativeHttpServer.bind] does, and throws a
  /// [TlsException] when a file is bad or the key does not fit the
  /// certificate. It opens no port and changes no server. Use it to fail
  /// once at the start of an app, before the app starts its isolates.
  void check() {
    NativeHttpServer.ensureAvailable();
    int rc = tlsCall(0, this);
    if (rc != 0) throw tlsError(rc, this);
  }
}

/// Gives the certificate files of [tls] to the server [handle] (see
/// `doxTls`). With [handle] 0 it only checks them (see `doxTlsCheck`).
/// Returns 0 or the error code.
int tlsCall(int handle, TlsOptions tls) {
  List<int> cert = utf8.encode(tls.certificatePath);
  List<int> key = utf8.encode(tls.keyPath);
  int size = cert.length + key.length;
  Pointer<Uint8> mem = doxAlloc(size);
  try {
    mem.asTypedList(size)
      ..setRange(0, cert.length, cert)
      ..setRange(cert.length, size, key);
    Pointer<Uint8> keyAt = mem + cert.length;
    if (handle == 0) return doxTlsCheck(mem, cert.length, keyAt, key.length);
    return doxTls(handle, mem, cert.length, keyAt, key.length);
  } finally {
    doxFree(mem, size);
  }
}

/// The error of the code that [tlsCall] returned.
TlsException tlsError(int code, TlsOptions tls) {
  String why = switch (code) {
    -1 => 'can not read the certificate file ${tls.certificatePath}',
    -2 => 'no PEM certificate in ${tls.certificatePath}',
    -3 => 'can not read the key file ${tls.keyPath}',
    -4 => 'no PEM private key in ${tls.keyPath}',
    -5 => 'the key in ${tls.keyPath} does not fit the certificate',
    -6 => 'a certificate or key path is not valid UTF-8',
    _ => 'error $code',
  };
  return TlsException('dox_http_native: $why');
}
