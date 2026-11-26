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
}
