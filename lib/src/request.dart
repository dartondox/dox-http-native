import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_native/src/headers.dart';
import 'package:dox_http_native/src/response.dart';
import 'package:dox_http_native/src/server.dart';

/// [HttpRequest] for the native server. The whole body is already read
/// (Rust stops at the size limit with 413), so the stream gives it at once.
class NativeRequest extends Stream<Uint8List> implements HttpRequest {
  NativeRequest(
    this.server,
    this.id,
    this.method,
    this._target,
    this._rawHeaders,
    this._body,
    this._ip,
    this._remotePort, {
    required this._http10,
    this.isWebSocketUpgrade = false,
  });

  final NativeHttpServer server;

  /// id of the request in Rust. The response is sent back with it.
  final int id;

  @override
  final String method;

  final String _target;
  // not final: [detach] swaps views into the batch for copies
  Uint8List _rawHeaders;
  Uint8List _body;
  Uint8List _ip;
  final int _remotePort;
  final bool _http10;

  /// true when Rust found a valid WebSocket upgrade request (GET,
  /// HTTP/1.1, `Upgrade: websocket`, version 13, a key).
  final bool isWebSocketUpgrade;

  NativeHeaders? _headers;

  @override
  NativeHeaders get headers =>
      _headers ??= NativeHeaders.request(_rawHeaders, http10: _http10);

  @override
  late final Uri uri = _uri(_target);

  /// A plain path (no query) skips the full parse. Same result.
  static Uri _uri(String target) {
    bool plain =
        target.startsWith('/') &&
        !target.startsWith('//') &&
        !target.contains('?') &&
        !target.contains('#');
    return plain ? Uri(path: target) : Uri.parse(target);
  }

  NativeResponse? _response;

  @override
  NativeResponse get response => _response ??= NativeResponse(this);

  /// true when the response is sent (or upgraded to a WebSocket)
  bool get isAnswered => _response?.isClosed ?? false;

  /// For the server. The request still runs after its batch: copy the
  /// bytes it holds from [batch], so it does not keep the whole batch
  /// (up to some 100 KB) alive. Bytes from elsewhere (a big body in native
  /// memory) stay as they are.
  // ponytail: a body view the handler already took still holds the batch
  void detach(Uint8List batch) {
    ByteBuffer buffer = batch.buffer;
    NativeHeaders? headers = _headers;
    if (headers != null) {
      headers.detach();
      _rawHeaders = _none;
    } else if (_rawHeaders.buffer == buffer) {
      _rawHeaders = Uint8List.fromList(_rawHeaders);
    }
    if (_body.buffer == buffer) _body = Uint8List.fromList(_body);
    if (_ip.buffer == buffer) _ip = Uint8List.fromList(_ip);
  }

  static final Uint8List _none = Uint8List(0);

  /// the body bytes. Same bytes the stream gives.
  Uint8List get body => _body;

  @override
  int get contentLength => headers.chunkedTransferEncoding ? -1 : _body.length;

  @override
  Uri get requestedUri {
    String host = headers.value(HttpHeaders.hostHeader) ?? '127.0.0.1';
    return Uri.parse('http://$host$_target');
  }

  @override
  late final List<Cookie> cookies = _cookies();

  List<Cookie> _cookies() {
    List<Cookie> out = <Cookie>[];
    for (String header in headers[HttpHeaders.cookieHeader] ?? <String>[]) {
      for (String part in header.split(';')) {
        int eq = part.indexOf('=');
        if (eq <= 0) continue;
        try {
          out.add(
            Cookie(part.substring(0, eq).trim(), part.substring(eq + 1).trim()),
          );
        } on FormatException {
          // skip a cookie with bad characters
        }
      }
    }
    return out;
  }

  @override
  bool get persistentConnection => headers.persistentConnection;

  @override
  String get protocolVersion => _http10 ? '1.0' : '1.1';

  @override
  X509Certificate? get certificate => null;

  @override
  HttpSession get session {
    throw UnsupportedError('dox_http_native has no HttpSession');
  }

  @override
  late final HttpConnectionInfo connectionInfo = _ConnectionInfo(
    InternetAddress.fromRawAddress(_ip),
    _remotePort,
    server.port,
  );

  @override
  StreamSubscription<Uint8List> listen(
    void Function(Uint8List event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    Stream<Uint8List> stream = _body.isEmpty
        ? Stream<Uint8List>.empty()
        : Stream<Uint8List>.value(_body);
    return stream.listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }
}

class _ConnectionInfo implements HttpConnectionInfo {
  _ConnectionInfo(this.remoteAddress, this.remotePort, this.localPort);

  @override
  final InternetAddress remoteAddress;

  @override
  final int remotePort;

  @override
  final int localPort;
}
