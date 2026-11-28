import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_native/src/headers.dart';
import 'package:dox_http_native/src/response.dart';
import 'package:dox_http_native/src/server.dart';

/// [HttpRequest] for the native server. The whole body is already read
/// (Rust stops at the size limit with 413), so the stream gives it at once.
///
/// An HTTP/2 request with `content-type: application/grpc*` is different:
/// its body comes in parts while the handler runs ([isStreamed]). The
/// client may send more only after the listener took the parts, so a
/// paused listener (or none) holds the client back. When the client
/// resets the stream, the body stream ends with an [HttpException].
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
    this._http2 = false,
    this.isStreamed = false,
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

  /// the request came over HTTP/2
  final bool _http2;

  /// true when the body comes in parts as the handler runs (HTTP/2,
  /// `content-type: application/grpc*`). Then [body] is empty: listen to
  /// the request instead. It has no size limit and no body timeout.
  final bool isStreamed;

  /// true when Rust found a valid WebSocket upgrade request (GET,
  /// HTTP/1.1, `Upgrade: websocket`, version 13, a key).
  final bool isWebSocketUpgrade;

  NativeHeaders? _headers;

  @override
  NativeHeaders get headers =>
      _headers ??= NativeHeaders.request(_rawHeaders, http10: _http10);

  @override
  late final Uri uri = _uri(_target);

  /// The path of the request, with no query. Always the same as
  /// `uri.path`, but a plain target (like `/api/users?page=2`) makes no
  /// [Uri]: the path is the text before `?` as it came.
  late final String path = plainPath(_target) ?? uri.path;

  /// The part of [target] before `?`, when [Uri] gives it back unchanged
  /// as the path. Else null. Plain means: it starts with one `/`, has no
  /// `//`, no `#`, no segment that starts with `.`, and only characters
  /// that a path keeps as they are (no `%`, no space, no non-ASCII).
  static String? plainPath(String target) {
    int n = target.length;
    if (n == 0 || target.codeUnitAt(0) != 0x2f) return null;
    int prev = 0x2f;
    for (int i = 1; i < n; i++) {
      int c = target.codeUnitAt(i);
      if (c == 0x3f) {
        // `?`: the rest is the query. A `#` in it changes the query, not
        // the path, but stay with the full parse for it.
        for (int j = i + 1; j < n; j++) {
          if (target.codeUnitAt(j) == 0x23) return null;
        }
        return target.substring(0, i);
      }
      if (c >= 128 || !_pathSafe[c]) return null;
      // `//`, or a segment that starts with `.` (`/.`, `/..`, `/.well-known`)
      if (prev == 0x2f && (c == 0x2f || c == 0x2e)) return null;
      prev = c;
    }
    return target;
  }

  /// ASCII characters that a [Uri] path keeps as they are: letters,
  /// digits, `-._~`, `!$&'()*+,;=`, `:@` and `/` (RFC 3986 pchar).
  static final List<bool> _pathSafe = List<bool>.generate(128, (int c) {
    if (c >= 0x30 && c <= 0x39) return true;
    if (c >= 0x41 && c <= 0x5a) return true;
    if (c >= 0x61 && c <= 0x7a) return true;
    return "-._~!\$&'()*+,;=:@/".codeUnits.contains(c);
  });

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

  /// the body bytes. Same bytes the stream gives. Empty when [isStreamed].
  Uint8List get body => _body;

  @override
  int get contentLength => isStreamed
      ? headers.contentLength
      : (headers.chunkedTransferEncoding ? -1 : _body.length);

  @override
  Uri get requestedUri {
    // an absolute-form target (RFC 9112 3.2.2) is the URI already
    Uri? absolute = Uri.tryParse(_target);
    if (absolute != null && absolute.hasScheme && absolute.hasAuthority) {
      return absolute;
    }
    String host = headers.value(HttpHeaders.hostHeader) ?? '127.0.0.1';
    String scheme = server.tls == null ? 'http' : 'https';
    return Uri.parse('$scheme://$host$_target');
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
  bool get persistentConnection => _http2 || headers.persistentConnection;

  @override
  String get protocolVersion => _http2 ? '2.0' : (_http10 ? '1.0' : '1.1');

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
    Stream<Uint8List> stream = isStreamed
        ? _parts.stream
        : _body.isEmpty
        ? Stream<Uint8List>.empty()
        : Stream<Uint8List>.value(_body);
    return stream.listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }

  // ---- a body in parts ([isStreamed]) ----

  /// bytes from Rust the listener did not take yet, in one buffer: a
  /// million tiny parts are one chunk, not a million objects. At most the
  /// stream window of Rust (1 MiB): the client waits for our acks.
  final BytesBuilder _waiting = BytesBuilder();

  /// sync: a part counts as taken once `add` returns (the listener's
  /// `onData` ran). A paused listener gets no part, so none is acked.
  late final StreamController<Uint8List> _parts = StreamController<Uint8List>(
    sync: true,
    onListen: _drainSoon,
    onResume: _drainSoon,
    onCancel: _dropAll,
  );
  bool _ended = false;
  bool _cancelled = false;
  bool _drainQueued = false;

  /// For the server: a part of the body came (BODY_PART).
  void bodyPart(Uint8List part) {
    if (_cancelled) {
      // nobody reads any more: drop it, but let the client go on
      server.sendBodyAck(id, part.length);
      return;
    }
    _waiting.add(part);
    _drainSoon();
  }

  /// For the server: the body ended (BODY_END). [status] 0 is a clean end.
  /// 2 means the response ended first (the handler answered before it read
  /// all): also a clean end, with no error. Any other status: the stream
  /// was reset (or the connection died): the parts not taken yet are
  /// dropped and the body stream ends with an error.
  void bodyEnd(int status) {
    if (_ended) return;
    _ended = true;
    if (status == 0 || status == 2) return _drainSoon();
    _waiting.clear();
    if (_cancelled) return;
    _parts.addError(HttpException('stream reset', uri: uri));
    _parts.close();
  }

  /// In a microtask: the listener's code does not run inside `listen` or
  /// inside the message loop of the server.
  void _drainSoon() {
    if (_drainQueued) return;
    _drainQueued = true;
    scheduleMicrotask(_drain);
  }

  void _drain() {
    _drainQueued = false;
    StreamController<Uint8List> parts = _parts;
    if (parts.isClosed) return;
    int taken = 0;
    try {
      // all that waits goes as one chunk, and what the listener got is acked
      if (_waiting.isNotEmpty && parts.hasListener && !parts.isPaused) {
        Uint8List part = _waiting.takeBytes();
        taken = part.length;
        parts.add(part);
      }
    } finally {
      // also when the listener throws: the client must not stall
      server.sendBodyAck(id, taken);
      if (_ended && _waiting.isEmpty) parts.close();
    }
  }

  /// The listener cancelled: drop what waits, and ack it and all later
  /// parts, so the client is not stuck.
  void _dropAll() {
    _cancelled = true;
    int dropped = _waiting.length;
    _waiting.clear();
    server.sendBodyAck(id, dropped);
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
