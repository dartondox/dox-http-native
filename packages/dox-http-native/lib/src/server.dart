import 'dart:async';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:dox_http_native/src/bindings.dart';
import 'package:dox_http_native/src/headers.dart';
import 'package:dox_http_native/src/request.dart';
import 'package:dox_http_native/src/response.dart';
import 'package:dox_http_native/src/tls.dart';
import 'package:dox_http_native/src/websocket.dart';

/// An [HttpServer] where Rust owns the sockets and parses HTTP. The socket
/// work runs on the thread of this isolate. Handlers run in Dart, like
/// with `dart:io`.
///
/// ```
/// NativeHttpServer server = await NativeHttpServer.bind(3000);
/// server.listen((HttpRequest req) {
///   req.response.write('Hello World');
///   req.response.close();
/// });
/// ```
///
/// Each isolate can bind the same port. The OS shares the connections
/// between them (`SO_REUSEPORT` on Linux and macOS).
class NativeHttpServer extends Stream<HttpRequest> implements HttpServer {
  NativeHttpServer._(
    this._handle,
    this._receive,
    this.port,
    this.address,
    this.tls,
  );

  static bool? _available;
  static String? _loadError;

  /// true when the native library is loaded and works.
  static bool get isAvailable => _available ??= _load();

  /// why [isAvailable] is false, or null.
  static String? get loadError {
    isAvailable;
    return _loadError;
  }

  /// Throws an [UnsupportedError] that says why the native library did not
  /// load and how to fix it. Does nothing when it loaded.
  static void ensureAvailable() {
    if (isAvailable) return;
    throw UnsupportedError(
      'dox_http_native did not load: $_loadError.\n'
      'Fix: install Rust (cargo, see https://rustup.rs), then run the app '
      'again with `dart run` or `dart build cli`. The build hook then builds '
      'the library from source. When you set `lib_path` in pubspec.yaml, '
      'rebuild that library: `cargo build --release` in '
      'packages/dox-http-native/native.',
    );
  }

  static bool _load() {
    try {
      int version = doxVersion();
      if (version != abiVersion) {
        _loadError = 'native library ABI $version, expected $abiVersion';
        return false;
      }
      int rc = doxInit(NativeApi.initializeApiDLData);
      if (rc != 0) {
        _loadError = 'dox_init failed ($rc)';
        return false;
      }
      return true;
    } catch (error) {
      // the build hook found no library for this platform
      _loadError = '$error';
      return false;
    }
  }

  /// Listen on [port]. With no [address] it listens on all interfaces
  /// (IPv6 and IPv4). [address] is an [InternetAddress] or a host name
  /// like `127.0.0.1` or `localhost`, the same as `HttpServer.bind`.
  /// Bodies bigger than [maxBodySize] bytes get 413 from Rust. A
  /// multipart form (`multipart/form-data`) may be up to
  /// [maxMultipartSize] bytes instead (default: [maxBodySize]). A body
  /// with a bigger `Content-Length` and `Expect: 100-continue` gets 413
  /// before it is sent. Any other too big body is read and dropped first,
  /// so the client sees the 413 and not a reset.
  /// A client that sends no body bytes for [bodyTimeout] (idle time) gets
  /// 408. A slow but steady upload is fine.
  /// With [maxConnections], this server holds at most that many connections
  /// open. Then it stops accepting until one closes, and new clients wait
  /// in the listen queue of the OS. Default: no limit.
  /// A keep-alive connection with no new request for [idleTimeout] closes.
  /// A request head must also come whole in this time. Behind a load
  /// balancer, set it longer than the idle timeout of the balancer (eg.
  /// 65 seconds for the 60 seconds of AWS ALB). Else the server may close
  /// a connection the balancer still sends on, and clients get 502.
  /// A client that closed its sending side (a half-close) waits at most
  /// [idleTimeout] for its answer: with no write in that time the
  /// connection closes. Raise it when such clients call slow handlers.
  /// With [http2], the same port also speaks HTTP/2 over plain TCP (h2c
  /// with prior knowledge, no TLS): a connection that starts with the
  /// HTTP/2 preface is HTTP/2, any other is HTTP/1.1. One HTTP/2
  /// connection has at most [maxConcurrentStreams] open requests. A
  /// request the client reset counts until its handler ends the response
  /// (at most 30 seconds). Default: off. An HTTP/2 request body has the
  /// same limits as on HTTP/1.1.
  ///
  /// [http2ResetBurst] and [http2ResetRate] limit stream resets on one
  /// HTTP/2 connection (Rapid Reset, CVE-2023-44487): resets of the client,
  /// and streams the server must reset or refuse. A connection may make
  /// [http2ResetBurst] of them at once (default: 2 x
  /// [maxConcurrentStreams], at least 1000), then [http2ResetRate] a
  /// second (default: 100). Each answer with a body gives half a reset
  /// back. Over the limit the connection drains: GOAWAY `NO_ERROR`, its
  /// open requests still get their answers, and new ones are refused
  /// (`REFUSED_STREAM`), so a proxy retries them on a new connection. If
  /// the resets still go on (one more burst), the connection closes at
  /// once (GOAWAY `ENHANCE_YOUR_CALM`). Behind a proxy (eg. Caddy or
  /// Envoy over h2c) one connection carries many users: raise them when
  /// the proxy sends many cancels.
  ///
  /// With [tls], the port speaks HTTPS only (TLS 1.2 and 1.3, rustls in
  /// Rust). A plain HTTP client gets no answer. The handshake must end in
  /// [idleTimeout]. With [http2], the client picks HTTP/2 or HTTP/1.1 in
  /// the handshake (ALPN `h2` or `http/1.1`). `requestedUri` then has the
  /// `https` scheme. A bad certificate or key file throws a
  /// [TlsException] here, before the server takes a connection. See
  /// [reloadCertificate] for a new certificate without a restart.
  static Future<NativeHttpServer> bind(
    int port, {
    Object? address,
    int maxBodySize = 100 * 1024 * 1024,
    int? maxMultipartSize,
    Duration bodyTimeout = const Duration(seconds: 120),
    int? maxConnections,
    Duration idleTimeout = const Duration(seconds: 30),
    bool http2 = false,
    int maxConcurrentStreams = 250,
    int? http2ResetBurst,
    int http2ResetRate = 100,
    TlsOptions? tls,
  }) async {
    ensureAvailable();
    if (port < 0 || port > 65535) {
      throw ArgumentError.value(port, 'port', 'must be 0 to 65535');
    }
    if (maxConnections != null && maxConnections < 1) {
      throw ArgumentError.value(
        maxConnections,
        'maxConnections',
        'must be 1 or more',
      );
    }
    if (maxConcurrentStreams < 1 || maxConcurrentStreams > 0x7fffffff) {
      throw ArgumentError.value(
        maxConcurrentStreams,
        'maxConcurrentStreams',
        'must be 1 to 2^31 - 1',
      );
    }
    if (http2ResetBurst != null &&
        (http2ResetBurst < 1 || http2ResetBurst > 0xffffffff)) {
      throw ArgumentError.value(
        http2ResetBurst,
        'http2ResetBurst',
        'must be 1 to 2^32 - 1',
      );
    }
    if (http2ResetRate < 0) {
      throw ArgumentError.value(
        http2ResetRate,
        'http2ResetRate',
        'must be 0 or more',
      );
    }
    if (idleTimeout <= Duration.zero) {
      throw ArgumentError.value(
        idleTimeout,
        'idleTimeout',
        'must be more than zero',
      );
    }
    InternetAddress? ip = await _resolve(address);
    List<int> raw = ip?.rawAddress ?? const <int>[];
    int family = ip == null ? 0 : (raw.length == 4 ? 4 : 6);
    int hi = 0, lo = 0;
    for (int i = 0; i < raw.length; i++) {
      if (raw.length == 16 && i < 8) {
        hi = (hi << 8) | raw[i];
      } else {
        lo = (lo << 8) | raw[i];
      }
    }
    late NativeHttpServer server;
    // Rust posts [1] when the sockets have something while Dart is idle,
    // and [2] when a graceful close is done. An empty message is a probe:
    // "is this isolate alive?"
    RawReceivePort receive = RawReceivePort((Object? msg) {
      if (msg is! Uint8List || msg.isEmpty) return;
      if (msg[0] == 2) return server._done();
      server._pump();
    }, 'dox_http_native');
    int handle = doxBind(
      port,
      receive.sendPort.nativePort,
      maxBodySize,
      maxMultipartSize ?? maxBodySize,
      bodyTimeout.inMilliseconds,
      family,
      hi,
      lo,
      maxConnections ?? 0,
      idleTimeout.inMilliseconds,
      http2,
      maxConcurrentStreams,
      http2ResetBurst ?? 0,
      http2ResetRate,
    );
    if (handle <= 0) {
      receive.close();
      int code = handle <= -1000 ? -(handle + 1000) : 0;
      throw SocketException(
        'dox_http_native: bind failed ($handle)',
        osError: OSError('', code),
        address: ip,
        port: port,
      );
    }
    if (tls != null) {
      // before the first `dox_run`: no connection gets in without TLS
      int rc = tlsCall(handle, tls);
      if (rc != 0) {
        doxUnbind(handle);
        receive.close();
        throw tlsError(rc, tls);
      }
    }
    server = NativeHttpServer._(
      handle,
      receive,
      doxBoundPort(handle),
      ip ?? InternetAddress.anyIPv6,
      tls,
    );
    // ignore: deprecated_member_use_from_same_package
    server.idleTimeout = idleTimeout;
    return server;
  }

  /// The certificate files of an HTTPS server, or null for plain HTTP.
  final TlsOptions? tls;

  /// Reads the certificate and key files of [tls] again, for example after
  /// `certbot` renewed them. New connections get the new certificate.
  /// Open connections keep the old one until they close. When a file is
  /// bad, it throws a [TlsException] and the server keeps the old
  /// certificate. All servers of this process that use the same two files
  /// (eg. one per isolate) get the new certificate, so one call is enough.
  /// Dox has no file watcher: the app decides when to call it (eg. on
  /// `SIGHUP`, or from a scheduled task).
  Future<void> reloadCertificate() async {
    TlsOptions? options = tls;
    if (options == null) {
      throw StateError('reloadCertificate: the server has no tls');
    }
    if (_handle == 0) {
      throw StateError('reloadCertificate: the server is closed');
    }
    int rc = tlsCall(_handle, options);
    if (rc != 0) throw tlsError(rc, options);
  }

  /// null for all interfaces. A host name gives its first address.
  static Future<InternetAddress?> _resolve(Object? address) async {
    if (address == null) return null;
    if (address is InternetAddress) return address;
    String host = address.toString();
    InternetAddress? ip = InternetAddress.tryParse(host);
    if (ip != null) return ip;
    List<InternetAddress> found = await InternetAddress.lookup(host);
    return found.first;
  }

  int _handle;
  final RawReceivePort _receive;

  /// open WebSockets of this server, by ws id
  final Map<int, NativeWebSocket> sockets = <int, NativeWebSocket>{};

  /// the id of the next WebSocket
  int nextSocketId = 1;

  /// responses that send their body in parts, by stream id
  final Map<int, NativeResponse> streams = <int, NativeResponse>{};

  /// HTTP/2 requests whose body still comes in parts (gRPC), by request id
  final Map<int, NativeRequest> bodies = <int, NativeRequest>{};
  int _nextStreamId = 1;

  /// open responses that are not streamed and whose `done` is awaited, by
  /// request id. Rust tells when their client is gone, then `done`
  /// completes. Other responses cost nothing here.
  final Map<int, NativeResponse> watched = <int, NativeResponse>{};

  /// ids of open requests whose client left before their `done` was read.
  /// `done` then completes at once. An id goes when its response ends. At
  /// most [maxGoneIds] are kept (the oldest go first), for handlers that
  /// never end their response.
  final Set<int> goneIds = <int>{};

  /// most entries of [goneIds]
  static const int maxGoneIds = 1024;

  /// false once the server is closed
  bool get isOpen => _handle != 0;
  final StreamController<HttpRequest> _requests = StreamController<HttpRequest>(
    sync: true,
  );

  @override
  final int port;

  @override
  final InternetAddress address;

  @override
  bool autoCompress = false;

  /// The gzip level of [autoCompress]: 1 (the default) to 12.
  ///
  /// A low level is fast, and the body is a little bigger. A high level
  /// takes more CPU for a smaller body. For a 6.7 kB JSON body, level 1
  /// took 13.5 us for 1,421 bytes, and level 6 took 32.6 us for 1,270
  /// bytes. The client reads every level the same way.
  ///
  /// Levels 10 to 12 are slow, and each uses 9 MB of memory. One helper
  /// thread gzips all answers of this server, so a slow gzip makes the
  /// other gzipped answers wait.
  int get compressLevel => _compressLevel;
  set compressLevel(int value) {
    RangeError.checkValueInInterval(value, 1, 12, 'compressLevel');
    _compressLevel = value;
  }

  int _compressLevel = 1;

  /// The idle timeout given to [bind]. Setting it later has no effect:
  /// Rust gets the value once, at bind. It stays so code that sets it
  /// still compiles.
  @Deprecated('Setting it has no effect. Pass idleTimeout to bind.')
  @override
  Duration? idleTimeout = const Duration(seconds: 30);

  @override
  String? get serverHeader => _serverHeader;
  String? _serverHeader;

  @override
  set serverHeader(String? value) {
    _serverHeader = value == null ? null : checkHeaderValue(value);
  }

  @override
  set sessionTimeout(int timeout) {}

  /// same defaults as dart:io
  @override
  final NativeHeaders defaultResponseHeaders = NativeHeaders.empty()
    ..set(HttpHeaders.contentTypeHeader, 'text/plain; charset=utf-8')
    ..set('x-frame-options', 'SAMEORIGIN')
    ..set('x-content-type-options', 'nosniff')
    ..set('x-xss-protection', '1; mode=block');

  @override
  HttpConnectionsInfo connectionsInfo() => HttpConnectionsInfo();

  @override
  StreamSubscription<HttpRequest> listen(
    void Function(HttpRequest event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    return _requests.stream.listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }

  Completer<void>? _drained;

  /// Stop listening. Like `dart:io`, open requests still get their answer.
  /// Each connection closes after its current answer, and the future
  /// completes when all are closed. With [force] every connection closes
  /// at once, also when a graceful close still waits.
  @override
  Future<void> close({bool force = false}) async {
    if (_handle == 0) return;
    if (!force) {
      Completer<void>? drained = _drained;
      if (drained == null) {
        drained = _drained = Completer<void>();
        doxShutdown(_handle);
        _schedulePump();
      }
      await drained.future;
    }
    if (_handle != 0) {
      doxUnbind(_handle);
      _handle = 0;
      doxFree(_outPtr, _out.length);
      doxFree(_inPtr, _inList.length);
      _receive.close();
      // not awaited: with no listener the close future never completes
      unawaited(_requests.close());
      // Rust is gone, so no close record comes any more
      for (NativeWebSocket ws in List<NativeWebSocket>.of(sockets.values)) {
        ws.closed(WebSocketStatus.abnormalClosure, '');
      }
      // and no stream gets room to send any more
      for (NativeResponse res in List<NativeResponse>.of(streams.values)) {
        res.streamed(-1);
      }
      streams.clear();
      for (NativeResponse res in List<NativeResponse>.of(watched.values)) {
        res.markGone();
      }
      watched.clear();
      // and no request body part comes any more
      for (NativeRequest req in List<NativeRequest>.of(bodies.values)) {
        req.bodyEnd(1);
      }
      bodies.clear();
      goneIds.clear();
      _done();
    }
  }

  void _done() {
    Completer<void>? drained = _drained;
    if (drained != null && !drained.isCompleted) drained.complete();
  }

  // ---- requests from Rust ----

  bool _inBatch = false;

  /// the requests of the batch being read. Kept, so no list per batch.
  final List<NativeRequest> _batch = <NativeRequest>[];

  /// Frees an external body when Dart is done with it.
  static final Pointer<NativeFinalizerFunction> _freeBody =
      Native.addressOf<NativeFinalizerFunction>(doxFreeBody);

  /// Rust closed the connection of request [id] before it got an answer.
  void _requestGone(int id) {
    NativeResponse? res = watched[id];
    if (res != null) return res.markGone();
    // a request of this batch: its handler already ran. When it answered
    // (or started a stream) nothing waits, so no id is kept.
    for (NativeRequest req in _batch) {
      if (req.id != id) continue;
      NativeResponse res = req.response;
      if (!res.isClosed && !res.headSent) res.markGone();
      return;
    }
    // An open request of an earlier batch whose `done` is not read yet:
    // Rust handled every answer Dart gave before this batch, so it is
    // still live. ponytail: at most [maxGoneIds] ids, the oldest go first.
    // Only a burst of more than that many open requests whose clients
    // left, before their handlers read `done`, loses ids.
    if (goneIds.length >= maxGoneIds) goneIds.remove(goneIds.first);
    goneIds.add(id);
  }

  /// The requests and messages from one `dox_run` call.
  void _onBatch(Uint8List b) {
    ByteData d = ByteData.sublistView(b);
    int p = 0;
    // bytes of WebSocket messages in this batch, acknowledged at the end
    int wsBytes = 0;
    _inBatch = true;
    try {
      while (p + 28 <= b.length) {
        int id = d.getUint64(p, Endian.little);
        int flags = d.getUint32(p + 8, Endian.little);
        int mlen = d.getUint16(p + 12, Endian.little);
        int tlen = d.getUint16(p + 14, Endian.little);
        int hlen = d.getUint32(p + 16, Endian.little);
        int blen = d.getUint32(p + 20, Endian.little);
        int remotePort = d.getUint16(p + 24, Endian.little);
        int q = p + 28;
        Uint8List ip = Uint8List.sublistView(b, q, q + 16);
        q += 16;
        String method = _method(b, q, mlen);
        q += mlen;
        String target = String.fromCharCodes(b, q, q + tlen);
        q += tlen;
        Uint8List headers = Uint8List.sublistView(b, q, q + hlen);
        q += hlen;
        // flag bit 2: the body is in native memory (address, token,
        // length), freed by its finalizer
        Uint8List body = flags & 4 == 4
            ? Pointer<Uint8>.fromAddress(d.getUint64(q, Endian.little))
                  .asTypedList(
                    d.getUint64(q + 16, Endian.little),
                    finalizer: _freeBody,
                    token: Pointer<Void>.fromAddress(
                      d.getUint64(q + 8, Endian.little),
                    ),
                  )
            : Uint8List.sublistView(b, q, q + blen);
        p = q + blen;
        int kind = (flags >> 8) & 0xff;
        if (kind == 4 || kind == 5) {
          // a streamed body: bytes sent to the client, or the client left
          NativeResponse? res = streams[id];
          if (kind == 5) streams.remove(id);
          res?.streamed(kind == 4 ? d.getUint64(q, Endian.little) : -1);
          continue;
        }
        if (kind == 6) {
          // the connection of a request with no answer yet is gone
          _requestGone(id);
          continue;
        }
        if (kind == 7) {
          // a part of a streamed request body. A copy: the request may
          // keep it longer than this batch.
          bodies[id]?.bodyPart(Uint8List.fromList(body));
          continue;
        }
        if (kind == 8) {
          // the end of a streamed request body: the port field is the
          // status (0: a clean end)
          bodies.remove(id)?.bodyEnd(remotePort);
          continue;
        }
        if (kind != 0) {
          // a WebSocket message or close. The port field is the close code.
          if (kind != 3) wsBytes += body.length;
          sockets[id]?.receive(kind, remotePort, body);
          continue;
        }
        _open++;
        NativeRequest req = NativeRequest(
          this,
          id,
          method,
          target,
          headers,
          body,
          ip,
          remotePort,
          http10: flags & 1 == 1,
          http2: flags & 8 == 8,
          isStreamed: flags & 16 == 16,
          isWebSocketUpgrade: flags & 2 == 2,
        );
        if (flags & 16 == 16) bodies[id] = req;
        _batch.add(req);
        _requests.add(req);
      }
    } finally {
      // a request still open after its batch (an async handler, a stream)
      // gets its own copy of its bytes, so it does not keep the batch alive.
      // A WebSocket upgrade counts as answered, but the app often keeps
      // its request for the life of the socket, so it gets a copy too.
      for (NativeRequest req in _batch) {
        if (!req.isAnswered || req.isWebSocketUpgrade) req.detach(b);
      }
      _batch.clear();
      // Rust stops reading WebSocket frames while Dart is too far behind
      if (wsBytes > 0) {
        sendWebSocket(wsBytes, NativeWebSocket.kindAck, 0, const <int>[]);
      }
      _inBatch = false;
      // more requests may wait: always ask Rust again
      _schedulePump();
    }
  }

  static String _method(Uint8List b, int at, int len) {
    // the common ones without a new String. Check every byte: a method
    // like `GOT` must not turn into `GET`.
    if (len == 3 && b[at] == 71 && b[at + 1] == 69 && b[at + 2] == 84) {
      return 'GET';
    }
    if (len == 4 &&
        b[at] == 80 &&
        b[at + 1] == 79 &&
        b[at + 2] == 83 &&
        b[at + 3] == 84) {
      return 'POST';
    }
    if (len == 3 && b[at] == 80 && b[at + 1] == 85 && b[at + 2] == 84) {
      return 'PUT';
    }
    if (len == 6 &&
        b[at] == 68 &&
        b[at + 1] == 69 &&
        b[at + 2] == 76 &&
        b[at + 3] == 69 &&
        b[at + 4] == 84 &&
        b[at + 5] == 69) {
      return 'DELETE';
    }
    return String.fromCharCodes(b, at, at + len);
  }

  // ---- responses to Rust ----

  // native memory: `dox_run` is not a leaf call, so it can not take
  // bytes of the Dart heap
  Pointer<Uint8> _outPtr = doxAlloc(1 << 16);
  late Uint8List _out = _outPtr.asTypedList(1 << 16);
  late ByteData _outData = ByteData.sublistView(_out);
  int _len = 0;

  Pointer<Uint8> _inPtr = doxAlloc(1 << 18);
  late Uint8List _inList = _inPtr.asTypedList(1 << 18);

  /// requests given to the app and not answered yet
  int _open = 0;
  bool _pumpQueued = false;
  int _chain = 0;

  /// How long `dox_run` may wait for new requests. Only when this call
  /// sends a whole answer, and no request and no streamed answer is open:
  /// the clients of the answers send their next requests soon, so a short
  /// wait here gets them without a trip through the idle thread and the
  /// event loop (2 thread wake-ups). In all other cases Dart can have
  /// work that Rust does not see (a timer, a database reply, the next
  /// part of a stream, the answer to a WebSocket message), and a wait
  /// here would hold it up. When nothing comes in this time, Rust hands
  /// the sockets to its idle thread.
  static const int _budgetUs = 1000;

  /// a whole answer was queued after the last `dox_run` call
  bool _answered = false;

  /// The time of the calls that had a wait budget, since the event loop
  /// last had a turn. At [_budgetUs] the next call goes through the event
  /// loop, so the waits of a chain of calls do not add up.
  int _waitedUs = 0;
  final Stopwatch _clock = Stopwatch()..start();

  Uint8List _defaultLines = Uint8List(0);
  int _defaultVersion = -1;

  void _ensure(int extra) {
    int need = _len + extra;
    if (need <= _out.length) return;
    int size = _out.length * 2;
    while (size < need) {
      size *= 2;
    }
    Pointer<Uint8> ptr = doxAlloc(size);
    Uint8List grown = ptr.asTypedList(size)..setRange(0, _len, _out);
    doxFree(_outPtr, _out.length);
    _outPtr = ptr;
    _out = grown;
    _outData = ByteData.sublistView(grown);
  }

  void _line(String name, String value) {
    if (name == HttpHeaders.contentLengthHeader ||
        name == HttpHeaders.transferEncodingHeader ||
        name == HttpHeaders.dateHeader) {
      // Rust sets these (the length and the framing from the body, the date
      // from its clock). A second `date` line would break RFC 9110 6.6.1.
      return;
    }
    if (name == HttpHeaders.connectionHeader &&
        value.toLowerCase().contains('close')) {
      _closeLine = true;
    }
    _ensure(name.length + value.length + 4);
    Uint8List o = _out;
    int at = _len;
    for (int i = 0; i < name.length; i++) {
      o[at++] = name.codeUnitAt(i);
    }
    o[at++] = 58;
    o[at++] = 32;
    for (int i = 0; i < value.length; i++) {
      o[at++] = value.codeUnitAt(i);
    }
    o[at++] = 13;
    o[at++] = 10;
    _len = at;
  }

  Uint8List _defaults() {
    NativeHeaders h = defaultResponseHeaders;
    if (h.version != _defaultVersion) {
      BytesBuilder sb = BytesBuilder();
      h.forEachLine((String name, String value) {
        // Rust sends its own `date`, as for the lines that `_line` skips
        if (name == HttpHeaders.dateHeader) return;
        sb.add('$name: $value\r\n'.codeUnits);
      });
      _defaultLines = sb.takeBytes();
      _defaultVersion = h.version;
    }
    return _defaultLines;
  }

  ContentType? _typedFor;
  Uint8List _typedLines = Uint8List(0);
  int _typedVersion = -1;

  /// the default header lines with another content type. Cached for the
  /// last type, as most apps answer with one type (eg. JSON).
  Uint8List _typed(ContentType type) {
    NativeHeaders h = defaultResponseHeaders;
    if (!identical(type, _typedFor) || h.version != _typedVersion) {
      BytesBuilder sb = BytesBuilder();
      h.forEachLine((String name, String value) {
        if (name == HttpHeaders.contentTypeHeader ||
            name == HttpHeaders.dateHeader) {
          return;
        }
        sb.add('$name: $value\r\n'.codeUnits);
      });
      sb.add('${HttpHeaders.contentTypeHeader}: $type\r\n'.codeUnits);
      _typedLines = sb.takeBytes();
      _typedFor = type;
      _typedVersion = h.version;
    }
    return _typedLines;
  }

  /// Starts a record: the 20 byte head. Header lines may follow, then
  /// [_endRecord] adds the body.
  int _startRecord(int id, int status, int kind) {
    // an answer to a request (whole, WebSocket accept or streamed)
    int k = kind & 0xff;
    if (k == 0 || k == 1 || k == 6) _open--;
    if (k == 0) _answered = true;
    _ensure(20);
    int start = _len;
    _outData.setUint64(start, id, Endian.little);
    _outData.setUint16(start + 8, status, Endian.little);
    _outData.setUint16(start + 10, kind, Endian.little);
    _len += 20;
    return start;
  }

  /// Ends the record that starts at [start]. [streamId] goes before the
  /// body (a streamed answer).
  void _endRecord(int start, List<int> body, [int? streamId]) {
    int extra = streamId == null ? 0 : 8;
    _outData.setUint32(start + 12, _len - start - 20, Endian.little);
    _outData.setUint32(start + 16, body.length + extra, Endian.little);
    _ensure(body.length + extra);
    if (streamId != null) {
      _outData.setUint64(_len, streamId, Endian.little);
      _len += 8;
    }
    _out.setRange(_len, _len + body.length, body);
    _len += body.length;
    _queueFlush();
  }

  void _headerLines(NativeResponse res) {
    NativeHeaders headers = res.headers;
    ContentType? type = headers.sharedType;
    Uint8List? lines = headers.isShared
        ? _defaults()
        : (type == null ? null : _typed(type));
    if (lines != null) {
      _raw(lines);
    } else {
      headers.forEachLine(_line);
    }
    if (_varyLine) _raw(_varyBytes);
    if (_encodingLine) _raw(_encodingBytes);
    _varyLine = false;
    _encodingLine = false;
    String? server = serverHeader;
    if (server != null) _line(HttpHeaders.serverHeader, server);
    if (res.hasCookies) {
      for (Cookie cookie in res.cookies) {
        _line(HttpHeaders.setCookieHeader, cookie.toString());
      }
    }
  }

  /// Queue the response of [req]. Sent at the end of the batch, or in a
  /// microtask when the handler answered later.
  void send(NativeRequest req, NativeResponse res) {
    if (_handle == 0) return;
    List<int> body = res.takeBody();
    _varyLine = false;
    _encodingLine = false;
    // kind bit 8: Rust gzips the body, off this isolate
    bool zip = autoCompress && body.length > 256 && _gzip(req, res);
    // kind bits 12 to 15: the gzip level
    int zipKind = zip ? 0x100 | (_compressLevel << 12) : 0;
    int start = _startRecord(req.id, res.statusCode, zipKind);
    _closeLine = false;
    _headerLines(res);
    // a HEAD answer has no body: it keeps the length the handler set.
    // 1xx, 204 and 304 have no length (like the streamed answer)
    int length = res.headers.contentLength;
    int status = res.statusCode;
    bool noBody =
        (status >= 100 && status < 200) || status == 204 || status == 304;
    if (req.method == 'HEAD' && body.isEmpty && length > 0 && !noBody) {
      _lengthLine(length);
    }
    // kind bit 9: the lines say `connection: close`, Rust closes after
    if (_closeLine) {
      _outData.setUint16(start + 10, zipKind | 0x200, Endian.little);
    }
    _endRecord(start, body);
  }

  /// [_line] wrote `connection: close`
  bool _closeLine = false;

  /// Starts the answer of [req] with a body in parts. [first] is the first
  /// part. Returns the stream id for [sendPart] and [endStream].
  int startStream(NativeRequest req, NativeResponse res, List<int> first) {
    int id = _nextStreamId++;
    if (_handle == 0) return id;
    streams[id] = res;
    int start = _startRecord(req.id, res.statusCode, 6);
    _headerLines(res);
    // Rust drops the content-length line of a full body. Here it is the
    // only way the client knows the length up front.
    // 1xx, 204 and 304 have no body, so no length either (like Rust)
    int length = res.headers.contentLength;
    int status = res.statusCode;
    bool noBody =
        (status >= 100 && status < 200) || status == 204 || status == 304;
    if (length >= 0 && !noBody) _lengthLine(length);
    _endRecord(start, first, id);
    return id;
  }

  void _lengthLine(int length) {
    List<int> line =
        '${HttpHeaders.contentLengthHeader}: $length\r\n'.codeUnits;
    _ensure(line.length);
    _out.setRange(_len, _len + line.length, line);
    _len += line.length;
  }

  /// Queue one part of the streamed body [id].
  ///
  /// A handler that makes parts (an `async*` generator, `addStream`) makes
  /// its next part only after the current microtask. If each part got a
  /// microtask pump, each part would cost its own poll and socket write.
  /// So a part asks for the pump in the next event-loop turn. All parts
  /// made in this microtask burst then go out in one write.
  void sendPart(int id, List<int> data) {
    if (_handle == 0 || data.isEmpty) return;
    _partsWaiting = true;
    try {
      _endRecord(_startRecord(id, 0, 7), data);
    } finally {
      _partsWaiting = false;
    }
  }

  /// true only while [sendPart] queues a record, see [_schedulePump].
  bool _partsWaiting = false;

  /// Ends the streamed body [id]. When not [ok], Rust cuts the connection
  /// (HTTP/2: the stream), so the client sees the body is not whole.
  /// [trailers] go out after the body (HTTP/2 only).
  void endStream(int id, {bool ok = true, NativeHeaders? trailers}) {
    streams.remove(id);
    if (_handle == 0) return;
    int start = _startRecord(id, ok ? 0 : 1, 8);
    if (ok && trailers != null) trailers.forEachLine(_line);
    _endRecord(start, const <int>[]);
  }

  /// Tells Rust the app consumed [bytes] of the streamed body of request
  /// [id] (kind 10). The client may then send that many bytes more.
  void sendBodyAck(int id, int bytes) {
    if (_handle == 0 || bytes <= 0) return;
    int start = _startRecord(id, 0, 10);
    _ensure(8);
    _outData.setUint64(_len, bytes, Endian.little);
    _len += 8;
    _outData.setUint32(start + 12, 0, Endian.little);
    _outData.setUint32(start + 16, 8, Endian.little);
    _queueFlush();
  }

  /// Queue a WebSocket record for Rust: an accept of the request [id], or
  /// a message for the WebSocket [id]. [kind] and [status] are the fields
  /// of the response record, see `NativeWebSocket`.
  void sendWebSocket(
    int id,
    int kind,
    int status,
    List<int> body, {
    String? protocol,
  }) {
    if (_handle == 0) return;
    int start = _startRecord(id, status, kind);
    if (protocol != null) _line('sec-websocket-protocol', protocol);
    _endRecord(start, body);
  }

  void _queueFlush() {
    if (!_inBatch) _schedulePump();
  }

  void _schedulePump() {
    if (_pumpQueued || _handle == 0) return;
    _pumpQueued = true;
    // A streamed part, and now and then any other answer, goes through
    // the event loop: timers and other ports (eg. a database) get a turn
    // under load, and more parts can join this write. A whole answer that
    // comes while such a pump waits also waits one event-loop turn. That
    // is short, and a microtask pump that is already queued is not delayed.
    if (_partsWaiting || ++_chain & 15 == 0 || _waitedUs >= _budgetUs) {
      _waitedUs = 0;
      Timer.run(_pump);
    } else {
      scheduleMicrotask(_pump);
    }
  }

  /// true when the client takes gzip and the body is not encoded yet.
  /// Then it sets `content-encoding` and Rust gzips the body. When the
  /// body could be gzipped, it also adds `Vary: Accept-Encoding`, so a
  /// shared cache does not give gzip to a client that can not read it.
  bool _gzip(NativeRequest req, NativeResponse res) {
    NativeHeaders headers = res.headers;
    if (headers.isShared || headers.sharedType != null) {
      return _gzipShared(req, headers);
    }
    if (headers.value(HttpHeaders.contentEncodingHeader) != null) {
      return false;
    }
    if (_compressed(headers.value(HttpHeaders.contentTypeHeader))) {
      return false;
    }
    if (!_varies(headers[HttpHeaders.varyHeader])) {
      headers.add(HttpHeaders.varyHeader, 'Accept-Encoding');
    }
    if (!_acceptsGzip(req.headers.value(HttpHeaders.acceptEncodingHeader))) {
      return false;
    }
    headers.set(HttpHeaders.contentEncodingHeader, 'gzip');
    return true;
  }

  /// [_gzip] for headers that are still the server defaults, maybe with
  /// a content type. A read or a change would copy them, and the cached
  /// lines of [_headerLines] would not be used. So nothing is changed:
  /// [_headerLines] writes the `vary` and `content-encoding` lines after
  /// the cached lines.
  bool _gzipShared(NativeRequest req, NativeHeaders headers) {
    NativeHeaders defaults = defaultResponseHeaders;
    if (defaults.version != _zipVersion) {
      _zipVersion = defaults.version;
      _defaultsEncoded =
          defaults.value(HttpHeaders.contentEncodingHeader) != null;
      _defaultsVary = _varies(defaults[HttpHeaders.varyHeader]);
      _defaultsCompressed = _compressed(
        defaults.value(HttpHeaders.contentTypeHeader),
      );
    }
    if (_defaultsEncoded) return false;
    ContentType? type = headers.sharedType;
    bool compressed = _defaultsCompressed;
    if (type != null) {
      if (!identical(type, _zipTypeFor)) {
        _zipTypeFor = type;
        _zipTypeCompressed = _compressed(type.toString());
      }
      compressed = _zipTypeCompressed;
    }
    if (compressed) return false;
    _varyLine = !_defaultsVary;
    if (!_acceptsGzip(req.headers.value(HttpHeaders.acceptEncodingHeader))) {
      return false;
    }
    _encodingLine = true;
    return true;
  }

  int _zipVersion = -1;
  bool _defaultsEncoded = false;
  bool _defaultsVary = false;
  bool _defaultsCompressed = false;
  ContentType? _zipTypeFor;
  bool _zipTypeCompressed = false;

  /// set by [_gzipShared] for the next [_headerLines]
  bool _varyLine = false;
  bool _encodingLine = false;

  static final Uint8List _varyBytes = Uint8List.fromList(
    'vary: Accept-Encoding\r\n'.codeUnits,
  );
  static final Uint8List _encodingBytes = Uint8List.fromList(
    'content-encoding: gzip\r\n'.codeUnits,
  );

  void _raw(Uint8List bytes) {
    _ensure(bytes.length);
    _out.setRange(_len, _len + bytes.length, bytes);
    _len += bytes.length;
  }

  /// true when [vary] names `Accept-Encoding` (or is `*`) already.
  static bool _varies(List<String>? vary) {
    if (vary == null) return false;
    for (String v in vary) {
      String l = v.toLowerCase();
      if (l.contains('accept-encoding') || l.trim() == '*') return true;
    }
    return false;
  }

  /// true when [accept] (an `Accept-Encoding` value) lists gzip with a q
  /// above 0. `gzip;q=0` means "not gzip".
  static bool _acceptsGzip(String? accept) {
    if (accept == null || !accept.contains('gzip')) return false;
    for (String part in accept.split(',')) {
      List<String> params = part.split(';');
      String name = params[0].trim().toLowerCase();
      if (name != 'gzip' && name != 'x-gzip') continue;
      for (int i = 1; i < params.length; i++) {
        String p = params[i].trim().toLowerCase();
        if (p.startsWith('q=')) {
          double? q = double.tryParse(p.substring(2).trim());
          if (q != null && q <= 0) return false;
        }
      }
      return true;
    }
    return false;
  }

  /// true for a content type that is compressed already (images, audio,
  /// video, archives, fonts). gzip makes those bigger, not smaller.
  static bool _compressed(String? contentType) {
    if (contentType == null) return false;
    String t = contentType.toLowerCase();
    int semi = t.indexOf(';');
    if (semi >= 0) t = t.substring(0, semi);
    t = t.trim();
    if (t.startsWith('image/')) return t != 'image/svg+xml';
    if (t.startsWith('audio/') || t.startsWith('video/')) return true;
    return _compressedTypes.contains(t);
  }

  static const Set<String> _compressedTypes = <String>{
    'application/zip',
    'application/gzip',
    'application/x-gzip',
    'application/x-bzip2',
    'application/x-xz',
    'application/x-7z-compressed',
    'application/x-rar-compressed',
    'application/vnd.rar',
    'application/zstd',
    'application/x-brotli',
    'font/woff',
    'font/woff2',
  };

  /// Sends the queued answers and takes the next requests, in one call.
  void _pump() {
    _pumpQueued = false;
    if (_handle == 0) return;
    int budget = _answered && _open <= 0 && streams.isEmpty ? _budgetUs : 0;
    _answered = false;
    int start = budget > 0 ? _clock.elapsedMicroseconds : 0;
    int n = doxRun(_handle, _outPtr, _len, _inPtr, _inList.length, budget);
    if (budget > 0) _waitedUs += _clock.elapsedMicroseconds - start;
    _len = 0;
    if (_out.length > 1 << 20) {
      // do not keep a big body buffer forever
      doxFree(_outPtr, _out.length);
      _outPtr = doxAlloc(1 << 16);
      _out = _outPtr.asTypedList(1 << 16);
      _outData = ByteData.sublistView(_out);
    }
    while (n < 0) {
      // one request is bigger than the buffer
      doxFree(_inPtr, _inList.length);
      int size = _inList.length * 2;
      while (size < -n) {
        size *= 2;
      }
      _inPtr = doxAlloc(size);
      _inList = _inPtr.asTypedList(size);
      n = doxRun(_handle, _outPtr, 0, _inPtr, size, 0);
    }
    if (n == 0) {
      _chain = 0;
      _waitedUs = 0;
      return; // Rust posts to the port when more comes
    }
    // a copy: Rust writes the next batch into the same buffer, and the
    // requests keep views of this one
    _onBatch(_inList.sublist(0, n));
    if (_inList.length > 1 << 20) {
      // do not keep a big buffer forever
      doxFree(_inPtr, _inList.length);
      _inPtr = doxAlloc(1 << 18);
      _inList = _inPtr.asTypedList(1 << 18);
    }
  }
}
