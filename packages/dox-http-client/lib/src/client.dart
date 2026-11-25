import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'bindings.dart';

/// True when the native library is loaded and works. A static is per
/// isolate, so each isolate checks the library and calls `dox_client_init`
/// once.
bool? _available;

/// Why [_available] is false, or null.
String? _loadError;

/// Throws an [UnsupportedError] that says why the native library did not
/// load and how to fix it. Does nothing when it loaded.
void _ensureAvailable() {
  if (_available ??= _load()) return;
  throw UnsupportedError(
    'dox_http_client did not load: $_loadError.\n'
    'Fix: install Rust (cargo, see https://rustup.rs), then run the app '
    'again with `dart run` or `dart build cli`. The build hook then builds '
    'the library from source. When you set `lib_path` in pubspec.yaml, '
    'rebuild that library: `cargo build --release` in '
    'packages/dox-http-client/native.',
  );
}

bool _load() {
  try {
    int version = doxClientVersion();
    if (version != abiVersion) {
      _loadError = 'native library ABI $version, expected $abiVersion';
      return false;
    }
    int rc = doxClientInit(NativeApi.initializeApiDLData);
    if (rc != 0) {
      _loadError = 'dox_client_init failed ($rc)';
      return false;
    }
    return true;
  } catch (error) {
    // the build hook found no library for this platform
    _loadError = '$error';
    return false;
  }
}

/// Why a request of a [DoxHttpClient] failed.
enum DoxHttpFailure {
  /// The host name has no address.
  lookupFailed,

  /// No connection to the server. Nothing was sent.
  connectionFailed,

  /// No whole answer within the timeout.
  timeout,

  /// The connection closed before the answer was whole.
  connectionClosed,

  /// The answer is not correct HTTP.
  badResponse,

  /// The answer body is bigger than `maxResponseSize`.
  responseTooBig,

  /// TLS failed. For example the certificate of the server is not
  /// trusted, is expired or is for another host name.
  tlsFailed,

  /// [DoxHttpClient.close] was called before the answer came.
  clientClosed,

  /// The answer was one more redirect after `maxRedirects` of them.
  tooManyRedirects,

  /// The body stream of [DoxHttpClient.sendStreamed] gave more or fewer
  /// bytes than its `contentLength`.
  badRequestBody,

  /// The `abort` future of [DoxHttpClient.sendStreamed] completed before
  /// the end of the answer.
  aborted,
}

/// A request of a [DoxHttpClient] that got no answer.
///
/// An answer with an error status, like 404 or 500, is not an exception:
/// look at [DoxHttpClientResponse.statusCode].
class DoxHttpClientException implements IOException {
  DoxHttpClientException(this.failure, this.url);

  final DoxHttpFailure failure;
  final Uri url;

  @override
  String toString() => 'DoxHttpClientException: ${failure.name}, url = $url';
}

/// The status and the headers of an answer. The body is in the subclasses.
abstract class DoxHttpClientResponseHead {
  DoxHttpClientResponseHead._(this.url, this.statusCode, this._lines);

  /// The URL that gave this answer. After a redirect, it is not the URL
  /// of the request.
  final Uri url;

  final int statusCode;

  /// The header lines from Rust: `name:value\n`, names in lower case.
  final Uint8List _lines;

  /// The headers. The names are in lower case. A header that comes more
  /// than once has its values joined with `, `.
  late final Map<String, String> headers = _parseHeaders();

  /// The reason phrase of the status line, like `OK` or `Not Found`. Null
  /// when the server sent none. An HTTP/2 answer has none.
  late final String? reasonPhrase = _reason();

  String? _reason() {
    // Rust gives it as a first line with no name: `:OK\n`
    if (_lines.isEmpty || _lines[0] != 0x3a) return null;
    int end = _lines.indexOf(0x0a);
    return latin1.decode(_lines.sublist(1, end < 0 ? _lines.length : end));
  }

  /// One header, or null. [name] may be in any case.
  String? header(String name) => headers[name.toLowerCase()];

  Map<String, String> _parseHeaders() {
    Map<String, String> map = <String, String>{};
    for (String line in latin1.decode(_lines).split('\n')) {
      int colon = line.indexOf(':');
      if (colon <= 0) continue;
      String name = line.substring(0, colon);
      String value = line.substring(colon + 1).trim();
      String? old = map[name];
      map[name] = old == null ? value : '$old, $value';
    }
    return Map<String, String>.unmodifiable(map);
  }
}

/// Bytes to JSON in one pass, with no `String` of the whole body between.
final Converter<List<int>, Object?> _utf8Json = const Utf8Decoder().fuse(
  const JsonDecoder(),
);

/// The answer to a request of a [DoxHttpClient].
class DoxHttpClientResponse extends DoxHttpClientResponseHead {
  DoxHttpClientResponse._(
    super.url,
    super.statusCode,
    super.lines,
    this.bodyBytes,
  ) : super._();

  /// The body as bytes.
  final Uint8List bodyBytes;

  /// The body as text (UTF-8).
  late final String body = utf8.decode(bodyBytes, allowMalformed: true);

  /// The body as a JSON object.
  ///
  /// The future fails with a [FormatException] when the body is not
  /// correct UTF-8 or JSON, or is JSON but not an object (for example a
  /// list).
  // ponytail: decodes on this isolate; the Future leaves room to move big
  // bodies to another isolate later without an API change.
  Future<Map<String, dynamic>> json() async {
    Object? value = _utf8Json.convert(bodyBytes);
    if (value is Map<String, dynamic>) return value;
    throw FormatException(
      'The body is not a JSON object: ${value.runtimeType}',
    );
  }
}

/// The answer to a request of [DoxHttpClient.sendStreamed]. The future of
/// the request completes with it when the head of the answer is there. The
/// body comes later, in [body].
class DoxHttpClientStreamedResponse extends DoxHttpClientResponseHead {
  DoxHttpClientStreamedResponse._(
    super.url,
    super.statusCode,
    super.lines,
    this.body,
  ) : super._();

  /// The body as it comes. A stream with one subscription.
  ///
  /// You must listen to it or cancel it. A body that nobody uses keeps its
  /// connection open. The client reads at most about 512 KiB ahead: when the
  /// listener pauses, the client stops to read the connection.
  ///
  /// A failure after the head (the connection closes, a chunk or the gzip
  /// data is not correct, the body is over `maxResponseSize`) is an error
  /// event with a [DoxHttpClientException]. Then the stream closes. When the
  /// subscription is canceled before the end, the client closes the
  /// connection.
  final Stream<Uint8List> body;
}

/// A request that waits for its answer.
abstract class _Pending {
  _Pending(this.url, this.request, this.flags, this.deadline);

  final Uri url;

  /// The HTTP request: head and body.
  final Uint8List request;
  final int flags;

  /// When the request fails with a timeout, in microseconds of
  /// [DoxHttpClient._clock]. 0: never.
  final int deadline;

  /// The addresses of the host, and the one in use.
  List<InternetAddress> addresses = const <InternetAddress>[];
  int index = 0;

  /// The whole timeout of a request with a body stream, in microseconds.
  /// 0: none, or not such a request.
  int uploadWait = 0;

  /// The id of the record in Rust.
  int id = 0;

  void fail(Object error, StackTrace trace);
}

/// A request with the whole answer in one record.
class _WholePending extends _Pending {
  _WholePending(super.url, super.request, super.flags, super.deadline);

  final Completer<DoxHttpClientResponse> completer =
      Completer<DoxHttpClientResponse>();

  @override
  void fail(Object error, StackTrace trace) {
    completer.completeError(error, trace);
  }
}

/// A request of [DoxHttpClient.sendStreamed]: its answer comes in parts,
/// and its body can come in parts.
class _StreamPending extends _Pending {
  _StreamPending(
    this.client,
    super.url,
    super.request,
    super.flags,
    super.deadline,
    this.upload,
    this.contentLength,
  ) {
    controller = StreamController<Uint8List>(
      sync: true,
      onListen: _onListen,
      onPause: pump,
      onResume: pump,
      onCancel: _onCancel,
    );
  }

  final DoxHttpClient client;

  /// The request body, and its length (null: chunked).
  final Stream<List<int>>? upload;
  final int? contentLength;

  final Completer<DoxHttpClientStreamedResponse> head =
      Completer<DoxHttpClientStreamedResponse>();
  late final StreamController<Uint8List> controller;

  // the answer body
  /// Parts from Rust that the listener did not get yet.
  final Queue<Uint8List> queue = Queue<Uint8List>();
  bool gotHead = false;
  bool listened = false;

  /// Rust has sent the end, or the failure.
  bool streamEnded = false;
  Object? failure;
  StackTrace? failureTrace;

  /// The stream is closed, or the request is canceled.
  bool finished = false;
  bool _pumping = false;

  /// Bytes that went to the listener, and bytes of that Rust knows.
  int consumed = 0;
  int credited = 0;

  // the request body
  StreamSubscription<List<int>>? _sub;
  bool _started = false;

  /// The upload ended, or it was stopped.
  bool uploadOver = false;
  bool _paused = false;

  /// Bytes given to Rust (the head and the body), and
  /// bytes that went out.
  int handed = 0;
  int wrote = 0;

  /// Bytes of the body as the caller gave them.
  int given = 0;

  @override
  void fail(Object error, StackTrace trace) {
    if (finished) return;
    _stopUpload();
    if (!gotHead) {
      finished = true;
      head.completeError(error, trace);
      return;
    }
    failure = error;
    failureTrace = trace;
    streamEnded = true;
    pump();
  }

  void onHead(int status, Uint8List lines) {
    gotHead = true;
    Stream<Uint8List> body = controller.stream;
    head.complete(DoxHttpClientStreamedResponse._(url, status, lines, body));
    // the server answered before it had the whole request
    if (upload != null && !uploadOver) _abortUpload();
  }

  void onPart(Uint8List data) {
    queue.addLast(data);
  }

  void onEnd() {
    streamEnded = true;
  }

  void _onListen() {
    listened = true;
    pump();
  }

  void _onCancel() {
    client._cancelStream(this);
  }

  /// Gives the parts to the listener while it does not pause, and tells
  /// Rust how many bytes went there.
  void pump() {
    if (_pumping || finished) return;
    _pumping = true;
    try {
      while (queue.isNotEmpty &&
          controller.hasListener &&
          !controller.isPaused) {
        Uint8List part = queue.removeFirst();
        consumed += part.length;
        controller.add(part);
        if (finished) return;
      }
      if (streamEnded && queue.isEmpty) {
        finished = true;
        Object? error = failure;
        if (error != null) controller.addError(error, failureTrace);
        controller.close();
        return;
      }
      if (!streamEnded && consumed - credited >= DoxHttpClient._creditStep) {
        credited = consumed;
        client._sendCredit(this);
      }
    } finally {
      _pumping = false;
    }
  }

  /// Rust says that [total] bytes of the request went out.
  void onProgress(int total) {
    if (total > wrote) wrote = total;
    if (upload == null || uploadOver) return;
    if (!_started) {
      _started = true;
      try {
        _sub = upload!.listen(
          _onData,
          onError: _onUploadError,
          onDone: _onUploadDone,
          cancelOnError: true,
        );
      } on Object catch (error, trace) {
        _uploadFailed(error, trace);
      }
    } else if (_paused && handed - wrote <= DoxHttpClient._lowWater) {
      _paused = false;
      _sub?.resume();
    }
  }

  void _onData(List<int> data) {
    int n = data.length;
    if (n == 0) return;
    int? length = contentLength;
    if (length != null && given + n > length) {
      _uploadFailed(
        DoxHttpClientException(DoxHttpFailure.badRequestBody, url),
        StackTrace.current,
      );
      return;
    }
    given += n;
    client._sendBody(this, data is Uint8List ? data : Uint8List.fromList(data));
    if (!_paused && handed - wrote > DoxHttpClient._highWater) {
      _paused = true;
      _sub?.pause();
    }
  }

  void _onUploadDone() {
    _sub = null;
    int? length = contentLength;
    if (length != null && given != length) {
      _uploadFailed(
        DoxHttpClientException(DoxHttpFailure.badRequestBody, url),
        StackTrace.current,
      );
      return;
    }
    uploadOver = true;
    client._sendEnd(this, false);
  }

  void _onUploadError(Object error, StackTrace trace) {
    _sub = null;
    _uploadFailed(error, trace);
  }

  /// The request body failed: the request fails with [error].
  void _uploadFailed(Object error, StackTrace trace) {
    if (finished) return;
    client._forget(this);
    if (!gotHead) {
      finished = true;
      head.completeError(error, trace);
      return;
    }
    failure = error;
    failureTrace = trace;
    streamEnded = true;
    pump();
  }

  /// Stops the upload with no word to Rust: the request is over there.
  void _stopUpload() {
    uploadOver = true;
    StreamSubscription<List<int>>? sub = _sub;
    _sub = null;
    sub?.cancel();
    Stream<List<int>>? body = upload;
    if (!_started && body != null) {
      // The body was never read. Cancel it, so its source knows and a
      // `close()` of its sink completes.
      _started = true;
      try {
        body.listen(null).cancel().ignore();
      } on Object {
        // a stream that has a listener, or that can not get one
      }
    }
  }

  /// Stops the upload and tells Rust: the connection is not used again.
  void _abortUpload() {
    _stopUpload();
    client._sendEnd(this, true);
  }
}

/// The request that a redirect asks for.
class _Hop {
  _Hop(this.url, this.method, this.headers, this.toGet);

  final Uri url;
  final String method;
  final Map<String, String>? headers;

  /// The method changed to GET: the body goes.
  final bool toGet;
}

/// The addresses of a host name, kept for [DoxHttpClient._lookupTime].
class _Lookup {
  _Lookup(this.until);

  /// The addresses, when the lookup is done.
  List<InternetAddress>? addresses;

  /// The lookup, while it runs.
  Future<List<InternetAddress>>? pending;

  /// In microseconds of [DoxHttpClient._clock].
  final int until;
}

/// A fast HTTP client. Rust owns the sockets and parses the answers,
/// on the thread of the isolate.
///
/// ```dart
/// DoxHttpClient client = DoxHttpClient();
/// DoxHttpClientResponse res = await client.get(
///   Uri.parse('http://127.0.0.1:8080/users'),
/// );
/// print(res.statusCode);
/// print(res.body);
/// ```
///
/// Make one client and use it for all requests: it keeps the connections
/// open (keep-alive) and uses them again. Each isolate needs its own
/// client.
///
/// It speaks HTTP/1.1, and HTTP/2 over `https://` when the server has it.
/// It follows redirects and decodes a gzip or brotli answer body.
class DoxHttpClient implements Finalizable {
  /// A request with no whole answer in [timeout] fails. `Duration.zero`
  /// is no timeout.
  ///
  /// With [maxConnectionsPerHost] > 0, the client holds at most that many
  /// connections open to one address. More requests wait for a free one.
  /// An HTTP/2 connection that takes no new requests (the server closes
  /// it, or it gave no answer in the timeout) does not count while it ends
  /// its requests.
  ///
  /// With [maxResponseSize] > 0, a request whose answer body has more
  /// bytes fails.
  ///
  /// For `https://`, the client trusts the root certificates of Mozilla.
  /// [trustedCertificates] adds your own, as PEM text: for example the
  /// certificate authority of your company, or a self-signed certificate.
  ///
  /// A connection with no request for [idleTimeout] closes.
  /// `Duration.zero` keeps idle connections open.
  ///
  /// With [followRedirects], the client goes to the new address of a
  /// redirect (301, 302, 303, 307, 308), at most [maxRedirects] times for
  /// one request.
  ///
  /// With [http2], an `https://` connection uses HTTP/2 when the server
  /// has it: the requests to one server then share one connection. With
  /// `http2: false`, the client speaks HTTP/1.1 only.
  ///
  /// [lookup] gives the addresses of a host name. The default is
  /// `InternetAddress.lookup`. The client connects only to the addresses
  /// that [lookup] gives, in their sequence, and keeps them for some
  /// seconds. TLS still checks the certificate of the host name. When
  /// [lookup] fails or gives no address, the request fails with
  /// [DoxHttpFailure.lookupFailed]. The client does not call [lookup] for a
  /// URL whose host is an IP address. Use it to refuse addresses, for
  /// example those of your own network when a user gives the URL.
  DoxHttpClient({
    this.timeout = const Duration(seconds: 30),
    this.followRedirects = true,
    this.maxRedirects = 5,
    this.http2 = true,
    Duration idleTimeout = const Duration(seconds: 15),
    int maxConnectionsPerHost = 0,
    int maxResponseSize = 0,
    String? trustedCertificates,
    Future<List<InternetAddress>> Function(String host)? lookup,
  }) : _lookup = lookup ?? InternetAddress.lookup {
    _ensureAvailable();
    Uint8List ca = utf8.encode(trustedCertificates ?? '');
    Pointer<Uint8> caPtr = doxClientAlloc(ca.length);
    caPtr.asTypedList(ca.length).setAll(0, ca);
    // The port must not hold the client: a client that nobody uses can
    // then be collected, and the finalizers close it.
    WeakReference<DoxHttpClient> weak = WeakReference<DoxHttpClient>(this);
    _receive = RawReceivePort((Object? _) => weak.target?._onWake())
      ..keepIsolateAlive = false;
    _handle = doxClientNew(
      _receive.sendPort.nativePort,
      maxConnectionsPerHost,
      maxResponseSize,
      _ceilMs(idleTimeout.inMicroseconds),
      caPtr,
      ca.length,
      http2 ? 1 : 0,
    );
    doxClientFree(caPtr, ca.length);
    if (_handle <= 0) {
      _receive.close();
      if (_handle == -3) {
        throw ArgumentError.value(
          trustedCertificates,
          'trustedCertificates',
          'must be certificates as PEM text',
        );
      }
      throw StateError('dox_client_new failed ($_handle)');
    }
    _finalizer.attach(this, Pointer<Void>.fromAddress(_handle), detach: this);
    _ports.attach(this, _receive, detach: this);
    _sizeOut(_outSize);
    _sizeIn(_inSize);
  }

  /// The timeout of a request that gives none.
  final Duration timeout;

  /// If the client goes to the new address of a redirect, for a request
  /// that does not say.
  final bool followRedirects;

  /// The most redirects of one request. One more fails the request with
  /// [DoxHttpFailure.tooManyRedirects].
  final int maxRedirects;

  /// If an `https://` connection uses HTTP/2 when the server has it.
  final bool http2;

  /// Gives the addresses of a host name.
  final Future<List<InternetAddress>> Function(String host) _lookup;

  /// Closes the native client (sockets, helper thread, buffers) of a
  /// client that was collected or whose isolate ended.
  static final NativeFinalizer _finalizer = NativeFinalizer(
    Native.addressOf<NativeFinalizerFunction>(doxClientClose),
  );

  /// Closes the port of a client that was collected.
  static final Finalizer<RawReceivePort> _ports = Finalizer<RawReceivePort>(
    (RawReceivePort port) => port.close(),
  );

  /// The clients with an open request. A client in this set is not
  /// collected, so each of its requests gets its answer.
  static final Set<DoxHttpClient> _busy = <DoxHttpClient>{};

  /// The most host names with kept addresses before the old ones go.
  static const int _maxLookups = 256;

  /// The most bytes of one request (head and body).
  static const int _maxRequest = 0x7FFFFFFF;

  /// [microseconds] in milliseconds for Rust, not below 1 when it is not 0.
  static int _ceilMs(int microseconds) {
    if (microseconds <= 0) return 0;
    return ((microseconds + 999) ~/ 1000).clamp(1, 0xFFFFFFFF);
  }

  static const int _reqHead = 40;
  static const int _resHead = 24;
  static const int _outSize = 1 << 16;
  static const int _inSize = 1 << 18;

  /// A buffer over this size is not kept after a call.
  static const int _keepSize = 1 << 20;

  /// Request flags, see native/src/client.rs.
  static const int _headOnly = 1;
  static const int _again = 2;
  static const int _tls = 4;
  static const int _decode = 8;
  static const int _stream = 16;
  static const int _upload = 32;
  static const int _chunked = 64;

  /// Request kinds and answer kinds, see native/src/client.rs.
  static const int _qPart = 1;
  static const int _qEnd = 2;
  static const int _qCredit = 3;
  static const int _qCancel = 4;
  static const int _rWhole = 0;
  static const int _rHead = 1;
  static const int _rPart = 2;
  static const int _rEnd = 3;
  static const int _rProgress = 4;

  /// A request body stream is paused while more than [_highWater] bytes
  /// that it gave did not go out. It goes on at [_lowWater].
  static const int _highWater = 256 * 1024;
  static const int _lowWater = _highWater ~/ 2;

  /// Dart tells Rust how many answer body bytes it used at each step of
  /// this size, so Rust may read on. Rust stops to read at 256 KiB.
  static const int _creditStep = 64 * 1024;

  /// The most bytes of a request body in one record.
  static const int _partMax = 256 * 1024;

  /// The most bytes of a redirect body that the client reads to keep the
  /// connection. After them it closes the connection.
  static const int _redirectBody = 64 * 1024;

  /// How long the addresses of a host name are kept.
  static const Duration _lookupTime = Duration(seconds: 30);

  /// Methods that are safe to send again (RFC 9110 9.2.2).
  static const Set<String> _idempotent = <String>{
    'GET',
    'HEAD',
    'PUT',
    'DELETE',
    'OPTIONS',
    'TRACE',
  };

  /// The statuses that send the client to a new address.
  static const Set<int> _redirects = <int>{301, 302, 303, 307, 308};

  /// Headers that must not go to a different server after a redirect.
  static const Set<String> _private = <String>{
    'authorization',
    'cookie',
    'host',
    'proxy-authorization',
  };

  /// Headers that describe a body. They go when a redirect drops the body.
  static const Set<String> _aboutBody = <String>{
    'content-encoding',
    'content-language',
    'content-location',
    'content-type',
  };

  static final Uint8List _noName = Uint8List(0);
  static final RegExp _token = RegExp(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$");
  static final RegExp _badValue = RegExp(r'[\r\n\x00]');

  late final RawReceivePort _receive;
  late final int _handle;
  final Stopwatch _clock = Stopwatch()..start();
  bool _closed = false;

  /// The request records for the next [_run]. Rust owns the memory of
  /// both buffers and frees it with the client.
  int _outCap = 0;
  late Pointer<Uint8> _outPtr;
  late Uint8List _out;
  int _len = 0;

  /// The answer records of the last [_run].
  int _inCap = 0;
  late Pointer<Uint8> _inPtr;
  late Uint8List _in;

  int _nextId = 1;
  bool _queued = false;
  final Map<int, _Pending> _open = <int, _Pending>{};
  final Map<String, _Lookup> _lookups = <String, _Lookup>{};

  /// Sends a GET request.
  Future<DoxHttpClientResponse> get(
    Uri url, {
    Map<String, String>? headers,
    Duration? timeout,
  }) {
    return send('GET', url, headers: headers, timeout: timeout);
  }

  /// Sends a HEAD request. The answer has no body.
  Future<DoxHttpClientResponse> head(
    Uri url, {
    Map<String, String>? headers,
    Duration? timeout,
  }) {
    return send('HEAD', url, headers: headers, timeout: timeout);
  }

  /// Sends a POST request with [body] (text, sent as UTF-8) or
  /// [bodyBytes].
  Future<DoxHttpClientResponse> post(
    Uri url, {
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
    Duration? timeout,
  }) {
    return send(
      'POST',
      url,
      headers: headers,
      body: body,
      bodyBytes: bodyBytes,
      timeout: timeout,
    );
  }

  /// Sends a PUT request with [body] (text, sent as UTF-8) or [bodyBytes].
  Future<DoxHttpClientResponse> put(
    Uri url, {
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
    Duration? timeout,
  }) {
    return send(
      'PUT',
      url,
      headers: headers,
      body: body,
      bodyBytes: bodyBytes,
      timeout: timeout,
    );
  }

  /// Sends a PATCH request with [body] (text, sent as UTF-8) or
  /// [bodyBytes].
  Future<DoxHttpClientResponse> patch(
    Uri url, {
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
    Duration? timeout,
  }) {
    return send(
      'PATCH',
      url,
      headers: headers,
      body: body,
      bodyBytes: bodyBytes,
      timeout: timeout,
    );
  }

  /// Sends a DELETE request.
  Future<DoxHttpClientResponse> delete(
    Uri url, {
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
    Duration? timeout,
  }) {
    return send(
      'DELETE',
      url,
      headers: headers,
      body: body,
      bodyBytes: bodyBytes,
      timeout: timeout,
    );
  }

  /// Sends a request with any [method], like `OPTIONS`.
  ///
  /// The body is [body] (text, sent as UTF-8) or [bodyBytes], not both.
  /// The client sets `host`, `content-length` and `accept-encoding`. A
  /// request with its own `accept-encoding` header or with a `range`
  /// header gets the body as the server sent it. [timeout] is for this
  /// request only: `Duration.zero` is no timeout. It includes the
  /// redirects. [followRedirects] and [maxRedirects] are for this request
  /// only.
  ///
  /// The future fails with a [DoxHttpClientException] when no answer
  /// comes.
  Future<DoxHttpClientResponse> send(
    String method,
    Uri url, {
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
    Duration? timeout,
    bool? followRedirects,
    int? maxRedirects,
  }) {
    int wait = (timeout ?? this.timeout).inMicroseconds;
    int deadline = wait > 0 ? _clock.elapsedMicroseconds + wait : 0;
    Future<DoxHttpClientResponse> first = _once(
      method,
      url,
      headers,
      body,
      bodyBytes,
      deadline,
    );
    if (!(followRedirects ?? this.followRedirects)) return first;
    return _follow(
      first,
      method,
      headers,
      body,
      bodyBytes,
      deadline,
      maxRedirects ?? this.maxRedirects,
    );
  }

  /// Sends a request and gives the answer when its head is there. The body
  /// of the answer comes in [DoxHttpClientStreamedResponse.body], and the
  /// request body can be a stream: both are never whole in memory.
  ///
  /// [body] is the request body. [contentLength] is its length: the client
  /// sends it as `content-length`. With no [contentLength], the client
  /// sends the body with `transfer-encoding: chunked`. A [body] that gives
  /// more or fewer bytes than [contentLength] fails the request with
  /// [DoxHttpFailure.badRequestBody]. A [body] that fails with an error
  /// fails the request with that error and closes the connection. Your
  /// headers must not have `content-length` or `transfer-encoding`.
  ///
  /// The client listens to [body] when the connection to the server works.
  /// It pauses the subscription while more than 256 KiB wait to go out, and
  /// so the memory stays small. When the server answers before the whole
  /// body went out (for example 401 or 413), the client cancels the
  /// subscription, gives you that answer and does not use the connection
  /// again.
  ///
  /// You must listen to the answer body or cancel it, also when you do not
  /// need it. A body that nobody uses keeps its connection open until
  /// [close].
  ///
  /// [timeout] is for the time until the head of the answer is there. After
  /// it, there is no timeout: a big body can take long. With a [body], the
  /// timeout starts again each time bytes of the body go out. Thus a big
  /// body can go out, and a server that does not read fails the request.
  ///
  /// With [followRedirects], a request with no [body] goes to the new
  /// address of a redirect like [send], at most [maxRedirects] times. A
  /// request with a [body] never
  /// follows: the stream can not be read again, so you get the redirect
  /// answer. The client sends a request with a [body] only once.
  ///
  /// When [abort] completes, the request stops and the client closes its
  /// connection. Before the head of the answer, the future fails with
  /// [DoxHttpFailure.aborted]. After it, the body gets that failure as an
  /// error event and closes. After the end of the body, [abort] does
  /// nothing. [abort] must not complete with an error.
  ///
  /// The future fails with a [DoxHttpClientException] when no head comes.
  Future<DoxHttpClientStreamedResponse> sendStreamed(
    String method,
    Uri url, {
    Map<String, String>? headers,
    Stream<List<int>>? body,
    int? contentLength,
    Duration? timeout,
    bool? followRedirects,
    int? maxRedirects,
    Future<void>? abort,
  }) {
    if (body == null && contentLength != null) {
      throw ArgumentError.value(contentLength, 'contentLength', 'needs a body');
    }
    if (contentLength != null && contentLength < 0) {
      throw ArgumentError.value(contentLength, 'contentLength');
    }
    int wait = (timeout ?? this.timeout).inMicroseconds;
    int deadline = wait > 0 ? _clock.elapsedMicroseconds + wait : 0;
    Future<DoxHttpClientStreamedResponse> first = _streamOnce(
      method,
      url,
      headers,
      body,
      contentLength,
      deadline,
      wait > 0 ? wait : 0,
      abort,
    );
    if (body != null || !(followRedirects ?? this.followRedirects)) {
      return first;
    }
    return _followStreamed(
      first,
      method,
      headers,
      deadline,
      abort,
      maxRedirects ?? this.maxRedirects,
    );
  }

  /// Gives the answer of [first], or of the address its redirects go to.
  Future<DoxHttpClientResponse> _follow(
    Future<DoxHttpClientResponse> first,
    String method,
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
    int deadline,
    int maxRedirects,
  ) async {
    DoxHttpClientResponse res = await first;
    for (int hop = 0; ; hop++) {
      _Hop? next = _hop(res, method, headers, hop, maxRedirects);
      if (next == null) return res;
      method = next.method;
      headers = next.headers;
      if (next.toGet) {
        body = null;
        bodyBytes = null;
      }
      res = await _once(method, next.url, headers, body, bodyBytes, deadline);
    }
  }

  /// Like [_follow], for a streamed request with no body.
  Future<DoxHttpClientStreamedResponse> _followStreamed(
    Future<DoxHttpClientStreamedResponse> first,
    String method,
    Map<String, String>? headers,
    int deadline,
    Future<void>? abort,
    int maxRedirects,
  ) async {
    DoxHttpClientStreamedResponse res = await first;
    for (int hop = 0; ; hop++) {
      _Hop? next;
      try {
        next = _hop(res, method, headers, hop, maxRedirects);
      } on Object {
        unawaited(_discard(res, deadline));
        rethrow;
      }
      if (next == null) return res;
      method = next.method;
      headers = next.headers;
      await _discard(res, deadline);
      res = await _streamOnce(
        method,
        next.url,
        headers,
        null,
        null,
        deadline,
        0,
        abort,
      );
    }
  }

  /// Reads the body of a redirect that the client follows, so its connection
  /// can be used again. A body that is big or slow is canceled: that closes
  /// the connection.
  static Future<void> _discard(
    DoxHttpClientStreamedResponse res,
    int deadline,
  ) {
    Completer<void> done = Completer<void>();
    int left = _redirectBody;
    late StreamSubscription<Uint8List> sub;
    sub = res.body.listen(
      (Uint8List part) {
        left -= part.length;
        if (left < 0) {
          sub.cancel();
          if (!done.isCompleted) done.complete();
        }
      },
      onError: (Object _) {
        if (!done.isCompleted) done.complete();
      },
      onDone: () {
        if (!done.isCompleted) done.complete();
      },
      cancelOnError: true,
    );
    return done.future.timeout(
      const Duration(seconds: 10),
      onTimeout: () {
        sub.cancel();
      },
    );
  }

  /// The request that a redirect asks for, or null when [res] is not a
  /// redirect that the client follows. [hop] counts the redirects so far.
  _Hop? _hop(
    DoxHttpClientResponseHead res,
    String method,
    Map<String, String>? headers,
    int hop,
    int maxRedirects,
  ) {
    String? location = res.header('location');
    if (location == null ||
        location.isEmpty ||
        !_redirects.contains(res.statusCode)) {
      return null;
    }
    Uri url = res.url;
    Uri next;
    try {
      next = url.resolve(location);
    } on FormatException {
      return null;
    }
    bool tls = next.scheme == 'https';
    if ((!tls && next.scheme != 'http') || next.host.isEmpty) return null;
    if (hop >= maxRedirects) {
      throw DoxHttpClientException(DoxHttpFailure.tooManyRedirects, next);
    }
    if (_closed) {
      throw DoxHttpClientException(DoxHttpFailure.clientClosed, next);
    }
    // RFC 9110 15.4: a 303 answer is read with GET. For 301 and 302,
    // clients change POST to GET.
    int status = res.statusCode;
    bool toGet =
        (status == 303 && method != 'GET' && method != 'HEAD') ||
        ((status == 301 || status == 302) && method == 'POST');
    bool sameServer =
        next.scheme == url.scheme &&
        next.host == url.host &&
        next.port == url.port;
    if (toGet) method = 'GET';
    if (headers != null && (toGet || !sameServer)) {
      headers = <String, String>{
        for (MapEntry<String, String> header in headers.entries)
          if (!(toGet && _aboutBody.contains(header.key.toLowerCase())) &&
              !(!sameServer && _private.contains(header.key.toLowerCase())))
            header.key: header.value,
      };
    }
    return _Hop(next, method, headers, toGet);
  }

  /// Sends one request. [deadline] is in microseconds of [_clock], 0 is
  /// none.
  Future<DoxHttpClientResponse> _once(
    String method,
    Uri url,
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
    int deadline,
  ) {
    if (_closed) throw StateError('the client is closed');
    bool tls = url.scheme == 'https';
    if ((!tls && url.scheme != 'http') || url.host.isEmpty) {
      throw ArgumentError.value(
        url,
        'url',
        'must be an http:// or https:// URL',
      );
    }
    if (body != null && bodyBytes != null) {
      throw ArgumentError('give body or bodyBytes, not both');
    }
    if (!_token.hasMatch(method)) {
      throw ArgumentError.value(method, 'method');
    }
    int flags = _idempotent.contains(method) ? _again : 0;
    if (method == 'HEAD') flags |= _headOnly;
    if (tls) flags |= _tls;
    // the client asks for a coded body, so Rust decodes it
    // Not for a part of a body (`range`): a part of a gzip body can not
    // be decoded.
    bool coded =
        !_hasHeader(headers, 'accept-encoding') &&
        !_hasHeader(headers, 'range');
    if (coded) flags |= _decode;
    Uint8List request = _encode(method, url, headers, body, bodyBytes, coded);
    // the length of a request record is 32 bits
    if (request.length > _maxRequest) {
      throw ArgumentError('the request is too big (over 2 GiB)');
    }
    _WholePending pending = _WholePending(url, request, flags, deadline);
    _start(pending);
    return pending.completer.future;
  }

  /// Sends one streamed request, see [sendStreamed].
  Future<DoxHttpClientStreamedResponse> _streamOnce(
    String method,
    Uri url,
    Map<String, String>? headers,
    Stream<List<int>>? body,
    int? contentLength,
    int deadline,
    int wait,
    Future<void>? abort,
  ) {
    if (_closed) throw StateError('the client is closed');
    bool tls = url.scheme == 'https';
    if ((!tls && url.scheme != 'http') || url.host.isEmpty) {
      throw ArgumentError.value(
        url,
        'url',
        'must be an http:// or https:// URL',
      );
    }
    if (!_token.hasMatch(method)) {
      throw ArgumentError.value(method, 'method');
    }
    int flags = _stream;
    // a request with a body stream can not be sent again
    if (body != null) {
      flags |= _upload;
      // no length: HTTP/1.1 sends the body in chunks
      if (contentLength == null) flags |= _chunked;
    } else if (_idempotent.contains(method)) {
      flags |= _again;
    }
    if (method == 'HEAD') flags |= _headOnly;
    if (tls) flags |= _tls;
    // see [_once]
    bool coded =
        !_hasHeader(headers, 'accept-encoding') &&
        !_hasHeader(headers, 'range');
    if (coded) flags |= _decode;
    Uint8List request = _encode(
      method,
      url,
      headers,
      null,
      null,
      coded,
      stream: body != null,
      streamLength: contentLength,
    );
    _StreamPending pending = _StreamPending(
      this,
      url,
      request,
      flags,
      deadline,
      body,
      contentLength,
    );
    if (body != null) pending.uploadWait = wait;
    _start(pending);
    // Rust forgets the request and closes its connection
    abort?.then(
      (_) => pending._uploadFailed(
        DoxHttpClientException(DoxHttpFailure.aborted, url),
        StackTrace.current,
      ),
    );
    return pending.head.future;
  }

  /// Looks up the host of [pending] and sends it.
  void _start(_Pending pending) {
    InternetAddress? address = InternetAddress.tryParse(pending.url.host);
    if (address != null) {
      pending.addresses = <InternetAddress>[address];
      _submit(pending);
    } else {
      _resolve(pending);
    }
  }

  /// Closes all connections. Requests with no answer yet fail with
  /// [DoxHttpFailure.clientClosed]. The client can not be used after it.
  void close() {
    if (_closed) return;
    _closed = true;
    _finalizer.detach(this);
    _ports.detach(this);
    _busy.remove(this);
    // Rust also frees the 2 buffers
    doxClientClose(Pointer<Void>.fromAddress(_handle));
    _receive.close();
    List<_Pending> open = _open.values.toList();
    _open.clear();
    for (_Pending pending in open) {
      _fail(pending, DoxHttpFailure.clientClosed);
    }
  }

  /// Rust forgets the request of [pending], and the client does too.
  void _forget(_StreamPending pending) {
    _open.remove(pending.id);
    pending._stopUpload();
    if (_closed) return;
    _record(pending.id, _qCancel, 0);
    _idleCheck();
  }

  /// The listener canceled the answer body, or nobody could listen to it.
  void _cancelStream(_StreamPending pending) {
    if (pending.finished) return;
    pending.finished = true;
    pending.queue.clear();
    _forget(pending);
  }

  /// No request is open: the isolate may end.
  void _idleCheck() {
    if (_open.isEmpty) {
      _receive.keepIsolateAlive = false;
      _busy.remove(this);
    }
  }

  /// Writes a record of [kind] for request [id], with [payload] bytes of
  /// data after its head. Returns the start of the data in [_out].
  int _record(int id, int kind, int payload) {
    int size = _reqHead + payload;
    if (_len + size > _outCap) {
      int cap = _outCap;
      while (cap < _len + size) {
        cap *= 2;
      }
      _sizeOut(cap);
    }
    int at = _len;
    _out.fillRange(at, at + _reqHead, 0);
    ByteData data = ByteData.sublistView(_out, at, at + _reqHead);
    data.setUint32(0, size, Endian.little);
    data.setUint64(4, id, Endian.little);
    data.setUint8(38, kind);
    _len += size;
    if (!_queued) {
      // one call for all records of this turn of the event loop
      _queued = true;
      scheduleMicrotask(_run);
    }
    return at + _reqHead;
  }

  /// Gives Rust bytes of the request body of [pending], in parts. Rust
  /// does the framing: it knows if the connection is HTTP/1.1 or HTTP/2.
  void _sendBody(_StreamPending pending, Uint8List bytes) {
    if (_closed) return;
    for (int at = 0; at < bytes.length; at += _partMax) {
      int end = at + _partMax < bytes.length ? at + _partMax : bytes.length;
      int start = _record(pending.id, _qPart, end - at);
      _out.setRange(start, start + end - at, bytes, at);
      pending.handed += end - at;
    }
  }

  /// The request body is whole, or the caller stops it ([aborted]).
  void _sendEnd(_StreamPending pending, bool aborted) {
    if (_closed) return;
    _record(pending.id, _qEnd, 0);
    // the flag is in the head of the record
    int at = _len - _reqHead;
    _out[at + 16] = aborted ? 1 : 0;
  }

  /// Tells Rust how many answer body bytes the listener got.
  void _sendCredit(_StreamPending pending) {
    if (_closed) return;
    _record(pending.id, _qCredit, 0);
    int at = _len - _reqHead;
    ByteData.sublistView(
      _out,
      at,
      at + _reqHead,
    ).setUint64(16, pending.consumed, Endian.little);
  }

  static bool _hasHeader(Map<String, String>? headers, String name) {
    if (headers == null) return false;
    return headers.keys.any((String key) => key.toLowerCase() == name);
  }

  /// The request line, the headers and the body as bytes.
  static Uint8List _encode(
    String method,
    Uri url,
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
    bool coded, {
    bool stream = false,
    int? streamLength,
  }) {
    String target = url.path.isEmpty ? '/' : url.path;
    if (url.hasQuery) target = '$target?${url.query}';
    StringBuffer head = StringBuffer('$method $target HTTP/1.1\r\n');
    bool hasHost = false;
    bool hasType = false;
    if (headers != null) {
      for (MapEntry<String, String> header in headers.entries) {
        String name = header.key.toLowerCase();
        if (!_token.hasMatch(name) || _badValue.hasMatch(header.value)) {
          throw ArgumentError.value(header.key, 'headers', 'bad header');
        }
        if (name == 'content-length' || name == 'transfer-encoding') {
          throw ArgumentError.value(
            header.key,
            'headers',
            'the client sets this header',
          );
        }
        if (name == 'host') hasHost = true;
        if (name == 'content-type') hasType = true;
        head.write('$name: ${header.value}\r\n');
      }
    }
    if (!hasHost) {
      String host = url.host.contains(':') ? '[${url.host}]' : url.host;
      int normal = url.scheme == 'https' ? 443 : 80;
      if (url.port != normal) host = '$host:${url.port}';
      head.write('host: $host\r\n');
    }
    if (coded) head.write('accept-encoding: gzip, br\r\n');
    Uint8List? payload = bodyBytes;
    if (body != null) {
      payload = utf8.encode(body);
      if (!hasType) head.write('content-type: text/plain; charset=utf-8\r\n');
    }
    if (stream) {
      // the body goes in later records
      head.write(
        streamLength == null
            ? 'transfer-encoding: chunked\r\n'
            : 'content-length: $streamLength\r\n',
      );
    } else {
      bool hasBody =
          payload != null ||
          method == 'POST' ||
          method == 'PUT' ||
          method == 'PATCH';
      if (hasBody) head.write('content-length: ${payload?.length ?? 0}\r\n');
    }
    head.write('\r\n');
    Uint8List start = utf8.encode(head.toString());
    if (payload == null || payload.isEmpty) return start;
    return Uint8List(start.length + payload.length)
      ..setRange(0, start.length, start)
      ..setRange(start.length, start.length + payload.length, payload);
  }

  /// Finds the addresses of the host of [pending], then sends it. One
  /// lookup serves all requests of the next [_lookupTime].
  void _resolve(_Pending pending) {
    String host = pending.url.host;
    int now = _clock.elapsedMicroseconds;
    _Lookup? known = _lookups[host];
    if (known == null || known.until <= now) {
      if (_lookups.length >= _maxLookups) {
        _lookups.removeWhere((String _, _Lookup old) => old.until <= now);
        // all are in use: the oldest one goes
        if (_lookups.length >= _maxLookups) {
          _lookups.remove(_lookups.keys.first);
        }
      }
      _Lookup fresh = _Lookup(now + _lookupTime.inMicroseconds);
      // sync: a lookup that throws is a failed lookup too
      fresh.pending = Future<List<InternetAddress>>.sync(() => _lookup(host))
          .then((List<InternetAddress> addresses) {
            fresh.addresses = addresses;
            fresh.pending = null;
            return addresses;
          });
      _lookups[host] = known = fresh;
    }
    List<InternetAddress>? addresses = known.addresses;
    if (addresses != null) {
      pending.addresses = addresses;
      return _submit(pending);
    }
    _Lookup entry = known;
    Future<List<InternetAddress>> lookup = entry.pending!;
    if (pending.deadline != 0) {
      // the timeout of the request also holds for the lookup
      int left = pending.deadline - now;
      lookup = lookup.timeout(Duration(microseconds: left < 1 ? 1 : left));
    }
    lookup.then(
      (List<InternetAddress> addresses) {
        if (_closed) return _fail(pending, DoxHttpFailure.clientClosed);
        pending.addresses = addresses;
        _submit(pending);
      },
      onError: (Object error) {
        if (error is TimeoutException) {
          return _fail(pending, DoxHttpFailure.timeout);
        }
        if (identical(_lookups[host], entry)) _lookups.remove(host);
        _fail(pending, DoxHttpFailure.lookupFailed);
      },
    );
  }

  void _fail(_Pending pending, DoxHttpFailure failure) {
    pending.fail(
      DoxHttpClientException(failure, pending.url),
      StackTrace.current,
    );
  }

  /// Writes the request record of [pending] for the next [_run].
  void _submit(_Pending pending) {
    // aborted while the lookup ran
    if (pending is _StreamPending && pending.finished) return;
    if (pending.index >= pending.addresses.length) {
      return _fail(pending, DoxHttpFailure.lookupFailed);
    }
    // what is left of the timeout: a lookup or an address with no server
    // took a part of it
    int timeoutMs = 0;
    if (pending.deadline != 0) {
      int left = pending.deadline - _clock.elapsedMicroseconds;
      if (left <= 0) return _fail(pending, DoxHttpFailure.timeout);
      timeoutMs = _ceilMs(left);
      // The timeout of an upload starts again each time bytes go out, so
      // Rust needs all of it, not what a lookup left.
      if (pending.uploadWait != 0) timeoutMs = _ceilMs(pending.uploadWait);
    }
    InternetAddress address = pending.addresses[pending.index];
    Uint8List request = pending.request;
    // the server name, to check the certificate against
    Uint8List name = pending.flags & _tls != 0
        ? utf8.encode(pending.url.host)
        : _noName;
    int size = _reqHead + name.length + request.length;
    if (_len + size > _outCap) {
      int cap = _outCap;
      while (cap < _len + size) {
        cap *= 2;
      }
      _sizeOut(cap);
    }

    int id = _nextId++;
    pending.id = id;
    if (pending is _StreamPending) pending.handed = request.length;
    Uint8List raw = address.rawAddress;
    ByteData data = ByteData.sublistView(_out, _len, _len + _reqHead);
    data.setUint32(0, size, Endian.little);
    data.setUint64(4, id, Endian.little);
    data.setUint8(12, raw.length == 16 ? 6 : 4);
    data.setUint8(13, pending.flags);
    data.setUint16(14, pending.url.port, Endian.little);
    data.setUint64(16, 0);
    data.setUint64(24, 0);
    _out.setRange(_len + 16, _len + 16 + raw.length, raw);
    data.setUint32(32, timeoutMs, Endian.little);
    data.setUint16(36, name.length, Endian.little);
    // a new request: kind 0
    data.setUint16(38, 0);
    int at = _len + _reqHead;
    _out.setRange(at, at + name.length, name);
    _out.setRange(at + name.length, _len + size, request);
    _len += size;

    if (_open.isEmpty) {
      _receive.keepIsolateAlive = true;
      _busy.add(this);
    }
    _open[id] = pending;
    if (!_queued) {
      // one call for all requests of this turn of the event loop
      _queued = true;
      scheduleMicrotask(_run);
    }
  }

  /// Gives the request buffer [size] bytes. It keeps its records.
  void _sizeOut(int size) {
    _outPtr = doxClientBuffer(_handle, 0, size);
    _outCap = size;
    _out = _outPtr.asTypedList(size);
  }

  /// Gives the answer buffer [size] bytes.
  void _sizeIn(int size) {
    _inPtr = doxClientBuffer(_handle, 1, size);
    _inCap = size;
    _in = _inPtr.asTypedList(size);
  }

  void _onWake() {
    if (!_queued) _run();
  }

  void _run() {
    _queued = false;
    if (_closed) return;
    int n = doxClientRun(_handle, _outPtr, _len, _inPtr, _inCap);
    _len = 0;
    if (_outCap > _keepSize) _sizeOut(_outSize);
    if (n < 0) {
      // one answer needs a bigger buffer
      _sizeIn(-n);
      n = doxClientRun(_handle, _outPtr, 0, _inPtr, _inCap);
    }
    // 0: Rust posts to the port when there is an answer
    if (n <= 0) return;
    ByteData data = ByteData.sublistView(_in, 0, n);
    int p = 0;
    // the streams that got parts: their listeners get them after the loop,
    // when the buffer is not needed any more
    Set<_StreamPending> touched = <_StreamPending>{};
    while (p < n) {
      int size = data.getUint32(p, Endian.little);
      int id = data.getUint64(p + 4, Endian.little);
      int status = data.getUint16(p + 12, Endian.little);
      int error = data.getUint16(p + 14, Endian.little);
      int lines = data.getUint32(p + 16, Endian.little);
      int kind = data.getUint8(p + 20);
      _Pending? pending = kind == _rWhole ? _open.remove(id) : _open[id];
      if (pending is _WholePending) {
        if (status != 0) {
          Uint8List copy = Uint8List.fromList(
            Uint8List.sublistView(_in, p + _resHead, p + size),
          );
          pending.completer.complete(
            DoxHttpClientResponse._(
              pending.url,
              status,
              Uint8List.sublistView(copy, 0, lines),
              Uint8List.sublistView(copy, lines),
            ),
          );
        } else {
          _failed(pending, error);
        }
      } else if (pending is _StreamPending) {
        switch (kind) {
          case _rHead:
            pending.onHead(
              status,
              Uint8List.fromList(
                Uint8List.sublistView(_in, p + _resHead, p + _resHead + lines),
              ),
            );
          case _rPart:
            pending.onPart(
              Uint8List.fromList(
                Uint8List.sublistView(_in, p + _resHead + lines, p + size),
              ),
            );
          case _rProgress:
            pending.onProgress(data.getUint64(p + _resHead, Endian.little));
          case _rEnd:
            _open.remove(id);
            pending.onEnd();
          case _:
            // the request failed
            _failed(pending, error);
        }
        touched.add(pending);
      }
      p += size;
    }
    if (_inCap > _keepSize) _sizeIn(_inSize);
    for (_StreamPending pending in touched) {
      pending.pump();
    }
    if (_open.isEmpty) {
      _receive.keepIsolateAlive = false;
      _busy.remove(this);
    } else if (!_queued) {
      // more answers may wait: look again after a turn of the event loop
      _queued = true;
      Timer.run(_run);
    }
  }

  /// Rust gave [error] for [pending], see native/src/client.rs.
  void _failed(_Pending pending, int error) {
    if (error == 1) {
      // nothing was sent: the next address of the host may work
      pending.index++;
      if (pending.index < pending.addresses.length) return _submit(pending);
      _lookups.remove(pending.url.host);
      return _fail(pending, DoxHttpFailure.connectionFailed);
    }
    _fail(pending, switch (error) {
      2 => DoxHttpFailure.timeout,
      3 => DoxHttpFailure.connectionClosed,
      5 => DoxHttpFailure.responseTooBig,
      6 => DoxHttpFailure.tlsFailed,
      _ => DoxHttpFailure.badResponse,
    });
  }
}
