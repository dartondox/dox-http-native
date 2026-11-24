import 'dart:async';
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

/// The answer to a request of a [DoxHttpClient].
class DoxHttpClientResponse {
  DoxHttpClientResponse._(
    this.url,
    this.statusCode,
    this._lines,
    this.bodyBytes,
  );

  /// The URL that gave this answer. After a redirect, it is not the URL
  /// of the request.
  final Uri url;

  final int statusCode;

  /// The body as bytes.
  final Uint8List bodyBytes;

  /// The header lines from Rust: `name:value\n`, names in lower case.
  final Uint8List _lines;

  /// The body as text (UTF-8).
  late final String body = utf8.decode(bodyBytes, allowMalformed: true);

  /// The headers. The names are in lower case. A header that comes more
  /// than once has its values joined with `, `.
  late final Map<String, String> headers = _parseHeaders();

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

/// A request that waits for its answer.
class _Pending {
  _Pending(this.url, this.request, this.flags, this.deadline);

  final Uri url;

  /// The HTTP request: head and body.
  final Uint8List request;
  final int flags;

  /// When the request fails with a timeout, in microseconds of
  /// [DoxHttpClient._clock]. 0: never.
  final int deadline;
  final Completer<DoxHttpClientResponse> completer =
      Completer<DoxHttpClientResponse>();

  /// The addresses of the host, and the one in use.
  List<InternetAddress> addresses = const <InternetAddress>[];
  int index = 0;
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

/// A fast HTTP/1.1 client. Rust owns the sockets and parses the answers,
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
/// It speaks HTTP/1.1 over `http://` and `https://`. It follows
/// redirects. It does not unzip a body.
class DoxHttpClient implements Finalizable {
  /// A request with no whole answer in [timeout] fails. `Duration.zero`
  /// is no timeout.
  ///
  /// With [maxConnectionsPerHost] > 0, the client holds at most that many
  /// connections open to one address. More requests wait for a free one.
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
  DoxHttpClient({
    this.timeout = const Duration(seconds: 30),
    this.followRedirects = true,
    this.maxRedirects = 5,
    Duration idleTimeout = const Duration(seconds: 15),
    int maxConnectionsPerHost = 0,
    int maxResponseSize = 0,
    String? trustedCertificates,
  }) {
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
  static const int _resHead = 20;
  static const int _outSize = 1 << 16;
  static const int _inSize = 1 << 18;

  /// A buffer over this size is not kept after a call.
  static const int _keepSize = 1 << 20;

  /// Request flags, see native/src/client.rs.
  static const int _headOnly = 1;
  static const int _again = 2;
  static const int _tls = 4;

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
  /// The client sets `host` and `content-length`. [timeout] is for this
  /// request only: `Duration.zero` is no timeout. It includes the
  /// redirects. [followRedirects] is for this request only.
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
    return _follow(first, method, headers, body, bodyBytes, deadline);
  }

  /// Gives the answer of [first], or of the address its redirects go to.
  Future<DoxHttpClientResponse> _follow(
    Future<DoxHttpClientResponse> first,
    String method,
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
    int deadline,
  ) async {
    DoxHttpClientResponse res = await first;
    for (int hop = 0; ; hop++) {
      String? location = res.header('location');
      if (location == null ||
          location.isEmpty ||
          !_redirects.contains(res.statusCode)) {
        return res;
      }
      Uri url = res.url;
      Uri next;
      try {
        next = url.resolve(location);
      } on FormatException {
        return res;
      }
      bool tls = next.scheme == 'https';
      if ((!tls && next.scheme != 'http') || next.host.isEmpty) return res;
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
      if (toGet) {
        method = 'GET';
        body = null;
        bodyBytes = null;
      }
      if (headers != null && (toGet || !sameServer)) {
        headers = <String, String>{
          for (MapEntry<String, String> header in headers.entries)
            if (!(toGet && _aboutBody.contains(header.key.toLowerCase())) &&
                !(!sameServer && _private.contains(header.key.toLowerCase())))
              header.key: header.value,
        };
      }
      res = await _once(method, next, headers, body, bodyBytes, deadline);
    }
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
    Uint8List request = _encode(method, url, headers, body, bodyBytes);
    // the length of a request record is 32 bits
    if (request.length > _maxRequest) {
      throw ArgumentError('the request is too big (over 2 GiB)');
    }
    _Pending pending = _Pending(url, request, flags, deadline);

    InternetAddress? address = InternetAddress.tryParse(url.host);
    if (address != null) {
      pending.addresses = <InternetAddress>[address];
      _submit(pending);
    } else {
      _resolve(pending);
    }
    return pending.completer.future;
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

  /// The request line, the headers and the body as bytes.
  static Uint8List _encode(
    String method,
    Uri url,
    Map<String, String>? headers,
    String? body,
    Uint8List? bodyBytes,
  ) {
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
    Uint8List? payload = bodyBytes;
    if (body != null) {
      payload = utf8.encode(body);
      if (!hasType) head.write('content-type: text/plain; charset=utf-8\r\n');
    }
    bool hasBody =
        payload != null ||
        method == 'POST' ||
        method == 'PUT' ||
        method == 'PATCH';
    if (hasBody) head.write('content-length: ${payload?.length ?? 0}\r\n');
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
      fresh.pending = InternetAddress.lookup(host)
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
    pending.completer.completeError(
      DoxHttpClientException(failure, pending.url),
      StackTrace.current,
    );
  }

  /// Writes the request record of [pending] for the next [_run].
  void _submit(_Pending pending) {
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
    while (p < n) {
      int size = data.getUint32(p, Endian.little);
      int id = data.getUint64(p + 4, Endian.little);
      int status = data.getUint16(p + 12, Endian.little);
      int error = data.getUint16(p + 14, Endian.little);
      int lines = data.getUint32(p + 16, Endian.little);
      _Pending? pending = _open.remove(id);
      if (pending != null) {
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
      }
      p += size;
    }
    if (_inCap > _keepSize) _sizeIn(_inSize);
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
