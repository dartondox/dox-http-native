import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_native/src/headers.dart';
import 'package:dox_http_native/src/request.dart';
import 'package:dox_http_native/src/server.dart';

/// [HttpResponse] for the native server.
///
/// A small body is kept in memory and sent to Rust in one piece on
/// [close]. When [addStream] passes [streamAt] bytes, the answer starts and
/// the body goes out in parts. At most [window] bytes wait in Rust, so a
/// big file never sits in memory and the client gets data early.
class NativeResponse implements HttpResponse {
  NativeResponse(this._request)
    : headers = NativeHeaders.response(_request.server.defaultResponseHeaders);

  final NativeRequest _request;

  @override
  final NativeHeaders headers;

  NativeHeaders? _trailers;

  /// Headers sent after the body, eg. `grpc-status` for gRPC. HTTP/2 only:
  /// on HTTP/1.1 they are dropped. Set them before [close]. `HttpResponse`
  /// has no trailers, so cast to [NativeResponse] to use them.
  NativeHeaders get trailers => _trailers ??= NativeHeaders.empty();

  /// the trailers to send: set, and HTTP/2
  NativeHeaders? get _sendTrailers {
    NativeHeaders? trailers = _trailers;
    if (trailers == null || _request.protocolVersion != '2.0') return null;
    bool any = false;
    trailers.forEachLine((String name, String value) => any = true);
    return any ? trailers : null;
  }

  int _statusCode = HttpStatus.ok;
  String _reasonPhrase = '';

  @override
  int get statusCode => _statusCode;

  @override
  set statusCode(int code) {
    if (_headSent()) return;
    _statusCode = code;
  }

  @override
  String get reasonPhrase => _reasonPhrase;

  @override
  set reasonPhrase(String phrase) {
    if (_headSent()) return;
    _reasonPhrase = phrase;
  }

  /// true when the head went out (a flush or a streamed body): a status
  /// change is then ignored, with a warning once
  bool _headSent() {
    if (!headers.sent) return false;
    warnHeadSent();
    return true;
  }

  @override
  bool bufferOutput = true;

  @override
  Duration? deadline;

  List<Cookie>? _cookies;

  @override
  List<Cookie> get cookies => _cookies ??= <Cookie>[];
  bool _closed = false;
  Completer<HttpResponse>? _done;
  Encoding? _encoding;

  List<int>? _first;
  BytesBuilder? _more;
  int _size = 0;

  /// [addStream] starts a streamed answer from this many body bytes
  static const int streamAt = 64 * 1024;

  /// most bytes given to Rust and not yet sent to the client
  static const int window = 1024 * 1024;

  /// the stream id, once the body goes out in parts
  int? _stream;
  int _unsent = 0;

  /// the content length of a streamed body (-1: none), and the bytes it
  /// still may have
  int _length = -1;
  int _left = 0;
  Completer<void>? _room;
  bool _gone = false;

  /// the source of [addStream] while it runs. Cancelled when the client
  /// leaves, so a quiet source (eg. server-sent events) does not stay.
  StreamIterator<List<int>>? _source;

  /// that cancel, so [addStream] gets an error of the source's `onCancel`
  Future<void>? _cancel;

  /// the zone of [addStream]. The cancel runs there: a future error can
  /// not go to another error zone (it would be an uncaught error).
  Zone? _zone;

  /// true after [close]
  bool get isClosed => _closed;

  /// true once the status and the headers went out (a [flush], or
  /// [addStream] past [streamAt] bytes). Then an error can not change the
  /// status any more, and later header or status changes are ignored. A
  /// body that is not streamed sends its head on [close].
  bool get headSent => headers.sent;

  /// the body, all chunks together
  List<int> takeBody() {
    BytesBuilder? more = _more;
    List<int> body = more != null ? more.takeBytes() : _first ?? const <int>[];
    _first = null;
    _more = null;
    _size = 0;
    return body;
  }

  /// Rust sent [bytes] of the streamed body to the client. -1: the client
  /// is gone, stop sending.
  void streamed(int bytes) {
    if (bytes < 0) return gone();
    _unsent -= bytes;
    _wake();
  }

  /// The client is gone. Writes are dropped from now on, [addStream] stops
  /// and [done] completes (with no error).
  void gone() {
    _gone = true;
    StreamIterator<List<int>>? source = _source;
    // ends a pending wait for the next chunk in [addStream]. An error of
    // the source's `onCancel` goes to [addStream], not to the zone.
    if (source != null && _cancel == null) {
      // in a microtask: the source's `onCancel` (app code) does not run
      // inside the message loop of the server
      _cancel = (_zone ?? Zone.current).run(
        () => Future<void>.microtask(() => _cancelSource(source))..ignore(),
      );
    }
    _wake();
    _complete();
  }

  static Future<void> _cancelSource(StreamIterator<List<int>> source) {
    Future<void> cancel;
    try {
      cancel = source.cancel();
    } catch (e, s) {
      // an `onCancel` that throws at once
      cancel = Future<void>.error(e, s);
    }
    // [addStream] awaits it. Until then the error is not uncaught.
    return cancel..ignore();
  }

  /// Lets a waiting [addStream] go on.
  void _wake() {
    Completer<void>? room = _room;
    if (room != null && (_gone || _unsent <= window)) {
      _room = null;
      room.complete();
    }
  }

  /// Completes [done], once.
  void _complete() {
    Completer<HttpResponse>? done = _done;
    if (done == null || done.isCompleted) return;
    _request.server.watched.remove(_request.id);
    done.complete(this);
  }

  /// Marks the response as sent: a WebSocket accept answers the request.
  void upgraded() {
    _closed = true;
    NativeHttpServer server = _request.server;
    if (server.goneIds.isNotEmpty) server.goneIds.remove(_request.id);
    _complete();
  }

  /// true when a cookie was added
  bool get hasCookies => _cookies?.isNotEmpty ?? false;

  @override
  int get contentLength => headers.contentLength;

  @override
  set contentLength(int length) => headers.contentLength = length;

  @override
  bool get persistentConnection => headers.persistentConnection;

  @override
  set persistentConnection(bool keep) => headers.persistentConnection = keep;

  @override
  HttpConnectionInfo? get connectionInfo => _request.connectionInfo;

  @override
  Encoding get encoding {
    Encoding? set = _encoding;
    if (set != null) return set;
    if (headers.isShared) return utf8; // default text/plain; charset=utf-8
    String? charset = headers.contentType?.charset;
    if (charset == null) return latin1;
    return Encoding.getByName(charset) ?? latin1;
  }

  @override
  set encoding(Encoding value) => _encoding = value;

  @override
  void add(List<int> data) {
    if (_closed) throw StateError('StreamSink is closed');
    // the client is gone: drop it, like the bytes of a closed socket
    if (data.isEmpty || _gone) return;
    int? stream = _stream;
    if (stream != null) {
      if (_length >= 0) {
        // like dart:io: more bytes than the content length is an error
        if (data.length > _left) {
          throw HttpException(
            'Content size exceeds specified contentLength. '
            '${_length - _left + data.length} bytes written while expected '
            '$_length.',
          );
        }
        _left -= data.length;
      }
      _request.server.sendPart(stream, data);
      _unsent += data.length;
      return;
    }
    _size += data.length;
    if (_first == null) {
      _first = data;
      return;
    }
    BytesBuilder more = _more ??= (BytesBuilder(copy: false)..add(_first!));
    more.add(data);
  }

  @override
  void write(Object? object) {
    String text = '$object';
    if (text.isNotEmpty) add(encoding.encode(text));
  }

  @override
  void writeAll(Iterable<Object?> objects, [String separator = '']) {
    write(objects.join(separator));
  }

  @override
  void writeln([Object? object = '']) => write('$object\n');

  @override
  void writeCharCode(int charCode) => write(String.fromCharCode(charCode));

  @override
  void addError(Object error, [StackTrace? stackTrace]) {
    Completer<HttpResponse>? done = _done;
    if (done == null || done.isCompleted) return;
    _request.server.watched.remove(_request.id);
    done.completeError(error, stackTrace);
  }

  @override
  Future<void> addStream(Stream<List<int>> stream) async {
    StreamIterator<List<int>> source = _source = StreamIterator<List<int>>(
      stream,
    );
    _zone = Zone.current;
    try {
      while (!_gone && await source.moveNext()) {
        // the client left while the next chunk came: it was cancelled
        if (_gone) break;
        List<int> chunk = source.current;
        add(chunk);
        if (_stream == null && _size >= streamAt) _startStream();
        // wait while Rust holds a full window, so memory stays small
        if (_unsent > window) await (_room ??= Completer<void>()).future;
        if (_gone) break; // the client left: stop reading the source
      }
    } catch (_) {
      // the client left: nobody gets this answer any more
      if (_gone) return;
      int? id = _stream;
      if (id != null && !_closed) {
        // the head is sent: cut the connection, the body is not whole
        _closed = true;
        _request.server.endStream(id, ok: false);
        _complete();
      }
      rethrow;
    } finally {
      _source = null;
      Future<void>? cancel = _cancel;
      _cancel = null;
      await (cancel ?? source.cancel());
    }
  }

  /// Sends the head and the body so far. Later parts go out at once.
  void _startStream() {
    _checkCookies();
    int length = headers.contentLength;
    if (length >= 0 && _size > length) {
      throw HttpException(
        'Content size exceeds specified contentLength. '
        '$_size bytes written while expected $length.',
      );
    }
    _length = length;
    _left = length - _size;
    List<int> first = takeBody();
    // from now on Rust tells [streamed] when the client is gone
    if (_done != null) _request.server.watched.remove(_request.id);
    _stream = _request.server.startStream(_request, this, first);
    _unsent += first.length;
    // the head is out: later header and status changes are ignored
    headers.sent = true;
    // the server is closed: nothing goes out, so no wait for room
    if (!_request.server.isOpen) gone();
  }

  /// Cookie does not check its domain, so a line break there would add a
  /// header. Check before the head is sent.
  void _checkCookies() {
    if (!hasCookies) return;
    for (Cookie cookie in cookies) {
      checkHeaderValue(cookie.toString());
    }
  }

  /// Sends what was added so far, like dart:io. The first flush sends the
  /// head: the body then goes out in parts (chunked, or up to the content
  /// length when one is set), and later [add] calls go out at once. A
  /// header or status change after it is ignored (a warning is logged
  /// once). An error (eg. more bytes than the content length) fails the
  /// Future.
  ///
  /// Completes when Rust has room for more (at most [window] bytes wait
  /// there), so a fast writer that awaits it keeps memory small. After
  /// [close], when the client is gone, or while [addStream] runs it does
  /// nothing. A response that never calls flush is sent as before.
  @override
  Future<void> flush() {
    if (_closed || _gone || _source != null) return Future<void>.value();
    if (_stream == null) {
      try {
        _startStream();
      } catch (e, s) {
        return Future<void>.error(e, s);
      }
    }
    if (_gone || _unsent <= window) return Future<void>.value();
    return (_room ??= Completer<void>()).future;
  }

  @override
  Future<HttpResponse> close() {
    end();
    return done;
  }

  /// Sends the response, like [close], but makes no Future. Use it when the
  /// result of [close] is not used: a Future per response keeps the request
  /// alive until the microtasks run, so the heap grows under load. An error
  /// (fewer bytes than the content length) still goes to [done].
  void end() {
    if (!_closed) {
      int? stream = _stream;
      // checked before the response is marked closed
      if (stream == null) _checkCookies();
      _closed = true;
      NativeHttpServer server = _request.server;
      if (server.goneIds.isNotEmpty) server.goneIds.remove(_request.id);
      // nothing more is sent: a waiting flush or addStream goes on (Rust
      // tells no more sent bytes once the stream ended)
      _unsent = 0;
      _wake();
      NativeHeaders? trailers = _sendTrailers;
      if (stream == null && trailers != null) {
        // only the end of a streamed answer (kind 8) carries trailers: the
        // whole body goes as its first part
        stream = _stream = server.startStream(_request, this, takeBody());
        headers.sent = true;
      }
      if (stream == null) {
        _request.server.send(_request, this);
      } else if (_length >= 0 && _left > 0 && !_gone) {
        // like dart:io: fewer bytes than the content length is an error.
        // Rust cuts the connection, so the client sees it.
        _request.server.endStream(stream, ok: false);
        HttpException error = HttpException(
          'Content size below specified contentLength. '
          '${_length - _left} bytes written but expected $_length.',
        );
        _done ??= Completer<HttpResponse>();
        addError(error);
        return;
      } else {
        _request.server.endStream(stream, trailers: trailers);
      }
      _complete();
    }
  }

  /// Completes when the response is sent, or when the client is gone
  /// (with no error, also when the handler never closes the response).
  /// So a handler that writes from its own timer (eg. server-sent events)
  /// can stop it: `response.done.then((_) => timer.cancel())`.
  @override
  Future<HttpResponse> get done {
    Completer<HttpResponse>? done = _done;
    if (done == null) {
      // the client left before anyone read done (see `goneIds`)
      NativeHttpServer server = _request.server;
      if (server.goneIds.isNotEmpty && server.goneIds.remove(_request.id)) {
        gone();
      }
      if (_closed || _gone) return Future<HttpResponse>.value(this);
      done = _done = Completer<HttpResponse>();
      // only an open response that someone waits on hears that its
      // client left (see `NativeHttpServer.watched`)
      if (_stream == null) _request.server.watched[_request.id] = this;
    }
    return done.future;
  }

  @override
  Future<void> redirect(
    Uri location, {
    int status = HttpStatus.movedTemporarily,
  }) {
    statusCode = status;
    headers.set(HttpHeaders.locationHeader, location.toString());
    return close();
  }

  @override
  Future<Socket> detachSocket({bool writeHeaders = true}) {
    // Rust owns the socket. For a WebSocket use `NativeWebSocket.upgrade`.
    throw UnsupportedError(
      'dox_http_native can not detach the socket. '
      'Use NativeWebSocket.upgrade for a WebSocket.',
    );
  }
}
