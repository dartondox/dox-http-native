/// [DoxHttpPackageClient]: a `Client` of `package:http` that sends its
/// requests through a [DoxHttpClient].
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:http/http.dart' as http;

import 'dox_http_client.dart';

/// A failure of a [DoxHttpPackageClient]. It is a `ClientException` of
/// `package:http` and also a [DoxHttpClientException], so code that catches
/// one of them gets it.
class DoxHttpPackageClientException extends http.ClientException
    implements DoxHttpClientException {
  DoxHttpPackageClientException(this.failure, this.url)
    : super(
        failure == DoxHttpFailure.tooManyRedirects
            ? 'Redirect limit exceeded'
            : 'DoxHttpClientException: ${failure.name}',
        url,
      );

  @override
  final DoxHttpFailure failure;

  @override
  final Uri url;
}

/// A `Client` of `package:http` that sends its requests through a
/// [DoxHttpClient].
///
/// Give it to code that takes an `http.Client`:
///
/// ```dart
/// http.Client client = DoxHttpPackageClient();
/// http.Response res = await client.get(Uri.parse('https://example.com'));
/// ```
///
/// What is different from the default client of `package:http`:
///
/// - A request with a body does not follow redirects.
/// - `persistentConnection` of a request has no effect.
/// - An HTTP/2 answer has no `reasonPhrase`.
/// - A failure is a [DoxHttpPackageClientException].
class DoxHttpPackageClient extends http.BaseClient {
  /// With no [client], the adapter makes a [DoxHttpClient] with no
  /// timeout, like the default client of `package:http`, and [close]
  /// closes it. A [client] that you give stays open: close it yourself.
  DoxHttpPackageClient({DoxHttpClient? client})
    : _owned = client == null,
      _client = client ?? DoxHttpClient(timeout: Duration.zero);

  final DoxHttpClient _client;
  final bool _owned;
  bool _closed = false;

  static const Set<int> _redirects = <int>{301, 302, 303, 307, 308};

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    if (_closed) {
      throw http.ClientException('The client is closed', request.url);
    }
    Future<void>? abort = request is http.Abortable
        ? request.abortTrigger
        : null;
    bool aborted = false;
    if (abort != null) {
      unawaited(abort.then((_) => aborted = true));
      // a trigger that is complete now: the request must not go out
      await null;
    }
    int? length = request.contentLength;
    Stream<List<int>> body = request.finalize();
    if (aborted) throw _aborted(body, request.url);
    // the native client sets these two itself
    Map<String, String> headers = <String, String>{};
    for (MapEntry<String, String> header in request.headers.entries) {
      String name = header.key.toLowerCase();
      if (name == 'content-length' || name == 'transfer-encoding') continue;
      headers[header.key] = header.value;
    }
    // No body: the native client then follows redirects and can send the
    // request again. It sends `content-length: 0` for POST, PUT and PATCH.
    bool empty = length == 0;
    if (empty) {
      body.drain<void>().ignore();
    } else if (length == null) {
      // A stream with no length can be empty, like a GET that is sent as a
      // StreamedRequest. Wait for its first bytes: with none, the request
      // has no body.
      // `rest` keeps the parts in their sequence until the native client
      // listens to it.
      Completer<bool> first = Completer<bool>();
      StreamController<List<int>> rest = StreamController<List<int>>();
      StreamSubscription<List<int>>? sub;
      sub = body.listen(
        (List<int> part) {
          if (part.isEmpty && !first.isCompleted) return;
          rest.add(part);
          if (first.isCompleted) return;
          // null: the part came while `listen` ran, see below
          sub?.pause();
          first.complete(true);
        },
        onError: (Object error, StackTrace trace) {
          if (first.isCompleted) return rest.addError(error, trace);
          first.completeError(error, trace);
        },
        onDone: () {
          rest.close();
          if (!first.isCompleted) first.complete(false);
        },
      );
      StreamSubscription<List<int>> source = sub;
      if (first.isCompleted) source.pause();
      rest
        ..onListen = source.resume
        ..onPause = source.pause
        ..onResume = source.resume
        ..onCancel = source.cancel;
      bool some;
      try {
        some = abort == null
            ? await first.future
            : await Future.any<bool>(<Future<bool>>[
                first.future,
                abort.then((_) => false),
              ]);
      } on Object {
        // the body failed before its first bytes
        source.cancel().ignore();
        rethrow;
      }
      if (aborted) {
        first.future.ignore();
        source.cancel().ignore();
        throw http.RequestAbortedException(request.url);
      }
      if (some) {
        body = rest.stream;
      } else {
        empty = true;
      }
    }
    DoxHttpClientStreamedResponse res;
    try {
      res = await _client.sendStreamed(
        request.method,
        request.url,
        headers: headers,
        body: empty ? null : body,
        contentLength: empty ? null : length,
        followRedirects: request.followRedirects,
        maxRedirects: request.maxRedirects,
        abort: abort,
      );
    } on DoxHttpClientException catch (e) {
      throw _failure(e);
    } on Object {
      // eg. a URL that is not http: the body was not read
      if (!empty) _drop(body);
      rethrow;
    }
    String? size = res.headers['content-length'];
    return http.StreamedResponse(
      res.body.transform(
        StreamTransformer<Uint8List, List<int>>.fromHandlers(
          handleError:
              (Object error, StackTrace trace, EventSink<List<int>> sink) {
                sink.addError(
                  error is DoxHttpClientException ? _failure(error) : error,
                  trace,
                );
              },
        ),
      ),
      res.statusCode,
      contentLength: size == null ? null : int.tryParse(size),
      request: request,
      headers: res.headers,
      reasonPhrase: res.reasonPhrase,
      isRedirect:
          _redirects.contains(res.statusCode) &&
          res.headers.containsKey('location'),
    );
  }

  /// The failure for `package:http`. An abort is its
  /// `RequestAbortedException`.
  static http.ClientException _failure(DoxHttpClientException e) {
    return e.failure == DoxHttpFailure.aborted
        ? http.RequestAbortedException(e.url)
        : DoxHttpPackageClientException(e.failure, e.url);
  }

  /// The exception of a request that was aborted before it went out. Its
  /// [body] is canceled, so a `close()` of the sink of the request
  /// completes.
  static http.ClientException _aborted(Stream<List<int>> body, Uri url) {
    _drop(body);
    return http.RequestAbortedException(url);
  }

  /// Cancels a request [body] that nobody reads, so a `close()` of the
  /// sink of the request completes.
  static void _drop(Stream<List<int>> body) {
    try {
      body.listen(null).cancel().ignore();
    } on Object {
      // it has a listener: that one ends it
    }
  }

  @override
  void close() {
    _closed = true;
    if (_owned) _client.close();
  }
}
