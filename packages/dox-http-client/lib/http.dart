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
    : super('DoxHttpClientException: ${failure.name}', url);

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
/// - `maxRedirects` and `persistentConnection` of a request have no effect:
///   set `maxRedirects` on the [DoxHttpClient].
/// - An answer has no `reasonPhrase`.
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

  static const Set<int> _redirects = <int>{301, 302, 303, 307, 308};

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    Future<void>? abort = request is http.Abortable
        ? request.abortTrigger
        : null;
    if (abort != null) {
      // a trigger that is complete now: the request must not go out
      bool aborted = false;
      unawaited(abort.then((_) => aborted = true));
      await null;
      if (aborted) throw http.RequestAbortedException(request.url);
    }
    int? length = request.contentLength;
    http.ByteStream body = request.finalize();
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
    if (empty) body.drain<void>().ignore();
    DoxHttpClientStreamedResponse res;
    try {
      res = await _client.sendStreamed(
        request.method,
        request.url,
        headers: headers,
        body: empty ? null : body,
        contentLength: empty ? null : length,
        followRedirects: request.followRedirects,
        abort: abort,
      );
    } on DoxHttpClientException catch (e) {
      throw _failure(e);
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

  @override
  void close() {
    if (_owned) _client.close();
  }
}
