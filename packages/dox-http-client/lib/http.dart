/// [DoxHttpPackageClient]: a `Client` of `package:http` that sends its
/// requests through a [DoxHttpClient].
library;

import 'dart:async';

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
      Future<DoxHttpClientStreamedResponse> pending = _client.sendStreamed(
        request.method,
        request.url,
        headers: headers,
        // an abort stops the upload: the request then fails with it
        body: empty
            ? null
            : abort == null
            ? body
            : _body(body, abort, request.url),
        contentLength: empty ? null : length,
        followRedirects: request.followRedirects,
      );
      if (abort == null) {
        res = await pending;
      } else {
        DoxHttpClientStreamedResponse? first =
            await Future.any<DoxHttpClientStreamedResponse?>(
              <Future<DoxHttpClientStreamedResponse?>>[
                pending,
                abort.then((_) => null),
              ],
            );
        if (first == null) {
          // nobody reads the answer: give its connection back
          unawaited(
            pending.then<void>(
              (DoxHttpClientStreamedResponse late) =>
                  late.body.listen(null).cancel(),
              onError: (Object _) {},
            ),
          );
          throw http.RequestAbortedException(request.url);
        }
        res = first;
      }
    } on DoxHttpClientException catch (e) {
      throw DoxHttpPackageClientException(e.failure, e.url);
    }
    String? size = res.headers['content-length'];
    return http.StreamedResponse(
      _body(res.body, abort, request.url),
      res.statusCode,
      contentLength: size == null ? null : int.tryParse(size),
      request: request,
      headers: res.headers,
      isRedirect:
          _redirects.contains(res.statusCode) &&
          res.headers.containsKey('location'),
    );
  }

  /// [source] with its failures as [DoxHttpPackageClientException]. When
  /// [abort] completes before the end, the stream gets a
  /// `RequestAbortedException` and closes, and [source] is canceled.
  Stream<List<int>> _body(
    Stream<List<int>> source,
    Future<void>? abort,
    Uri url,
  ) {
    StreamSubscription<List<int>>? sub;
    bool over = false;
    late StreamController<List<int>> out;
    out = StreamController<List<int>>(
      sync: true,
      onListen: () {
        if (over) return;
        sub = source.listen(
          out.add,
          onError: (Object error, StackTrace trace) {
            out.addError(
              error is DoxHttpClientException
                  ? DoxHttpPackageClientException(error.failure, error.url)
                  : error,
              trace,
            );
          },
          onDone: () {
            over = true;
            out.close();
          },
        );
      },
      onPause: () => sub?.pause(),
      onResume: () => sub?.resume(),
      onCancel: () {
        over = true;
        return sub?.cancel();
      },
    );
    abort?.then((_) {
      if (over) return;
      over = true;
      // a body that nobody listens to yet is canceled too
      unawaited((sub ?? source.listen(null)).cancel());
      out.addError(http.RequestAbortedException(url));
      out.close();
    });
    return out.stream;
  }

  @override
  void close() {
    if (_owned) _client.close();
  }
}
