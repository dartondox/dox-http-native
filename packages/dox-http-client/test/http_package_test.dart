import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dox_http_client/dox_http_client.dart';
import 'package:dox_http_client/http.dart';
import 'package:http/http.dart' as http;
import 'package:test/test.dart';

import 'client_test.dart' show handle;

void main() {
  late HttpServer server;
  late http.Client client;
  late Uri base;

  setUp(() async {
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    server.listen(handle);
    base = Uri.parse('http://127.0.0.1:${server.port}');
    client = DoxHttpPackageClient();
  });

  tearDown(() async {
    client.close();
    await server.close(force: true);
  });

  Map<String, Object?> echo(http.Response res) {
    return jsonDecode(res.body) as Map<String, Object?>;
  }

  test('get gives the status, the body and the headers', () async {
    http.Response res = await client.get(base);
    expect(res.statusCode, 200);
    expect(res.body, 'Hello World');
    expect(res.headers['x-test'], 'one, two');
    expect(res.contentLength, 11);
    expect(res.request!.url, base);
    expect((await client.get(base.resolve('/status/404'))).statusCode, 404);
  });

  test('post sends the body, its length and the headers', () async {
    http.Response res = await client.post(
      base.resolve('/echo'),
      headers: <String, String>{'X-Name': 'dox'},
      body: 'héllo',
    );
    Map<String, Object?> got = echo(res);
    expect(got['method'], 'POST');
    expect(got['body'], 'héllo');
    expect(got['length'], '6');
    expect(got['x-name'], 'dox');
    expect(got['type'], 'text/plain; charset=utf-8');
  });

  test('a post with no body sends content-length 0', () async {
    Map<String, Object?> got = echo(await client.post(base.resolve('/echo')));
    expect(got['length'], '0');
    expect(got['body'], '');
  });

  test('a content-length header of the request does not break it', () async {
    http.Request request = http.Request('PUT', base.resolve('/echo'))
      ..headers['Content-Length'] = '3'
      ..body = 'abc';
    http.Response res = await http.Response.fromStream(
      await client.send(request),
    );
    expect(echo(res)['body'], 'abc');
    expect(echo(res)['length'], '3');
  });

  test('a streamed request with no length goes out chunked', () async {
    http.StreamedRequest request = http.StreamedRequest(
      'POST',
      base.resolve('/echo'),
    );
    request.sink.add(utf8.encode('part one, '));
    request.sink.add(utf8.encode('part two'));
    unawaited(request.sink.close());
    http.Response res = await http.Response.fromStream(
      await client.send(request),
    );
    expect(echo(res)['body'], 'part one, part two');
    expect(echo(res)['length'], isNull);
  });

  test('a multipart request goes out whole', () async {
    http.MultipartRequest request =
        http.MultipartRequest('POST', base.resolve('/echo'))
          ..fields['name'] = 'dox'
          ..files.add(
            http.MultipartFile.fromString(
              'file',
              'the file',
              filename: 'a.txt',
            ),
          );
    http.Response res = await http.Response.fromStream(
      await client.send(request),
    );
    Map<String, Object?> got = echo(res);
    expect(got['type'], startsWith('multipart/form-data; boundary='));
    expect(got['body'], contains('the file'));
    expect(got['body'], contains('name="name"'));
    expect(got['length'], '${request.contentLength}');
  });

  test('the answer body is a stream', () async {
    http.StreamedResponse res = await client.send(
      http.Request('GET', base.resolve('/stream/300000')),
    );
    expect(res.contentLength, 300000);
    int length = 0;
    await for (List<int> part in res.stream) {
      length += part.length;
    }
    expect(length, 300000);
  });

  test('a get follows a redirect, and can be told not to', () async {
    Uri url = base.resolve('/redirect/302?to=/');
    expect((await client.get(url)).body, 'Hello World');
    http.StreamedResponse res = await client.send(
      http.Request('GET', url)..followRedirects = false,
    );
    await res.stream.drain<void>();
    expect(res.statusCode, 302);
    expect(res.isRedirect, isTrue);
    expect(res.headers['location'], '/');
  });

  test('a failure is a ClientException and a DoxHttpClientException', () async {
    ServerSocket socket = await ServerSocket.bind('127.0.0.1', 0);
    int port = socket.port;
    await socket.close();
    Uri url = Uri.parse('http://127.0.0.1:$port/');
    Object? error;
    try {
      await client.get(url);
    } catch (e) {
      error = e;
    }
    expect(error, isA<http.ClientException>());
    expect(error, isA<DoxHttpClientException>());
    expect(
      (error! as DoxHttpClientException).failure,
      DoxHttpFailure.connectionFailed,
    );
    expect((error as http.ClientException).uri, url);
  });

  test('a failure in the body is a ClientException on the stream', () async {
    http.StreamedResponse res = await client.send(
      http.Request('GET', base.resolve('/die')),
    );
    expect(
      res.stream.drain<void>(),
      throwsA(
        isA<DoxHttpPackageClientException>().having(
          (DoxHttpPackageClientException e) => e.failure,
          'failure',
          DoxHttpFailure.connectionClosed,
        ),
      ),
    );
  });

  test('abort before the head fails the request', () async {
    Completer<void> abort = Completer<void>();
    Future<http.StreamedResponse> pending = client.send(
      http.AbortableRequest(
        'GET',
        base.resolve('/slow'),
        abortTrigger: abort.future,
      ),
    );
    abort.complete();
    await expectLater(pending, throwsA(isA<http.RequestAbortedException>()));
    // the client still works
    expect((await client.get(base)).body, 'Hello World');
  });

  test('abort in the body ends the stream with the exception', () async {
    Completer<void> abort = Completer<void>();
    http.StreamedResponse res = await client.send(
      http.AbortableRequest(
        'GET',
        base.resolve('/stream/3000000'),
        abortTrigger: abort.future,
      ),
    );
    List<int> got = <int>[];
    Completer<Object?> done = Completer<Object?>();
    res.stream.listen(
      (List<int> part) {
        got.addAll(part);
        if (!abort.isCompleted) abort.complete();
      },
      onError: (Object e) => done.complete(e),
      onDone: () {
        if (!done.isCompleted) done.complete(null);
      },
    );
    expect(await done.future, isA<http.RequestAbortedException>());
    expect(got.length, lessThan(3000000));
  });

  test('abort after the end does nothing', () async {
    Completer<void> abort = Completer<void>();
    http.Response res = await http.Response.fromStream(
      await client.send(
        http.AbortableRequest('GET', base, abortTrigger: abort.future),
      ),
    );
    abort.complete();
    await Future<void>.delayed(Duration.zero);
    expect(res.body, 'Hello World');
  });

  test('close leaves a client that was given open', () async {
    DoxHttpClient inner = DoxHttpClient();
    DoxHttpPackageClient(client: inner).close();
    expect((await inner.get(base)).body, 'Hello World');
    inner.close();
  });
}
