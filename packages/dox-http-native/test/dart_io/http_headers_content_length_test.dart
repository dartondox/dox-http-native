// Copyright (c) 2020, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_headers_content_length_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('content does not match content length', () async {
    var (int port, Future<HttpRequest> first) = await serveFirst();
    HttpClient client = newClient();
    HttpClientRequest request = await client.get('127.0.0.1', port, '/');
    request.headers.contentLength = 100;
    request.headers.set('Content-Length', 100);
    expect(request.headers['Content-Length']?[0], '100');
    try {
      await request.close();
    } catch (e) {
      request.abort();
    }
    HttpClientRequest req = await client.get('127.0.0.1', port, '/');
    req.headers.contentLength = 100;
    req.add(List<int>.filled(100, 0));
    Future<HttpClientResponse> resFuture = req.close();
    HttpRequest serverReq = await first;
    expect(serverReq.headers.value(HttpHeaders.contentLengthHeader), '100');
    expect(serverReq.headers.value(HttpHeaders.transferEncodingHeader), isNull);
    expect(serverReq.contentLength, 100);
    expect(serverReq.headers.chunkedTransferEncoding, isFalse);
    await serverReq.drain<void>();
    await serverReq.response.close();
    await (await resFuture).drain<void>();
  });

  /// Sends one request with [method] and the client settings of [prepare];
  /// returns what the server sees.
  Future<HttpRequest> roundTrip(
    String method,
    void Function(HttpClientRequest) prepare, {
    bool drain = true,
  }) async {
    var (int port, Future<HttpRequest> first) = await serveFirst();
    HttpClient client = newClient();
    HttpClientRequest req = await client.openUrl(
      method,
      Uri.parse('http://127.0.0.1:$port/'),
    );
    prepare(req);
    Future<HttpClientResponse> resFuture = req.close();
    HttpRequest serverReq = await first;
    if (drain) await serverReq.drain<void>();
    await serverReq.response.close();
    await (await resFuture).drain<void>();
    return serverReq;
  }

  for (String method in <String>['GET', 'HEAD']) {
    test('$method request defaults', () async {
      HttpRequest r = await roundTrip(method, (HttpClientRequest req) {
        expect(req.contentLength, 0);
        expect(req.headers.value('content-length'), '0');
      }, drain: false);
      expect(r.headers.value(HttpHeaders.contentLengthHeader), isNull);
      expect(r.headers.value(HttpHeaders.transferEncodingHeader), isNull);
      expect(r.headers.chunkedTransferEncoding, isFalse);
    });
    test(
      '$method request defaults: contentLength is -1',
      () async {
        HttpRequest r = await roundTrip(method, (_) {}, drain: false);
        expect(r.contentLength, -1);
      },
      skip:
          'dox_http_native: request.contentLength is 0, not -1, with no '
          'Content-Length header',
    );

    test('$method with contentLength 10 and a body', () async {
      HttpRequest r = await roundTrip(method, (HttpClientRequest req) {
        req.contentLength = 10;
        req.add(List<int>.filled(10, 0));
      });
      expect(r.headers.value(HttpHeaders.contentLengthHeader), '10');
      expect(r.headers.value(HttpHeaders.transferEncodingHeader), isNull);
      expect(r.contentLength, 10);
      expect(r.headers.chunkedTransferEncoding, isFalse);
    });

    test('$method with chunked transfer encoding', () async {
      HttpRequest r = await roundTrip(method, (HttpClientRequest req) {
        req.headers.chunkedTransferEncoding = true;
        req.write('hello');
      });
      expect(r.headers.value(HttpHeaders.contentLengthHeader), isNull);
      expect(r.headers.value(HttpHeaders.transferEncodingHeader), 'chunked');
      expect(r.contentLength, -1);
      expect(r.headers.chunkedTransferEncoding, isTrue);
    });

    test('$method with contentLength -1 has no body headers', () async {
      HttpRequest r = await roundTrip(method, (HttpClientRequest req) {
        req.contentLength = -1;
      }, drain: false);
      expect(r.headers.value(HttpHeaders.contentLengthHeader), isNull);
      expect(r.headers.value(HttpHeaders.transferEncodingHeader), isNull);
      expect(r.headers.chunkedTransferEncoding, isFalse);
    });
  }

  test('POST request defaults are chunked', () async {
    HttpRequest r = await roundTrip('POST', (HttpClientRequest req) {
      expect(req.headers.chunkedTransferEncoding, isTrue);
      req.write('hello');
    });
    expect(r.headers.value(HttpHeaders.contentLengthHeader), isNull);
    expect(r.headers.value(HttpHeaders.transferEncodingHeader), 'chunked');
    expect(r.contentLength, -1);
    expect(r.headers.chunkedTransferEncoding, isTrue);
  });

  test('POST with contentLength 10', () async {
    HttpRequest r = await roundTrip('POST', (HttpClientRequest req) {
      req.contentLength = 10;
      req.add(List<int>.filled(10, 0));
    });
    expect(r.headers.value(HttpHeaders.contentLengthHeader), '10');
    expect(r.headers.value(HttpHeaders.transferEncodingHeader), isNull);
    expect(r.contentLength, 10);
    expect(r.headers.chunkedTransferEncoding, isFalse);
  });

  test('POST with contentLength reset to -1 is chunked', () async {
    HttpRequest r = await roundTrip('POST', (HttpClientRequest req) {
      req.contentLength = 10;
      req.contentLength = -1;
      req.write('hello');
    });
    expect(r.headers.value(HttpHeaders.contentLengthHeader), isNull);
    expect(r.headers.value(HttpHeaders.transferEncodingHeader), 'chunked');
    expect(r.contentLength, -1);
    expect(r.headers.chunkedTransferEncoding, isTrue);
  });
}
