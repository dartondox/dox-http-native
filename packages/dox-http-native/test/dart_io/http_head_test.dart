// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_head_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('HEAD requests', () async {
    NativeHttpServer server = await serve((HttpRequest request) {
      HttpResponse response = request.response;
      switch (request.uri.path) {
        case '/test100':
          response.contentLength = 100;
          response.close();
        case '/test200':
          response.contentLength = 200;
          response.add(List<int>.filled(200, 0));
          response.close();
        case '/testChunked100':
          response.add(List<int>.filled(100, 0));
          response.close();
        case '/testChunked200':
          response.add(List<int>.filled(200, 0));
          response.close();
        default:
          fail('unexpected path');
      }
    });
    HttpClient client = newClient();
    for (int i = 0; i < 4; i++) {
      int len = i % 2 == 0 ? 100 : 200;
      HttpClientRequest request = await client.open(
        'HEAD',
        '127.0.0.1',
        server.port,
        '/test$len',
      );
      HttpClientResponse response = await request.close();
      expect(response.contentLength, len);
      expect(await response.length, 0, reason: 'data from a HEAD request');

      request = await client.open(
        'HEAD',
        '127.0.0.1',
        server.port,
        '/testChunked$len',
      );
      response = await request.close();
      expect(await response.length, 0, reason: 'data from a HEAD request');
    }
  });

  test(
    'HEAD without content length: dart:io sends no length',
    () async {
      NativeHttpServer server = await serve((HttpRequest request) {
        request.response.add(List<int>.filled(100, 0));
        request.response.close();
      });
      String text = await raw(
        server.port,
        'HEAD / HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      expect(text, isNot(contains('content-length')));
    },
    skip:
        'dox_http_native: HEAD answers carry the real Content-Length of '
        'the body (valid, RFC 9110 9.3.2), not chunked',
  );

  test(
    'HEAD with a set contentLength and status 204 sends no length',
    () async {
      NativeHttpServer server = await serve((HttpRequest request) {
        request.response
          ..statusCode = 204
          ..contentLength = 5
          ..close();
      });
      String text = await raw(
        server.port,
        'HEAD / HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      expect(statusOf(text), 204);
      expect(text.toLowerCase(), isNot(contains('content-length')));
      await server.close();
    },
  );

  test('a date in defaultResponseHeaders gives one Date header', () async {
    NativeHttpServer server = await serve((HttpRequest request) {
      // `/json` takes the other path: defaults with another content type
      if (request.uri.path == '/json') {
        request.response.headers.contentType = ContentType.json;
      }
      request.response.close();
    });
    server.defaultResponseHeaders.set(HttpHeaders.dateHeader, DateTime(2000));
    for (String path in <String>['/', '/json']) {
      String text = await raw(
        server.port,
        'GET $path HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      expect(
        RegExp(
          '^date:',
          caseSensitive: false,
          multiLine: true,
        ).allMatches(text).length,
        1,
      );
    }
    await server.close();
  });
}
