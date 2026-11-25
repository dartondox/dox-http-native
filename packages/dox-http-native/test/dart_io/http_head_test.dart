// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_head_test.dart.
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('HEAD requests', () async {
    var server = await serve((HttpRequest request) {
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
      var server = await serve((HttpRequest request) {
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
}
