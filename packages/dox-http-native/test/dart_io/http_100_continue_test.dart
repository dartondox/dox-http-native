// Copyright (c) 2015, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_100_continue_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

// The SDK test is about a client that reads `100 Continue` answers. Here the
// server side: how it answers a request with `Expect: 100-continue`.
void main() {
  test('Expect: 100-continue gets a 100, then the final answer', () async {
    NativeHttpServer server = await serve((HttpRequest request) async {
      List<int> body = <int>[for (List<int> c in await request.toList()) ...c];
      request.response.write('got ${body.length}');
      await request.response.close();
    });
    String text = await raw(
      server.port,
      'POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n'
      'Expect: 100-continue\r\nConnection: close\r\n\r\n',
      chunks: <String>['hello'],
    );
    expect(text, startsWith('HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200'));
    expect(text, endsWith('got 5'));
  });

  test('Expect: 100-continue with HttpClient', () async {
    NativeHttpServer server = await serve((HttpRequest request) async {
      await request.drain<void>();
      request.response.write('ok');
      await request.response.close();
    });
    HttpClient client = newClient();
    HttpClientRequest request = await client.post(
      '127.0.0.1',
      server.port,
      '/',
    );
    request.headers.set(HttpHeaders.expectHeader, '100-continue');
    request.contentLength = 5;
    request.write('hello');
    HttpClientResponse response = await request.close();
    expect(response.statusCode, 200);
    await response.drain<void>();
  });

  test(
    'Expect: 100-continue with a body that is too big gets 413 first',
    () async {
      NativeHttpServer server = await serve((HttpRequest request) async {
        await request.response.close();
      }, maxBodySize: 10);
      String text = await raw(
        server.port,
        'POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n'
        'Expect: 100-continue\r\nConnection: close\r\n\r\n',
      );
      expect(statusOf(text), 413);
      expect(text, isNot(contains('100 Continue')));
    },
  );
}
