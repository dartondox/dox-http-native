// Copyright (c) 2021, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_parser_connect_method_test.dart.
// ignore_for_file: unawaited_futures
// The SDK test is about the client parser. Server side: a CONNECT request
// reaches the handler, and the answer is a normal one.
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  const Map<String, String> cases = <String, String>{
    'content-length': '0',
    'transfer-encoding': 'chunked',
    'testHeader': 'testValue',
  };
  cases.forEach((String header, String value) {
    test('CONNECT request, answer with $header', () async {
      NativeHttpServer server = await serve((HttpRequest request) {
        expect(request.method, 'CONNECT');
        request.response.statusCode = 200;
        request.response.headers.add(header, value);
        request.response.close();
      });
      String text = await raw(
        server.port,
        'CONNECT 127.0.0.1:80 HTTP/1.1\r\nHost: 127.0.0.1:80\r\nConnection: close\r\n\r\n',
      );
      expect(statusOf(text), 200);
      // never a transfer-encoding the body does not follow
      if (header == 'transfer-encoding') {
        expect(
          text.toLowerCase(),
          isNot(contains('transfer-encoding: chunked')),
        );
      }
    });
  });

  test('CONNECT with the HttpClient (origin form)', () async {
    NativeHttpServer server = await serve((HttpRequest request) {
      expect(request.method, 'CONNECT');
      request.response.close();
    });
    HttpClient client = newClient();
    HttpClientRequest request = await client.open(
      'CONNECT',
      '127.0.0.1',
      server.port,
      '/',
    );
    expect((await request.close()).statusCode, 200);
  });
}
