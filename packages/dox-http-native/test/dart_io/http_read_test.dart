// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_read_test.dart.
import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  for (bool chunked in <bool>[true, false]) {
    test('read the request body and echo it (chunked: $chunked)', () async {
      String data = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
      var server = await serve((HttpRequest request) {
        HttpResponse response = request.response;
        if (request.uri.path == '/echo') {
          expect(request.method, 'POST');
          response.contentLength = request.contentLength;
          request.cast<List<int>>().pipe(response);
        } else {
          response.statusCode = HttpStatus.notFound;
          response.headers.set('Content-Type', 'text/html; charset=UTF-8');
          response.write('Page not found');
          response.close();
        }
      });
      HttpClient client = newClient();
      for (int i = 0; i < 10; i++) {
        HttpClientRequest request = await client.post(
          '127.0.0.1',
          server.port,
          '/echo',
        );
        if (chunked) {
          request.write(data.substring(0, 10));
          request.write(data.substring(10));
        } else {
          request.contentLength = data.length;
          request.add(data.codeUnits);
        }
        HttpClientResponse response = await request.close();
        expect(response.statusCode, HttpStatus.ok);
        expect(await utf8.decodeStream(response), data);
      }
    });
  }
}
