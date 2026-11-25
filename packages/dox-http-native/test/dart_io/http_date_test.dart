// Copyright (c) 2012, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_date_test.dart.
// HttpDate is part of dart:io, not of the server. Server side: the Date and
// Expires headers that the server sends.
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('Date header is a valid, current HTTP date', () async {
    var server = await serve((HttpRequest request) => request.response.close());
    HttpClient client = newClient();
    HttpClientRequest request = await client.get('127.0.0.1', server.port, '/');
    HttpClientResponse response = await request.close();
    await response.drain<void>();
    DateTime date = HttpDate.parse(response.headers.value('date')!);
    expect(
      DateTime.now().toUtc().difference(date).abs(),
      lessThan(const Duration(seconds: 5)),
    );
  });

  test('Expires set by the handler goes out in HTTP date format', () async {
    DateTime date = DateTime.utc(1999, DateTime.june, 11, 18, 46, 53);
    var server = await serve((HttpRequest request) {
      request.response.headers.expires = date;
      request.response.close();
    });
    String text = await raw(
      server.port,
      'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    expect(
      text.toLowerCase(),
      contains('expires: fri, 11 jun 1999 18:46:53 gmt'),
    );
  });
}
