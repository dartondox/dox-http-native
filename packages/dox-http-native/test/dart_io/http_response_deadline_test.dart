// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_response_deadline_test.dart.
import 'dart:async';
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('simple deadline', () async {
    var server = await serve((HttpRequest request) {
      request.response.deadline = const Duration(seconds: 1000);
      request.response.write('stuff');
      request.response.close();
    });
    HttpClient client = newClient();
    await Future.wait(<Future<void>>[
      for (int i = 0; i < 10; i++)
        client
            .get('127.0.0.1', server.port, '/')
            .then((HttpClientRequest r) => r.close())
            .then((HttpClientResponse r) => r.drain<void>()),
    ]);
  });

  test('exceeded deadline ends the answer with an error', () async {
    var server = await serve((HttpRequest request) {
      request.response.deadline = const Duration(milliseconds: 100);
      request.response.contentLength = 10000;
      request.response.write('stuff');
    });
    HttpClient client = newClient();
    await Future.wait(<Future<void>>[
      for (int i = 0; i < 10; i++)
        client
            .get('127.0.0.1', server.port, '/')
            .then((HttpClientRequest r) => r.close())
            .then((HttpClientResponse r) => r.drain<void>())
            .then(
              (_) => fail('Expected error'),
              onError: (Object e) {
                // expected
              },
            ),
    ]).timeout(const Duration(seconds: 5));
  }, skip: 'dox_http_native: HttpResponse.deadline is stored but not enforced');

  test(
    'deadline and detachSocket',
    () {},
    skip: 'dox_http_native: detachSocket is not supported',
  );
}
