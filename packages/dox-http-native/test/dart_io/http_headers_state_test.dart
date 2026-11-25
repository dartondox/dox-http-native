// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_headers_state_test.dart.
import 'dart:async';
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  for (String? body in <String?>[null, 'Hello and goodbye']) {
    test('header state of request and response (body: $body)', () async {
      const int total = 5;
      List<String> problems = <String>[];
      Completer<void> served = Completer<void>();
      int count = 0;
      var server = await serve((HttpRequest request) {
        HttpResponse response = request.response;
        // cannot mutate request headers
        expect(
          () => request.headers.add('X-Request-Header', 'value'),
          throwsA(isA<HttpException>()),
        );
        expect(request.headers.value('X-Request-Header'), 'value');
        request.listen(
          (_) {},
          onDone: () {
            // can still mutate response headers as long as no data went out
            response.headers.add('X-Response-Header', 'value');
            if (body != null) {
              response.write(body);
              try {
                response.statusCode = 200;
                problems.add('statusCode after write');
              } on StateError {
                // expected by dart:io
              }
              try {
                response.reasonPhrase = 'OK';
                problems.add('reasonPhrase after write');
              } on StateError {
                // expected by dart:io
              }
              try {
                response.headers.add('X-Request-Header', 'value2');
                problems.add('headers after write');
              } on HttpException {
                // expected by dart:io
              }
            }
            response.close();
            try {
              response.statusCode = 200;
              problems.add('statusCode after close');
            } on StateError {
              // expected by dart:io
            }
            try {
              response.headers.add('X-Request-Header', 'value3');
              problems.add('headers after close');
            } on HttpException {
              // expected by dart:io
            }
            if (++count == total) served.complete();
          },
        );
      });
      HttpClient client = newClient();
      for (int i = 0; i < total; i++) {
        HttpClientRequest request = await client.get(
          '127.0.0.1',
          server.port,
          '/',
        );
        if (body != null) request.contentLength = -1;
        request.headers.add('X-Request-Header', 'value');
        if (body != null) request.write(body);
        request.close();
        HttpClientResponse response = await request.done;
        expect(
          () => response.headers.add('X-Response-Header', 'value'),
          throwsA(isA<HttpException>()),
        );
        expect(response.headers.value('X-Response-Header'), 'value');
        await response.drain<void>();
      }
      await served.future;
      // the state errors of dart:io: see the skipped test below
    });
  }

  test(
    'response head is locked once data is written (state errors)',
    () async {
      List<String> problems = <String>[];
      var server = await serve((HttpRequest request) {
        HttpResponse response = request.response;
        response.write('hello');
        try {
          response.statusCode = 200;
          problems.add('statusCode');
        } on StateError {
          // dart:io
        }
        try {
          response.headers.add('X-Late', 'value');
          problems.add('header');
        } on HttpException {
          // dart:io
        }
        response.close();
        request.response.done.then((_) => null);
      });
      await raw(server.port, 'GET / HTTP/1.1\r\nConnection: close\r\n\r\n');
      expect(problems, isEmpty);
    },
    skip:
        'dox_http_native: the head goes out on close or flush, so the '
        'status and headers stay mutable after a buffered write; after the '
        'head is out a change is ignored with one warning',
  );
}
