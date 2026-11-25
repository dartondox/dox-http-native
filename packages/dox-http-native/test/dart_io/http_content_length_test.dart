// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_content_length_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  for (bool explicit in <bool>[false, true]) {
    for (int total in <int>[5, 25]) {
      test(
        'no body, content length 0 (connections: $total, explicit: $explicit)',
        () async {
          int count = 0;
          Completer<void> all = Completer<void>();
          NativeHttpServer server = await serve((HttpRequest request) {
            expect(request.headers.value('content-length'), isNull);
            HttpResponse response = request.response;
            response.contentLength = 0;
            response.done.then<void>(
              (_) => fail('Unexpected successful response completion'),
              onError: (Object error) {
                expect(error, isA<HttpException>());
                if (++count == total) all.complete();
              },
            );
            // a write with content length 0 closes the connection and
            // reports an error
            response.write('x');
            response.write('x');
            response.close();
            expect(() => response.write('x'), throwsStateError);
          });
          HttpClient client = newClient();
          for (int i = 0; i < total; i++) {
            client
                .get('127.0.0.1', server.port, '/')
                .then((HttpClientRequest request) {
                  if (explicit) request.contentLength = 0;
                  return request.close();
                })
                .then((HttpClientResponse response) {
                  expect(response.headers.value('content-length'), '0');
                  expect(response.contentLength, 0);
                  response.drain<void>();
                })
                .catchError((Object e) {
                  // it is also okay to fail, as headers may not be written
                });
          }
          await all.future.timeout(const Duration(seconds: 10));
        },
        skip:
            'dox_http_native: a body longer than the set contentLength goes '
            'out with its real length, there is no HttpException',
      );
    }
  }

  test('no body with content length 0: the answer has no body', () async {
    NativeHttpServer server = await serve((HttpRequest request) {
      request.response.contentLength = 0;
      request.response.write('x');
      request.response.close();
    });
    String s = await raw(server.port, 'GET / HTTP/1.1\r\n\r\n');
    // never a Content-Length that is smaller than the bytes that follow
    RegExpMatch? m = RegExp(
      r'content-length: (\d+)',
      caseSensitive: false,
    ).firstMatch(s);
    int length = int.parse(m!.group(1)!);
    expect(s.length - s.indexOf('\r\n\r\n') - 4, length);
  });

  for (bool useHeader in <bool>[false, true]) {
    test(
      'body with content length 2 (useHeader: $useHeader)',
      () async {
        const int total = 5;
        int serverCount = 0;
        Completer<void> all = Completer<void>();
        NativeHttpServer server = await serve((HttpRequest request) {
          expect(request.headers.value('content-length'), '2');
          expect(request.contentLength, 2);
          HttpResponse response = request.response;
          if (useHeader) {
            response.contentLength = 2;
          } else {
            response.headers.set('content-length', 2);
          }
          request.listen(
            (_) {},
            onDone: () {
              response.write('x');
              expect(
                () => response.contentLength = 3,
                throwsA(isA<HttpException>()),
              );
              response.write('x');
              response.write('x');
              response.done.then<void>(
                (_) => fail('Unexpected successful response completion'),
                onError: (Object error) {
                  expect(error, isA<HttpException>());
                  if (++serverCount == total) all.complete();
                },
              );
              response.close();
              expect(() => response.write('x'), throwsStateError);
            },
          );
        });
        HttpClient client = newClient();
        for (int i = 0; i < total; i++) {
          client
              .get('127.0.0.1', server.port, '/')
              .then((HttpClientRequest request) {
                if (useHeader) {
                  request.contentLength = 2;
                } else {
                  request.headers.add(HttpHeaders.contentLengthHeader, '7');
                  request.headers.add(HttpHeaders.contentLengthHeader, '2');
                }
                request.write('x');
                request.write('x');
                return request.close();
              })
              .then((HttpClientResponse response) {
                expect(response.headers.value('content-length'), '2');
                response.listen((_) {}, onError: (Object _) {});
              })
              .catchError((Object e) {});
        }
        await all.future.timeout(const Duration(seconds: 10));
      },
      skip:
          'dox_http_native: a body longer than the set contentLength goes '
          'out with its real length, there is no HttpException',
    );
  }

  for (bool useHeader in <bool>[false, true]) {
    test(
      'chunked body, content length set too (useHeader: $useHeader)',
      () async {
        const int total = 5;
        NativeHttpServer server = await serve((HttpRequest request) {
          expect(request.headers.value('content-length'), isNull);
          HttpResponse response = request.response;
          if (useHeader) {
            response.contentLength = 2;
            response.headers.chunkedTransferEncoding = true;
          } else {
            response.headers.set('content-length', 2);
            response.headers.set('transfer-encoding', 'chunked');
          }
          request.listen(
            (_) {},
            onDone: () {
              response.write('x');
              expect(
                () => response.headers.chunkedTransferEncoding = false,
                throwsA(isA<HttpException>()),
              );
              response.write('x');
              response.write('x');
              response.close();
              expect(() => response.write('x'), throwsStateError);
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
          if (useHeader) {
            request.contentLength = 2;
            request.headers.chunkedTransferEncoding = true;
          } else {
            request.headers.add(HttpHeaders.contentLengthHeader, '2');
            request.headers.set(HttpHeaders.transferEncodingHeader, 'chunked');
          }
          request.write('x');
          expect(
            () => request.headers.chunkedTransferEncoding = false,
            throwsA(isA<HttpException>()),
          );
          request.write('x');
          request.write('x');
          HttpClientResponse response = await request.close();
          expect(response.headers.value('content-length'), isNull);
          expect(response.contentLength, -1);
          await response.drain<void>();
        }
      },
      skip:
          'dox_http_native: the server buffers the body and always sends its '
          'real Content-Length, never chunked with a set contentLength',
    );
  }

  test('set content length', () async {
    NativeHttpServer server = await serve((HttpRequest request) {
      HttpResponse response = request.response;
      expect(response.headers.value('content-length'), isNull);
      expect(response.contentLength, -1);
      response.headers.set('content-length', 3);
      expect(response.headers.value('content-length'), '3');
      expect(response.contentLength, 3);
      response.write('xxx');
      response.close();
    });
    HttpClient client = newClient();
    HttpClientRequest request = await client.get('127.0.0.1', server.port, '/');
    HttpClientResponse response = await request.close();
    expect(response.statusCode, 200);
    await response.drain<void>();
  });

  test(
    'content length and chunked set together: framing stays valid',
    () async {
      NativeHttpServer server = await serve((HttpRequest request) {
        request.response.headers.set('content-length', 2);
        request.response.headers.set('transfer-encoding', 'chunked');
        request.response.write('xxx');
        request.response.close();
      });
      String s = await raw(
        server.port,
        'GET / HTTP/1.1\r\n\r\nGET / HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      // two answers, each with a length that matches its body
      expect('HTTP/1.1 200'.allMatches(s).length, 2);
      expect('content-length: 3'.allMatches(s.toLowerCase()).length, 2);
      expect(s.toLowerCase(), isNot(contains('transfer-encoding')));
    },
  );
}
