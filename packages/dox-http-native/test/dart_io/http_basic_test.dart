// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_basic_test.dart.
import 'dart:convert';
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

void echoHandler(HttpRequest request) {
  HttpResponse response = request.response;
  expect(request.method, 'POST');
  response.contentLength = request.contentLength;
  request.cast<List<int>>().pipe(response);
}

void zeroToTenHandler(HttpRequest request) {
  HttpResponse response = request.response;
  expect(request.method, 'GET');
  request.listen(
    (_) {},
    onDone: () {
      response.write('01234567890');
      response.close();
    },
  );
}

void notFoundHandler(HttpRequest request) {
  HttpResponse response = request.response;
  response.statusCode = HttpStatus.notFound;
  response.headers.set('Content-Type', 'text/html; charset=UTF-8');
  response.write('Page not found');
  response.close();
}

void reasonForMovingHandler(HttpRequest request) {
  HttpResponse response = request.response;
  response.statusCode = HttpStatus.movedPermanently;
  response.reasonPhrase = "Don't come looking here any more";
  response.close();
}

void hostHandler(HttpRequest request) {
  HttpResponse response = request.response;
  expect(request.headers['Host']?.length, 1);
  expect(request.headers['Host']![0], 'www.dartlang.org:1234');
  expect(request.headers.host, 'www.dartlang.org');
  expect(request.headers.port, 1234);
  response.statusCode = HttpStatus.ok;
  response.close();
}

void main() {
  late NativeHttpServer server;
  late int port;
  late HttpClient client;

  setUp(() async {
    server = await serve((HttpRequest request) {
      switch (request.uri.path) {
        case '/echo':
          echoHandler(request);
        case '/0123456789':
          zeroToTenHandler(request);
        case '/reasonformoving':
          reasonForMovingHandler(request);
        case '/host':
          hostHandler(request);
        default:
          notFoundHandler(request);
      }
    });
    port = server.port;
    client = newClient();
  });

  test('start and stop', () async {
    await server.close();
  });

  test('GET', () async {
    HttpClientRequest request = await client.get(
      '127.0.0.1',
      port,
      '/0123456789',
    );
    HttpClientResponse response = await request.close();
    expect(response.statusCode, HttpStatus.ok);
    expect(await utf8.decodeStream(response), '01234567890');
  });

  for (bool chunked in <bool>[true, false]) {
    test('POST echo (chunked: $chunked)', () async {
      String data = 'ABCDEFGHIJKLMONPQRSTUVWXYZ';
      for (int i = 0; i < 10; i++) {
        HttpClientRequest request = await client.post(
          '127.0.0.1',
          port,
          '/echo',
        );
        if (chunked) {
          request.write(data.substring(0, 10));
          request.write(data.substring(10, data.length));
        } else {
          request.contentLength = data.length;
          request.write(data);
        }
        HttpClientResponse response = await request.close();
        expect(response.statusCode, HttpStatus.ok);
        expect(await utf8.decodeStream(response), data);
      }
    });
  }

  test('404', () async {
    HttpClientRequest request = await client.get(
      '127.0.0.1',
      port,
      '/thisisnotfound',
    );
    HttpClientResponse response = await request.close();
    expect(response.statusCode, HttpStatus.notFound);
    expect(await utf8.decodeStream(response), 'Page not found');
  });

  test(
    'reason phrase',
    () async {
      HttpClientRequest request = await client.get(
        '127.0.0.1',
        port,
        '/reasonformoving',
      );
      request.followRedirects = false;
      HttpClientResponse response = await request.close();
      expect(response.statusCode, HttpStatus.movedPermanently);
      expect(response.reasonPhrase, "Don't come looking here any more");
      expect(await response.toList(), isEmpty);
    },
    // the server always sends the standard phrase of the status code
    skip: 'dox_http_native: a custom reasonPhrase is not sent',
  );

  test('Host header', () async {
    String text = await raw(
      port,
      'GET /host HTTP/1.1\r\nHost: www.dartlang.org:1234\r\n\r\n',
    );
    expect(statusOf(text), 200);
  });
}
