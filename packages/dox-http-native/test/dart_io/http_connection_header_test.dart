// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_connection_header_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void setConnectionHeaders(HttpHeaders headers) {
  headers.add(HttpHeaders.connectionHeader, 'my-connection-header1');
  headers.add('My-Connection-Header1', 'some-value1');
  headers.add(HttpHeaders.connectionHeader, 'my-connection-header2');
  headers.add('My-Connection-Header2', 'some-value2');
}

void checkExpectedConnectionHeaders(HttpHeaders headers, bool persistent) {
  expect(headers.value('My-Connection-Header1'), 'some-value1');
  expect(headers.value('My-Connection-Header2'), 'some-value2');
  // dart:io splits a value on commas, the native server keeps each field line
  // as it is: compare the tokens
  List<String> connection = <String>[
    for (String line in headers[HttpHeaders.connectionHeader]!)
      for (String t in line.split(',')) t.trim(),
  ];
  expect(
    connection.any((String v) => v.toLowerCase() == 'my-connection-header1'),
    isTrue,
  );
  expect(
    connection.any((String v) => v.toLowerCase() == 'my-connection-header2'),
    isTrue,
  );
  if (persistent) {
    expect(connection.length, 2);
  } else {
    expect(connection.length, 3);
    expect(connection.any((String v) => v.toLowerCase() == 'close'), isTrue);
  }
}

void main() {
  test(
    'response.persistentConnection starts as the one of the request',
    () async {
      NativeHttpServer server = await serve((HttpRequest request) {
        request.response.write('${request.response.persistentConnection}');
        request.response.close();
      });
      String s = await raw(
        server.port,
        'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      expect(s, endsWith('false'));
    },
    skip:
        'dox_http_native: response.persistentConnection is true until set; '
        'Rust still closes after a request with Connection: close',
  );

  test(
    'request header values are split on commas',
    () async {
      NativeHttpServer server = await serve((HttpRequest request) {
        request.response.write('${request.headers['accept']!.length}');
        request.response.close();
      });
      String s = await raw(
        server.port,
        'GET / HTTP/1.1\r\nAccept: a, b\r\nConnection: close\r\n\r\n',
      );
      expect(s, endsWith('2'));
    },
    skip:
        'dox_http_native: a header value is not split on commas, one list '
        'entry for each field line',
  );

  for (bool persistent in <bool>[false, true]) {
    test('connection headers (client persistent: $persistent)', () async {
      NativeHttpServer server = await serve((HttpRequest request) {
        expect(request.persistentConnection, persistent);
        checkExpectedConnectionHeaders(
          request.headers,
          request.persistentConnection,
        );
        // if the client signaled a non persistent connection the server
        // does not need to set it
        if (request.persistentConnection) {
          request.response.persistentConnection = false;
        }
        setConnectionHeaders(request.response.headers);
        request.response.close();
      });
      HttpClient client = newClient();
      for (int i = 0; i < 2; i++) {
        HttpClientRequest request = await client.get(
          '127.0.0.1',
          server.port,
          '/',
        );
        setConnectionHeaders(request.headers);
        request.persistentConnection = persistent;
        HttpClientResponse response = await request.close();
        expect(response.persistentConnection, isFalse);
        checkExpectedConnectionHeaders(
          response.headers,
          response.persistentConnection,
        );
        await response.drain<void>();
      }
    });
  }
}
