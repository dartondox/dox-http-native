// Copyright (c) 2020, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_big_header_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  // The client of dart:io limits the size of a response head. Here the server
  // writes big response headers: it must not crash or hang, and must serve on.
  test('server writes a big response head and keeps serving', () async {
    String str = 'a' * 1000;
    NativeHttpServer server = await serve((HttpRequest request) async {
      if (request.uri.path == '/big') {
        for (int i = 0; i < 2000; i++) {
          request.response.headers.add('dummy', str);
        }
      }
      await request.response.close();
    });
    Socket socket = await Socket.connect('127.0.0.1', server.port);
    socket.write('GET /big HTTP/1.1\r\nConnection: close\r\n\r\n');
    int total = 0;
    await socket.forEach((List<int> d) => total += d.length);
    socket.destroy();
    expect(total, greaterThan(0));
    // a normal request still works
    String ok = await raw(
      server.port,
      'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    expect(statusOf(ok), 200);
  });

  // The server limits the size of a request head (16 KB on the native server).
  for (int size in <int>[64, 6400, 640000, 64000000]) {
    test('request head with $size bytes of headers', () async {
      NativeHttpServer server = await serve((HttpRequest request) async {
        await request.response.close();
      });
      String str = 'a' * 1000;
      StringBuffer head = StringBuffer('GET / HTTP/1.1\r\n');
      for (int s = 0; s < size; s += 1000) {
        head.write('dummy: $str\r\n');
        if (head.length > 70 * 1000 * 1000) break;
      }
      head.write('\r\n');
      String text = await raw(server.port, head.toString());
      if (size < 8000) {
        expect(statusOf(text), 200);
      } else {
        // 431 (head too large), or the connection closes
        expect(statusOf(text), anyOf(431, 400, 0));
        // and the server is still alive
        String ok = await raw(
          server.port,
          'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
        );
        expect(statusOf(ok), 200);
      }
    });
  }

  test('request head over the limit is never given to the handler', () async {
    int handled = 0;
    NativeHttpServer server = await serve((HttpRequest request) async {
      handled++;
      await request.response.close();
    });
    String head = 'GET / HTTP/1.1\r\nX: ${'a' * (2 * 1024 * 1024)}\r\n\r\n';
    String text = await raw(server.port, head);
    expect(statusOf(text), 431);
    expect(handled, 0);
  });

  test('obs-fold at the head limit is refused, not crashed', () async {
    const int limit = 1024 * 1024;
    NativeHttpServer server = await serve((HttpRequest request) async {
      await request.response.close();
    });
    String head = 'GET / HTTP/1.1\r\nX: ${'a' * (limit - 4)}\r\n b\r\n\r\n';
    String text = await raw(server.port, head);
    expect(statusOf(text), anyOf(431, 400, 0));
    expect(utf8.encode(text), isNotNull);
    String ok = await raw(
      server.port,
      'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    expect(statusOf(ok), 200);
    await Future<void>.delayed(Duration.zero);
  });
}
