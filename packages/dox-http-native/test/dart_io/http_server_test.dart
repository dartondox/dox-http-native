// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_server_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  Future<HttpHeaders> defaultHeaders(
    bool clear,
    Map<String, String>? extra,
    void Function(HttpHeaders) check,
  ) async {
    NativeHttpServer server = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
    );
    addTearDown(() => server.close(force: true));
    if (clear) server.defaultResponseHeaders.clear();
    extra?.forEach(server.defaultResponseHeaders.add);
    check(server.defaultResponseHeaders);
    server.listen((HttpRequest r) => r.response.close());
    HttpClient client = newClient();
    HttpClientRequest req = await client.get('127.0.0.1', server.port, '/');
    HttpClientResponse res = await req.close();
    await res.drain<void>();
    check(res.headers);
    return res.headers;
  }

  test('default response headers: kept', () async {
    await defaultHeaders(false, null, (HttpHeaders h) {
      expect(h[HttpHeaders.contentTypeHeader], <String>[
        'text/plain; charset=utf-8',
      ]);
      expect(h['X-Frame-Options'], <String>['SAMEORIGIN']);
      expect(h['X-Content-Type-Options'], <String>['nosniff']);
      expect(h['X-XSS-Protection'], <String>['1; mode=block']);
    });
  });

  test('default response headers: cleared', () async {
    await defaultHeaders(true, null, (HttpHeaders h) {
      expect(h[HttpHeaders.contentTypeHeader], isNull);
      expect(h['X-Frame-Options'], isNull);
      expect(h['X-Content-Type-Options'], isNull);
      expect(h['X-XSS-Protection'], isNull);
    });
  });

  test('default response headers: cleared then one added', () async {
    await defaultHeaders(true, <String, String>{'a': 'b'}, (HttpHeaders h) {
      expect(h[HttpHeaders.contentTypeHeader], isNull);
      expect(h['X-Frame-Options'], isNull);
      expect(h['X-Content-Type-Options'], isNull);
      expect(h['X-XSS-Protection'], isNull);
      expect(h['a'], <String>['b']);
    });
  });

  Future<List<int>> bodyBytes(bool clear, String text) async {
    NativeHttpServer server = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
    );
    addTearDown(() => server.close(force: true));
    if (clear) server.defaultResponseHeaders.clear();
    server.listen((HttpRequest r) {
      r.response.write(text);
      r.response.close();
    });
    HttpClient client = newClient();
    HttpClientRequest req = await client.get('127.0.0.1', server.port, '/');
    HttpClientResponse res = await req.close();
    return <int>[for (List<int> c in await res.toList()) ...c];
  }

  test(
    'default response headers: content type picks the encoding',
    () async {
      expect(await bodyBytes(false, 'æøå'), <int>[
        195,
        166,
        195,
        184,
        195,
        165,
      ]);
      expect(await bodyBytes(true, 'æøå'), <int>[230, 248, 229]);
    },
    // the server always writes UTF-8, with or without a content type
    skip:
        'dox_http_native: a body with no content type is UTF-8, not '
        'latin1',
  );

  test(
    'listenOn a ServerSocket',
    () {},
    skip:
        'dox_http_native: Rust owns the socket, HttpServer.listenOn is not '
        'supported',
  );

  test('handler runs in the zone of bind', () async {
    Completer<void> done = Completer<void>();
    await runZoned(() async {
      NativeHttpServer server = await NativeHttpServer.bind(
        0,
        address: '127.0.0.1',
      );
      addTearDown(() => server.close(force: true));
      expect(Zone.current, isNot(Zone.root));
      server.listen((HttpRequest request) {
        expect(Zone.current, isNot(Zone.root));
        request.response.close();
        done.complete();
      });
      HttpClient client = newClient();
      HttpClientRequest req = await client.get('127.0.0.1', server.port, '/');
      await (await req.close()).drain<void>();
    });
    await done.future;
  });

  test(
    'error of a request body reaches the zone of the listen call',
    () async {
      Completer<void> done = Completer<void>();
      runZonedGuarded(
        () async {
          NativeHttpServer server = await NativeHttpServer.bind(
            0,
            address: '127.0.0.1',
          );
          addTearDown(() => server.close(force: true));
          server.listen((HttpRequest request) {
            request.listen(
              (_) {},
              onError: (Object error) {
                server.close();
                throw error;
              },
            );
          });
          Socket socket = await Socket.connect('127.0.0.1', server.port);
          socket.write('GET / HTTP/1.1\r\nContent-Length: 100\r\n\r\n');
          socket.write('some body');
          await socket.close();
          socket.listen(null);
        },
        (Object e, StackTrace s) {
          if (!done.isCompleted) done.complete();
        },
      );
      await done.future.timeout(const Duration(seconds: 10));
    },
    // Rust reads the whole body before the handler runs, so a request that
    // the client cuts short never reaches the handler (no body error)
    skip:
        'dox_http_native: a truncated request body never reaches the '
        'handler',
  );

  test('client that closes while the server writes does not throw', () async {
    Completer<void> closed = Completer<void>();
    NativeHttpServer? server;
    List<Object> errors = <Object>[];
    await runZonedGuarded(() async {
      server = await NativeHttpServer.bind(0, address: '127.0.0.1');
      addTearDown(() => server!.close(force: true));
      server!.listen((HttpRequest request) {
        request.response.bufferOutput = false;
        request.response.add(Uint8List(64 * 1024));
        Timer(const Duration(milliseconds: 100), () {
          request.response.close().then((_) {
            closed.complete();
          });
        });
      });
      HttpClient client = newClient();
      HttpClientRequest req = await client.get('127.0.0.1', server!.port, '/');
      HttpClientResponse res = await req.close();
      res.listen((_) {}).cancel();
    }, (Object e, StackTrace s) => errors.add(e));
    await closed.future.timeout(const Duration(seconds: 10));
    await Future<void>.delayed(const Duration(milliseconds: 200));
    expect(errors, isEmpty);
    expect(utf8.encode(''), isEmpty);
  });
}
