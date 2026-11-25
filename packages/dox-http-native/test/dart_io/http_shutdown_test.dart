// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_shutdown_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  for (int total in <int>[1, 10]) {
    test('shutdown 1: server answers at once ($total)', () async {
      NativeHttpServer server = await serve(
        (HttpRequest request) => request.response.close(),
      );
      HttpClient client = newClient();
      await Future.wait(<Future<void>>[
        for (int i = 0; i < total; i++)
          client
              .get('127.0.0.1', server.port, '/')
              .then((HttpClientRequest r) => r.close())
              .then((HttpClientResponse r) => r.drain<void>()),
      ]);
    });
  }

  for ((int, int) c in <(int, int)>[(1, 10), (10, 10), (10, 1000)]) {
    test(
      'shutdown 2: server answers before the body (${c.$1}, ${c.$2})',
      () async {
        NativeHttpServer server = await serve((HttpRequest request) {
          request.response.write('!dlrow ,olleH');
          request.response.close();
        });
        HttpClient client = newClient();
        await Future.wait(<Future<void>>[
          for (int i = 0; i < c.$1; i++)
            client
                .get('127.0.0.1', server.port, '/')
                .then((HttpClientRequest request) {
                  request.contentLength = -1;
                  for (int j = 0; j < c.$2; j++) {
                    request.write('Hello, world!');
                  }
                  request.done.then<void>((_) {}, onError: (Object _) {});
                  return request.close();
                })
                .then(
                  (HttpClientResponse response) =>
                      response.drain<void>().catchError((Object _) {}),
                )
                .catchError((Object _) {}),
        ]);
      },
    );
  }

  for (int total in <int>[1, 10]) {
    test('shutdown 3: server answers after the body ($total)', () async {
      NativeHttpServer server = await serve((HttpRequest request) {
        request.listen(
          (_) {},
          onDone: () {
            request.response.write('!dlrow ,olleH');
            request.response.close();
          },
        );
      });
      HttpClient client = newClient();
      await Future.wait(<Future<void>>[
        for (int i = 0; i < total; i++)
          client
              .get('127.0.0.1', server.port, '/')
              .then((HttpClientRequest request) {
                request.contentLength = -1;
                request.write('Hello, world!');
                return request.close();
              })
              .then((HttpClientResponse r) => r.drain<void>()),
      ]);
    });
  }

  test('shutdown 4: close() completes when the connection is idle', () async {
    NativeHttpServer server = await serve((HttpRequest request) {
      request.listen((_) {}, onDone: () => request.response.close());
    });
    HttpClient client = newClient();
    HttpClientRequest request = await client.get('127.0.0.1', server.port, '/');
    HttpClientResponse response = await request.close();
    await response.drain<void>();
    client.close();
    await server.close().timeout(const Duration(seconds: 10));
  });

  for (int total in <int>[1, 10]) {
    test(
      'shutdown 5: the server drops all connections when the client closes ($total)',
      () async {
        NativeHttpServer server = await serve((HttpRequest request) {
          request.listen(
            (_) {},
            onDone: () {
              request.response.close();
              request.response.done.catchError((Object e) {});
            },
            onError: (Object error) {},
          );
        });
        HttpClient client = newClient();
        client.maxConnectionsPerHost = total;
        for (int i = 0; i < total; i++) {
          client
              .post('127.0.0.1', server.port, '/')
              .then((HttpClientRequest request) {
                request.add(<int>[0]);
                Future<HttpClientResponse?>.value(request.done)
                    .catchError((Object e) => null);
                return request.close();
              })
              .then((HttpClientResponse response) {})
              .catchError(
                (Object e) {},
                test: (Object e) => e is HttpException || e is SocketException,
              );
        }
        await Future<void>.delayed(const Duration(milliseconds: 300));
        client.close(force: true);
        // all connections are gone, so a graceful close completes
        await server.close().timeout(const Duration(seconds: 10));
      },
    );
  }
}
