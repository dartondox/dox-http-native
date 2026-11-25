// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_close_test.dart.
import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  const int connections = 10;

  void checkInfo(NativeHttpServer server) {
    expect(server.connectionsInfo().active, 0);
    expect(server.connectionsInfo().total, server.connectionsInfo().idle);
  }

  test('client and server close, nobody listens to the body', () async {
    int closed = 0;
    Completer<void> all = Completer<void>();
    late NativeHttpServer server;
    server = await serve((HttpRequest request) {
      request.response.close();
      request.response.done.then((_) {
        closed++;
        if (closed == connections) {
          checkInfo(server);
          all.complete();
        }
      });
    });
    HttpClient client = newClient();
    List<Future<void>> calls = <Future<void>>[];
    for (int i = 0; i < connections; i++) {
      calls.add(
        client
            .get('127.0.0.1', server.port, '/')
            .then((HttpClientRequest r) => r.close())
            .then((HttpClientResponse r) => r.drain<void>()),
      );
    }
    await Future.wait(calls);
    await all.future.timeout(const Duration(seconds: 10));
  });

  test('client close, server listens to the body', () async {
    int closed = 0;
    Completer<void> all = Completer<void>();
    late NativeHttpServer server;
    void check() {
      closed++;
      if (closed == connections * 2) {
        checkInfo(server);
        all.complete();
      }
    }

    server = await serve((HttpRequest request) {
      request.listen(
        (_) {},
        onDone: () {
          request.response.close();
          request.response.done.then((_) => check());
        },
      );
    });
    HttpClient client = newClient();
    for (int i = 0; i < connections; i++) {
      client
          .get('127.0.0.1', server.port, '/')
          .then((HttpClientRequest r) => r.close())
          .then((HttpClientResponse r) => check());
    }
    await all.future.timeout(const Duration(seconds: 10));
  });

  for (bool flush in <bool>[true, false]) {
    test(
      'client closes while the server sends the answer (flush: $flush)',
      skip: flush ? null : 'dox_http_native: bufferOutput has no effect, the head and the bytes go out only on flush, close or addStream',
      () async {
        Uint8List buffer = Uint8List(64 * 1024);
        Random rand = Random();
        for (int i = 0; i < buffer.length; i++) {
          buffer[i] = rand.nextInt(256);
        }
        int closed = 0;
        Completer<void> all = Completer<void>();
        late NativeHttpServer server;
        void check() {
          closed++;
          // wait for both the server and the client to see the close
          if (closed == connections * 2) {
            checkInfo(server);
            all.complete();
          }
        }

        server = await serve((HttpRequest request) {
          Timer timer = Timer.periodic(const Duration(milliseconds: 50), (_) {
            request.response.add(buffer);
            if (flush) request.response.flush();
          });
          request.response.done.catchError((Object _) {}).whenComplete(() {
            check();
            timer.cancel();
          });
        });
        HttpClient client = newClient();
        for (int i = 0; i < connections; i++) {
          client
              .get('127.0.0.1', server.port, '/')
              .then((HttpClientRequest r) => r.close())
              .then((HttpClientResponse response) {
                StreamSubscription<List<int>> subscription = response.listen(
                  (_) {},
                );
                Timer(const Duration(milliseconds: 20), () {
                  subscription.cancel();
                  check();
                });
              });
        }
        await all.future.timeout(const Duration(seconds: 20));
      },
    );
  }

  test(
    'client closes while sending the request: the request stream errors',
    () async {
      int serverErrors = 0;
      Completer<void> all = Completer<void>();
      var server = await serve((HttpRequest request) {
        request.listen(
          (_) {},
          onError: (Object _) {
            if (++serverErrors == connections) all.complete();
          },
        );
      });
      HttpClient client = newClient();
      for (int i = 0; i < connections; i++) {
        client
            .post('127.0.0.1', server.port, '/')
            .then((HttpClientRequest request) {
              request.contentLength = 110;
              request.write('0123456789');
              return request.close();
            })
            .catchError((Object _) => null);
      }
      await all.future.timeout(const Duration(seconds: 10));
    },
    skip:
        'dox_http_native: Rust reads the whole body before the handler runs, '
        'a request that is cut short never reaches the handler',
  );
}
