// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_server_early_client_close_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

Future<void> sendData(List<int> data, int port) async {
  Socket socket = await Socket.connect('127.0.0.1', port);
  socket.listen((List<int> d) => fail('No data response was expected'));
  socket.add(data);
  await socket.close();
  socket.destroy();
}

void main() {
  // [expectRequest]: the handler runs. For a request whose body is cut short
  // it does not on the native server (the body is read first).
  void early(String name, String data, {bool expectRequest = false}) {
    test(
      'early close: $name',
      () async {
        int requests = 0;
        Completer<void> requestDone = Completer<void>();
        Completer<void> errored = Completer<void>();
        NativeHttpServer server = await serve((HttpRequest request) {
          requests++;
          request.listen(
            (_) {},
            onDone: requestDone.complete,
            onError: (Object error) {
              expect(
                (error as HttpException).message,
                'Connection closed while receiving data',
              );
              errored.complete();
            },
          );
          request.response.close();
        });
        await sendData(data.codeUnits, server.port);
        if (expectRequest) {
          await errored.future.timeout(const Duration(seconds: 5));
          expect(requests, 1);
        } else {
          // the server is still alive and never ran the handler
          await Future<void>.delayed(const Duration(milliseconds: 300));
          expect(requests, 0);
          String ok = await raw(
            server.port,
            'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
          );
          expect(statusOf(ok), 200);
          expect(requests, 1);
        }
        await server.close().timeout(const Duration(seconds: 5));
      },
      skip: expectRequest
          ? 'dox_http_native: a request with a cut-short body never reaches '
                'the handler, there is no request stream error'
          : null,
    );
  }

  // close while sending the head
  early('G', 'G');
  early('GET /', 'GET /');
  early('GET / HTTP/1.1', 'GET / HTTP/1.1');
  early('GET / HTTP/1.1 and CRLF', 'GET / HTTP/1.1\r\n');
  // close while sending the content
  early(
    'content',
    'GET / HTTP/1.1\r\nContent-Length: 100\r\n\r\n',
    expectRequest: true,
  );
  early(
    'content, 1 byte',
    'GET / HTTP/1.1\r\nContent-Length: 100\r\n\r\n1',
    expectRequest: true,
  );

  test('early close while the answer is a file', () async {
    int done = 0;
    NativeHttpServer server = await serve((HttpRequest request) {
      File('pubspec.yaml')
          .openRead()
          .cast<List<int>>()
          .pipe(request.response)
          .catchError((Object e) {
            // ignore
          })
          .whenComplete(() => done++);
    });
    for (int i = 0; i < 10; i++) {
      Socket socket = await Socket.connect('127.0.0.1', server.port);
      socket.write('GET / HTTP/1.1\r\nContent-Length: 0\r\n\r\n');
      socket.close();
      await socket.done;
      socket.destroy();
    }
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect(done, 10);
  });

  test('cancel of the request subscription does not error', () async {
    int requests = 0;
    NativeHttpServer server = await serve((HttpRequest request) {
      requests++;
      StreamSubscription<List<int>> subscription = request.listen(
        (_) {},
        onError: (Object error) {},
      );
      subscription.cancel();
      request.response.close();
    });
    Socket socket = await Socket.connect('127.0.0.1', server.port);
    socket.write('GET / HTTP/1.1\r\nContent-Length: 10\r\n\r\ndata');
    socket.close();
    socket.listen((_) {}, onError: (Object _) {});
    socket.done.catchError((Object _) {});
    await Future<void>.delayed(const Duration(milliseconds: 300));
    // 4 of 10 body bytes came: no request reaches the handler
    expect(requests, 0);
    String ok = await raw(
      server.port,
      'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
    );
    expect(statusOf(ok), 200);
  });
}
