// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_connection_close_test.dart.
import 'dart:async';
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test(
    'flush and addStream complete with the response',
    () async {
      Completer<List<Object?>> got = Completer<List<Object?>>();
      var server = await serve((HttpRequest request) async {
        HttpResponse response = request.response;
        Object? flushed = await response.flush();
        Object? added = await response.addStream(
          Stream<List<int>>.fromIterable(<List<int>>[
            <int>[1],
          ]),
        );
        await response.close();
        got.complete(<Object?>[flushed, added, response]);
      });
      await raw(server.port, 'GET / HTTP/1.1\r\nConnection: close\r\n\r\n');
      List<Object?> v = await got.future;
      expect(v[0], same(v[2]));
      expect(v[1], same(v[2]));
    },
    skip:
        'dox_http_native: flush and addStream complete with null, not the '
        'response',
  );

  for (bool closeRequest in <bool>[false, true]) {
    test('HTTP/1.0 close (closeRequest: $closeRequest)', () async {
      var server = await serve((HttpRequest r) => r.response.close());
      Socket socket = await Socket.connect('127.0.0.1', server.port);
      Completer<void> done = Completer<void>();
      socket.listen((_) {}, onDone: done.complete);
      socket.write('GET / HTTP/1.0\r\n\r\n');
      if (closeRequest) await socket.close();
      await done.future.timeout(const Duration(seconds: 10));
      socket.destroy();
    });

    test('HTTP/1.1 Connection: close (closeRequest: $closeRequest)', () async {
      var server = await serve((HttpRequest r) => r.response.close());
      Socket socket = await Socket.connect('127.0.0.1', server.port);
      Completer<void> done = Completer<void>();
      socket.listen((_) {}, onDone: done.complete);
      socket.write('GET / HTTP/1.1\r\nConnection: close\r\n\r\n');
      if (closeRequest) await socket.close();
      await done.future.timeout(const Duration(seconds: 10));
      socket.destroy();
    });
  }

  test('stream response, client closes', () async {
    Completer<void> done = Completer<void>();
    var server = await serve((HttpRequest request) {
      Timer timer = Timer.periodic(Duration.zero, (_) {
        request.response.write(
          'data:${DateTime.now().millisecondsSinceEpoch}\n\n',
        );
        request.response.flush();
      });
      request.response.done
          .whenComplete(() {
            timer.cancel();
            done.complete();
          })
          .catchError((Object _) {});
    });
    HttpClient client = newClient();
    HttpClientRequest request = await client.getUrl(
      Uri.parse('http://127.0.0.1:${server.port}'),
    );
    HttpClientResponse response = await request.close();
    int bytes = 0;
    response.listen((List<int> data) {
      bytes += data.length;
      if (bytes > 100) client.close(force: true);
    }, onError: (Object _) {});
    await done.future.timeout(const Duration(seconds: 10));
  });

  /// Starts a raw request, reads the first bytes, and leaves (destroy).
  Future<void> leaveAfterFirstBytes(int port, {bool rst = false}) async {
    Socket socket = await Socket.connect('127.0.0.1', port);
    socket.write('GET / HTTP/1.1\r\nHost: localhost\r\n\r\n');
    await socket.flush();
    await socket.first;
    socket.destroy();
  }

  test('writes, flushes and close stay silent after a disconnect', () async {
    Completer<Object?> doneValue = Completer<Object?>();
    Completer<void> finished = Completer<void>();
    var server = await serve((HttpRequest request) async {
      HttpResponse response = request.response..bufferOutput = false;
      unawaited(response.done.then(doneValue.complete));
      while (!doneValue.isCompleted) {
        response.write('data: tick\n\n');
        await response.flush();
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      // later writes, flushes and close remain silent after the disconnect
      response.write('data: after-disconnect\n\n');
      await response.flush();
      expect(await response.close(), response);
      finished.complete();
    });
    await leaveAfterFirstBytes(server.port);
    expect(await doneValue.future, isA<HttpResponse>());
    await finished.future.timeout(const Duration(seconds: 10));
  });

  test('close inside the done callback during a flush', () async {
    Completer<Object?> closeFromDone = Completer<Object?>();
    Completer<void> finished = Completer<void>();
    var server = await serve((HttpRequest request) async {
      HttpResponse response = request.response..bufferOutput = false;
      unawaited(
        response.done.then((_) {
          // must not throw a StateError while flush() unwinds
          closeFromDone.complete(response.close());
        }),
      );
      while (!closeFromDone.isCompleted) {
        response.write('data: tick\n\n');
        await response.flush();
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      expect(await closeFromDone.future, response);
      finished.complete();
    });
    await leaveAfterFirstBytes(server.port);
    await finished.future.timeout(const Duration(seconds: 10));
  });

  test(
    'flush and addStream after an unflushed write and a disconnect',
    () async {
      Completer<void> finished = Completer<void>();
      var server = await serve((HttpRequest request) async {
        HttpResponse response = request.response..bufferOutput = false;
        bool isDone = false;
        unawaited(response.done.then((_) => isDone = true));
        // the first part goes out with a flush, then the client leaves
        response.write('data: tick\n\n');
        await response.flush();
        while (!isDone) {
          response.write('data: tick\n\n');
          await Future<void>.delayed(const Duration(milliseconds: 10));
        }
        // must not hang on a stale state
        await response.flush();
        await response.addStream(
          Stream<List<int>>.fromIterable(<List<int>>[
            <int>[1, 2, 3],
          ]),
        );
        response.write('data: after\n\n');
        expect(await response.close(), response);
        finished.complete();
      });
      await leaveAfterFirstBytes(server.port);
      await finished.future.timeout(const Duration(seconds: 10));
    },
  );

  test('addStream and a disconnect: done completes first', skip: 'dox_http_native: addStream starts the answer after 64 KB, not at once (call flush first)', () async {
    Completer<Object?> doneValue = Completer<Object?>();
    Completer<void> finished = Completer<void>();
    var server = await serve((HttpRequest request) async {
      HttpResponse response = request.response..bufferOutput = false;
      unawaited(response.done.then(doneValue.complete));
      Stream<List<int>> stream = Stream<List<int>>.periodic(
        const Duration(milliseconds: 10),
        (_) => 'data: tick\n\n'.codeUnits,
      );
      await response.addStream(stream);
      // done must complete before close() is called
      expect(await doneValue.future, response);
      response.write('data: after-stream\n\n');
      expect(await response.close(), response);
      finished.complete();
    });
    await leaveAfterFirstBytes(server.port);
    await finished.future.timeout(const Duration(seconds: 10));
    expect(await doneValue.future, isA<HttpResponse>());
  });

  test('reset between flushes', () async {
    Completer<void> firstFlushDone = Completer<void>();
    Completer<void> clientDestroyed = Completer<void>();
    Completer<Object?> doneValue = Completer<Object?>();
    Completer<void> finished = Completer<void>();
    var server = await serve((HttpRequest request) async {
      HttpResponse response = request.response..bufferOutput = false;
      unawaited(response.done.then(doneValue.complete));
      response.write('data: unread-before-rst\n\n');
      await response.flush();
      firstFlushDone.complete();
      await clientDestroyed.future;
      await Future<void>.delayed(const Duration(milliseconds: 50));
      while (!doneValue.isCompleted) {
        response.write('data: after-idle-rst\n\n');
        await response.flush();
        if (!doneValue.isCompleted) {
          await Future<void>.delayed(const Duration(milliseconds: 20));
        }
      }
      expect(await doneValue.future, response);
      response.write('data: after-done\n\n');
      await response.flush();
      expect(await response.close(), response);
      finished.complete();
    });
    // do not read the answer: destroy() closes with unread data and the OS
    // may send a RST while the server is between two flushes
    Socket socket = await Socket.connect('127.0.0.1', server.port);
    socket.write('GET / HTTP/1.1\r\nHost: localhost\r\n\r\n');
    await socket.flush();
    await firstFlushDone.future;
    await Future<void>.delayed(const Duration(milliseconds: 20));
    socket.destroy();
    clientDestroyed.complete();
    await finished.future.timeout(const Duration(seconds: 10));
  });
}
