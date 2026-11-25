// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/web_socket_error_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  test(
    'server destroys a detached upgraded socket',
    () {},
    skip: 'dox_http_native: detachSocket is not supported',
  );

  // The same idea without detachSocket: the server drops the connections
  // right after the upgrade. The clients must see the end, not hang.
  test('force close of the server ends all WebSockets', () async {
    const int total = 10;
    int upgraded = 0;
    Completer<void> allUp = Completer<void>();
    NativeHttpServer server = await serveWebSockets((WebSocket ws) {
      ws.listen((_) {});
      if (++upgraded == total) allUp.complete();
    });
    List<Future<void>> closed = <Future<void>>[];
    for (int i = 0; i < total; i++) {
      Completer<void> done = Completer<void>();
      closed.add(done.future);
      WebSocket client = await WebSocket.connect(wsUrl(server));
      client.add('Hello, world!');
      client.listen((_) => fail('unexpected message'), onDone: done.complete);
    }
    await allUp.future.timeout(const Duration(seconds: 10));
    await server.close(force: true);
    await Future.wait(closed).timeout(const Duration(seconds: 10));
  });
}
