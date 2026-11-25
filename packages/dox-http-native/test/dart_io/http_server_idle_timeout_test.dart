// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_server_idle_timeout_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

// The native server takes `idleTimeout` in `bind`, it has no setter.
void main() {
  test('idle timeout after a request', () async {
    NativeHttpServer server = await serve(
      (HttpRequest request) => request.response.close(),
      idleTimeout: const Duration(milliseconds: 100),
    );
    Socket socket = await Socket.connect('127.0.0.1', server.port);
    Completer<void> done = Completer<void>();
    socket.listen(null, onDone: done.complete);
    socket.write('GET / HTTP/1.1\r\nContent-Length: 0\r\n\r\n');
    await done.future.timeout(const Duration(seconds: 5));
    await socket.close();
  });

  test('idle timeout before a request', () async {
    NativeHttpServer server = await serve(
      (HttpRequest request) => request.response.close(),
      idleTimeout: const Duration(milliseconds: 100),
    );
    Socket socket = await Socket.connect('127.0.0.1', server.port);
    Completer<void> done = Completer<void>();
    socket.listen(null, onDone: done.complete);
    await done.future.timeout(const Duration(seconds: 5));
    await socket.close();
  });
}
