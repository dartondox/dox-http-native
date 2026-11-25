// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/web_socket_ping_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  // The SDK test has a server that never answers a ping (it destroys the
  // socket). The ping of a native server is sent by Rust: a client that does
  // not answer pings is closed, and a client that answers keeps the socket.
  test('server pingInterval: a client that answers keeps the socket', () async {
    Completer<void> pings = Completer<void>();
    NativeHttpServer server = await serveWebSockets((WebSocket ws) {
      ws.pingInterval = const Duration(milliseconds: 100);
      ws.listen((_) {});
    });
    WebSocket client = await WebSocket.connect(wsUrl(server));
    client.listen(
      (_) {},
      onDone: () {
        if (!pings.isCompleted) pings.complete();
      },
    );
    // the dart:io client answers pings by itself
    await Future<void>.delayed(const Duration(milliseconds: 700));
    expect(pings.isCompleted, isFalse);
    expect(client.readyState, WebSocket.open);
    await client.close();
  });

  test('a client that never answers a ping is closed', () async {
    Completer<void> closed = Completer<void>();
    NativeHttpServer server = await serveWebSockets((WebSocket ws) {
      ws.pingInterval = const Duration(milliseconds: 100);
      ws.listen((_) {}, onDone: closed.complete);
    });
    Socket raw = await Socket.connect('127.0.0.1', server.port);
    addTearDown(raw.destroy);
    raw.listen((_) {}, onError: (Object e) {});
    raw.write(
      'GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
      'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n',
    );
    await raw.flush();
    await closed.future.timeout(const Duration(seconds: 15));
  });

  test('ping timer is cancelled on close', () async {
    Completer<void> done = Completer<void>();
    NativeHttpServer server = await serveWebSockets((WebSocket ws) {
      ws.listen((_) {});
    });
    WebSocket client = await WebSocket.connect(wsUrl(server));
    client.pingInterval = const Duration(seconds: 100);
    client.listen((_) => fail('unexpected message'), onDone: done.complete);
    await client.close();
    await done.future.timeout(const Duration(seconds: 10));
  });
}
