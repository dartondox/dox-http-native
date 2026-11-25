// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/web_socket_abnormal_close_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  // The SDK test destroys the socket of the server side after a second. Here
  // the server closes all connections at once (force), which cuts the TCP
  // connection without a close frame.
  test('the server drops the connection: close code 1006', () async {
    late NativeHttpServer server;
    server = await serveWebSockets((WebSocket ws) {
      ws.drain<void>().catchError((Object e) {});
      Timer(const Duration(milliseconds: 300), () => server.close(force: true));
    });
    WebSocket client = await WebSocket.connect(wsUrl(server));
    await client.drain<void>().timeout(const Duration(seconds: 10));
    expect(client.closeCode, WebSocketStatus.abnormalClosure);
  });
}
