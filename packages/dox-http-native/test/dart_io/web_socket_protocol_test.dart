// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/web_socket_protocol_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('empty protocol list: no protocol', () async {
    NativeHttpServer server = await serveWebSockets(
      (WebSocket ws) => ws.close(),
    );
    WebSocket client = await WebSocket.connect(
      wsUrl(server),
      protocols: <String>[],
    );
    expect(client.protocol, isNull);
    await client.close();
  });

  for (String used in <String>['v1.example.com', 'v2.example.com']) {
    test('protocol $used', () async {
      NativeHttpServer server = await serveWebSockets((WebSocket ws) {
        expect(ws.protocol, used);
        ws.close();
      }, protocol: used);
      WebSocket client = await WebSocket.connect(
        wsUrl(server),
        protocols: <String>['v1.example.com', 'v2.example.com'],
      );
      expect(client.protocol, used);
      await client.close();
    });
  }

  test('protocol the client did not offer: 500 and an error', () async {
    Object? error;
    NativeHttpServer server = await serve((HttpRequest request) async {
      try {
        await NativeWebSocket.upgrade(request, protocol: 'v2.example.com');
      } on WebSocketException catch (e) {
        error = e;
      }
    });
    await expectLater(
      WebSocket.connect(wsUrl(server), protocols: <String>['v1.example.com']),
      throwsA(isA<WebSocketException>()),
    );
    expect(error, isA<WebSocketException>());
    String text = await raw(
      server.port,
      'GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\n'
      'Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n'
      'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n'
      'Sec-WebSocket-Protocol: v1.example.com\r\n\r\n',
    );
    expect(statusOf(text), 500);
    await server.close();
  });

  test('client offered no protocol: the answer has none', () async {
    NativeHttpServer server = await serveWebSockets(
      (WebSocket ws) => ws.close(),
      protocol: 'v2.example.com',
    );
    WebSocket client = await WebSocket.connect(wsUrl(server));
    expect(client.protocol, isNull);
    await client.close();
    await server.close();
  });

  test('protocol with a line break is refused', () async {
    Object? error;
    NativeHttpServer server = await serve((HttpRequest request) async {
      try {
        await NativeWebSocket.upgrade(request, protocol: 'a\r\nInjected: yes');
      } on FormatException catch (e) {
        error = e;
        request.response.statusCode = 400;
        await request.response.close();
      }
    });
    String text = await raw(
      server.port,
      'GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
      'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n',
    );
    expect(error, isNotNull);
    expect(text.toLowerCase(), isNot(contains('\r\ninjected')));
  });

  test(
    'selector that throws',
    () {},
    skip: 'dox_http_native: upgrade takes a protocol, not a protocolSelector',
  );
  test(
    'bad upgrade response (client)',
    () {},
    skip: 'dox_http_native: client-only test',
  );
}
