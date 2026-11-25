// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/web_socket_compression_test.dart.
// ignore_for_file: unawaited_futures
// The native server never agrees on permessage-deflate (see the README).
// What must hold: a client that asks for compression, with any parameters,
// still gets a working WebSocket, and no extension is answered.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

const String guid = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

void main() {
  for (bool client in <bool>[false, true]) {
    for (bool takeover in <bool>[false, true]) {
      test(
        'compression support (client: $client, no takeover: $takeover)',
        () async {
          NativeHttpServer server = await serveWebSockets((WebSocket ws) {
            ws.listen((dynamic m) {
              expect(m, 'Hello World');
              ws.add(m);
              ws.close();
            });
            ws.add('Hello World');
          });
          WebSocket ws = await WebSocket.connect(
            wsUrl(server),
            compression: CompressionOptions(
              enabled: client,
              serverNoContextTakeover: takeover,
              clientNoContextTakeover: takeover,
            ),
          );
          ws.add('Hello World');
          await ws
              .listen((dynamic m) => expect(m, 'Hello World'))
              .asFuture<void>()
              .timeout(const Duration(seconds: 15));
        },
      );
    }
  }

  for (int messages in <int>[1, 3]) {
    test('many messages with compression asked ($messages)', () async {
      NativeHttpServer server = await serveWebSockets((WebSocket ws) {
        ws.listen((dynamic m) {
          expect(m, 'Hello World');
          ws.add(m);
        });
      });
      WebSocket ws = await WebSocket.connect(wsUrl(server));
      int i = 1;
      Completer<void> done = Completer<void>();
      ws.listen((dynamic m) {
        expect(m, 'Hello World');
        if (i == messages) {
          ws.close();
          return;
        }
        ws.add('Hello World');
        i++;
      }, onDone: done.complete);
      ws.add('Hello World');
      await done.future.timeout(const Duration(seconds: 15));
    });
  }

  for (String extension in <String>[
    'permessage-deflate',
    'permessage-deflate; client_max_window_bits',
    'permessage-deflate; server_max_window_bits=10; client_max_window_bits=12; server_no_context_takeover',
    'permessage-deflate; server_max_window_bits=99',
    'permessage-deflate; client_no_context_takeover; bogus=1',
    'x-unknown-extension',
    'permessage-deflate; permessage-deflate',
  ]) {
    test('Sec-WebSocket-Extensions "$extension": no extension answered', () async {
      NativeHttpServer server = await serveWebSockets((WebSocket ws) {
        ws.listen((dynamic m) {
          ws.add(m);
          ws.close();
        });
      });
      Socket socket = await Socket.connect('127.0.0.1', server.port);
      addTearDown(socket.destroy);
      String nonce = base64.encode(List<int>.filled(16, 9));
      StringBuffer text = StringBuffer();
      Completer<void> head = Completer<void>();
      socket.listen((List<int> d) {
        text.write(latin1.decode(d));
        if (text.toString().contains('\r\n\r\n') && !head.isCompleted) {
          head.complete();
        }
      }, onError: (Object e) {});
      socket.write(
        'GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
        'Sec-WebSocket-Key: $nonce\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Extensions: $extension\r\n\r\n',
      );
      await socket.flush();
      await head.future.timeout(const Duration(seconds: 10));
      String response = text.toString().toLowerCase();
      expect(response, startsWith('http/1.1 101'));
      expect(
        response,
        contains(
          base64
              .encode(sha1.convert('$nonce$guid'.codeUnits).bytes)
              .toLowerCase(),
        ),
      );
      expect(response, isNot(contains('sec-websocket-extensions')));
      // a masked, uncompressed text frame "hi" echoes back
      socket.add(<int>[
        0x81,
        0x82,
        1,
        2,
        3,
        4,
        'h'.codeUnitAt(0) ^ 1,
        'i'.codeUnitAt(0) ^ 2,
      ]);
      await socket.flush();
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(text.toString(), contains('hi'));
    });
  }

  test(
    'a compressed frame (RSV1) without an agreed extension is refused',
    () async {
      int messages = 0;
      NativeHttpServer server = await serveWebSockets((WebSocket ws) {
        ws.listen((dynamic m) => messages++);
      });
      Socket socket = await Socket.connect('127.0.0.1', server.port);
      addTearDown(socket.destroy);
      StringBuffer text = StringBuffer();
      socket.listen(
        (List<int> d) => text.write(latin1.decode(d)),
        onError: (Object e) {},
      );
      socket.write(
        'GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
        'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n',
      );
      await socket.flush();
      await Future<void>.delayed(const Duration(milliseconds: 200));
      socket.add(<int>[0xC1, 0x82, 1, 2, 3, 4, 5 ^ 1, 6 ^ 2]);
      await socket.flush();
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(messages, 0);
      // close frame 1002 (protocol error)
      expect(text.toString().codeUnits, containsAllInOrder(<int>[0x88]));
    },
  );
}
