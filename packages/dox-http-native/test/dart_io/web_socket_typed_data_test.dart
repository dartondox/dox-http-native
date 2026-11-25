// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/web_socket_typed_data_test.dart.
// ignore_for_file: unawaited_futures
import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

void main() {
  for (bool compression in <bool>[false, true]) {
    CompressionOptions options = compression
        ? CompressionOptions.compressionDefault
        : CompressionOptions.compressionOff;

    Future<WebSocket> connect(int port) =>
        WebSocket.connect('ws://127.0.0.1:$port/', compression: options);

    Future<void> echoTest(List<int> expected, List<List<int>> testData) async {
      int serverCount = 0;
      Completer<void> serverDone = Completer<void>();
      NativeHttpServer server = await serveWebSockets((WebSocket ws) {
        ws.listen(
          (dynamic m) {
            expect(m, expected);
            ws.add(testData[serverCount]);
            serverCount++;
          },
          onDone: () {
            expect(serverCount, testData.length);
            serverDone.complete();
          },
        );
      });
      WebSocket client = await connect(server.port);
      int count = 0;
      Completer<void> clientDone = Completer<void>();
      client.listen((dynamic m) {
        expect(m, expected);
        if (++count == testData.length) client.close();
      }, onDone: clientDone.complete);
      testData.forEach(client.add);
      await Future.wait(<Future<void>>[serverDone.future, clientDone.future])
          .timeout(const Duration(seconds: 15));
    }

    test('unsigned lists (compression: $compression)', () async {
      List<int> fill = List<int>.generate(256, (int i) => i);
      List<List<int>> data = <List<int>>[
        Uint8List(256),
        Uint8ClampedList(256),
        Uint16List(256),
        Uint32List(256),
        Uint64List(256),
      ];
      for (List<int> l in data) {
        l.setAll(0, fill);
      }
      await echoTest(fill, data);
    });

    test('signed lists (compression: $compression)', () async {
      List<int> fill = List<int>.generate(128, (int i) => i);
      List<List<int>> data = <List<int>>[
        Int8List(128),
        Int16List(128),
        Int32List(128),
        Int64List(128),
      ];
      for (List<int> l in data) {
        l.setAll(0, fill);
      }
      await echoTest(fill, data);
    });

    test(
      'out of range data from the client (compression: $compression)',
      () async {
        NativeHttpServer server = await serveWebSockets((WebSocket ws) {
          ws.listen((_) => fail('No message expected'));
        });
        List<List<int>> bad = <List<int>>[
          Uint16List(1)..[0] = 256,
          Uint32List(1)..[0] = 256,
          Uint64List(1)..[0] = 256,
          Int16List(1)..[0] = 256,
          Int16List(1)..[0] = -1,
          Int32List(1)..[0] = 256,
          Int32List(1)..[0] = -1,
          Int64List(1)..[0] = 256,
          Int64List(1)..[0] = -1,
          <int>[-1],
          <int>[256],
        ];
        for (List<int> data in bad) {
          WebSocket client = await connect(server.port);
          client.listen((_) => fail('No message expected'));
          client.add(data);
          client.close();
          await expectLater(
            client.done,
            throwsA(anything),
            reason: 'Message $data did not fail',
          );
        }
      },
    );

    test(
      'out of range data from the server (compression: $compression)',
      () async {
        List<List<int>> bad = <List<int>>[
          Uint16List(1)..[0] = 256,
          Uint32List(1)..[0] = 256,
          Uint64List(1)..[0] = 256,
          Int16List(1)..[0] = 256,
          Int16List(1)..[0] = -1,
          Int32List(1)..[0] = 256,
          Int32List(1)..[0] = -1,
          Int64List(1)..[0] = 256,
          Int64List(1)..[0] = -1,
          <int>[-1],
          <int>[256],
        ];
        // dart:io fails `done`, the native socket throws in `add` (nothing is sent)
        int refused = 0;
        Completer<void> allDone = Completer<void>();
        int closed = 0;
        NativeHttpServer server = await serveWebSockets((WebSocket ws) {
          ws.listen((dynamic m) {
            try {
              ws.add(bad[(m as List<int>)[0]]);
            } on ArgumentError {
              refused++;
            }
            ws.close();
            if (++closed == bad.length) allDone.complete();
          });
        });
        List<Future<void>> clients = <Future<void>>[];
        for (int i = 0; i < bad.length; i++) {
          Completer<void> done = Completer<void>();
          clients.add(done.future);
          WebSocket client = await connect(server.port);
          client.listen(
            (_) => fail('No message expected'),
            onDone: done.complete,
          );
          client.add(<int>[i]);
        }
        await Future.wait(clients).timeout(const Duration(seconds: 15));
        await allDone.future.timeout(const Duration(seconds: 15));
        expect(refused, bad.length);
      },
    );
  }
}
