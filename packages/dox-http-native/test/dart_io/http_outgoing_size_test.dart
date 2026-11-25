// Copyright (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_outgoing_size_test.dart.
import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('several chunks near the buffer size lose no data', () async {
    Uint8List sendData = Uint8List(8 * 1024 - 4);
    for (int i = 0; i < sendData.length; i++) {
      sendData[i] = i % 256;
    }
    var server = await serve((HttpRequest request) {
      for (int i = 0; i < 8; i++) {
        request.response.add(sendData);
      }
      request.response.close();
    });
    HttpClient client = newClient();
    HttpClientRequest request = await client.get('127.0.0.1', server.port, '/');
    request.headers.set(HttpHeaders.acceptEncodingHeader, '');
    HttpClientResponse response = await request.close();
    List<int> buffer = <int>[for (List<int> c in await response.toList()) ...c];
    expect(buffer.length, sendData.length * 8);
    for (int i = 0; i < buffer.length; i++) {
      expect(buffer[i], sendData[i % sendData.length]);
    }
  });
}
