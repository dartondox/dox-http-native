// Copyright (c) 2021, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_client_parser_crlfs_tolerant_test.dart.
// ignore_for_file: unawaited_futures
// The SDK test is about the client parser. The same CR*LF sequences, sent to
// the server as the end of a request head.
import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

import 'dart:io';

void main() {
  int handled = 0;
  Future<int> port0() async {
    NativeHttpServer server = await serve((HttpRequest request) {
      handled++;
      request.response.write('Test!');
      request.response.close();
    });
    return server.port;
  }

  for (String end in <String>['\n\n', '\r\n\n', '\n\r\n', '\r\n\r\n']) {
    test('request head ended by ${end.codeUnits}', () async {
      int port = await port0();
      String text = await raw(
        port,
        'GET / HTTP/1.1$end',
        wait: const Duration(milliseconds: 500),
      );
      expect(statusOf(text), 200);
      expect(text, endsWith('Test!'));
    });
  }

  for (String end in <String>['\n\r', '\r\r\n\n', '\r\r', '\r']) {
    test('request head ended by ${end.codeUnits} is not a request', () async {
      int port = await port0();
      int before = handled;
      String text = await raw(
        port,
        'GET / HTTP/1.1$end',
        wait: const Duration(milliseconds: 500),
      );
      expect(handled, before);
      expect(statusOf(text), anyOf(400, 0));
    });
  }
}
