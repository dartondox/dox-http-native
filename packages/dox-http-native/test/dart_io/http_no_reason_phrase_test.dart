// (c) 2014, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_no_reason_phrase_test.dart.
// ignore_for_file: unawaited_futures
// The SDK test is about the client parsing a status line with no reason
// phrase. Server side: every status line of the server has a code and a
// well-formed line.
import 'package:dox_http_native/src/server.dart';
import 'package:test/test.dart';

import 'util.dart';

import 'dart:io';

void main() {
  for (int status in <int>[200, 500, 404]) {
    test('status line of $status is well formed', () async {
      NativeHttpServer server = await serve((HttpRequest request) {
        request.response.statusCode = status;
        request.response.close();
      });
      String text = await raw(
        server.port,
        'GET / HTTP/1.1\r\nConnection: close\r\n\r\n',
      );
      expect(text, matches(RegExp('^HTTP/1\\.1 $status [^\r\n]*\r\n')));
    });
  }
}
