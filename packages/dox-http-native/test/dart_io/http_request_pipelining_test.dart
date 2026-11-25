// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_request_pipelining_test.dart.
import 'dart:async';
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('pipelined requests are all answered', () async {
    const int requestCount = 100;
    int count = 0;
    Completer<void> done = Completer<void>();
    var server = await serve((HttpRequest request) {
      count++;
      request.response.write(request.uri.path);
      request.response.close();
      if (request.uri.path == '/done') {
        request.response.done.then((_) {
          expect(count, requestCount + 1);
          done.complete();
        });
      }
    });
    Socket s = await Socket.connect('127.0.0.1', server.port);
    addTearDown(s.destroy);
    StringBuffer out = StringBuffer();
    Completer<void> closed = Completer<void>();
    s.listen(
      (List<int> data) => out.write(String.fromCharCodes(data)),
      onDone: closed.complete,
    );
    for (int i = 0; i < requestCount; i++) {
      s.write('GET /$i HTTP/1.1\r\nX-Header-1: 111\r\n\r\n');
    }
    s.write('GET /done HTTP/1.1\r\nConnection: close\r\n\r\n');
    await s.close();
    await done.future.timeout(const Duration(seconds: 10));
    await closed.future.timeout(const Duration(seconds: 10));
    // the answers come in request order
    String text = out.toString();
    int last = -1;
    for (int i = 0; i < requestCount; i++) {
      int at = text.indexOf('\r\n\r\n/$i', last + 1);
      expect(at, greaterThan(last), reason: 'answer $i');
      last = at;
    }
  });
}
