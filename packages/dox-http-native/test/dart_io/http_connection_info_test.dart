// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_connection_info_test.dart.
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test('connection info', () async {
    int? clientPort;
    var server = await serve((HttpRequest request) {
      HttpResponse response = request.response;
      expect(request.connectionInfo!.remoteAddress, isA<InternetAddress>());
      expect(response.connectionInfo!.remoteAddress, isA<InternetAddress>());
      expect(request.connectionInfo!.localPort, request.headers.port);
      expect(response.connectionInfo!.localPort, request.headers.port);
      expect(clientPort, isNotNull);
      expect(request.connectionInfo!.remotePort, clientPort);
      expect(response.connectionInfo!.remotePort, clientPort);
      request.listen((_) {}, onDone: () => request.response.close());
    }, host: '0.0.0.0');
    HttpClient client = newClient();
    HttpClientRequest request = await client.get('127.0.0.1', server.port, '/');
    expect(request.connectionInfo!.remoteAddress, isA<InternetAddress>());
    expect(request.connectionInfo!.remotePort, server.port);
    clientPort = request.connectionInfo!.localPort;
    HttpClientResponse response = await request.close();
    expect(response.connectionInfo!.remotePort, server.port);
    expect(response.connectionInfo!.localPort, clientPort);
    await response.drain<void>();
  });
}
