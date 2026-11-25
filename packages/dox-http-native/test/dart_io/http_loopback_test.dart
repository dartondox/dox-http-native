// Copyright (c) 2017, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_loopback_test.dart.
// The SDK test checks `RawServerSocket` and `RawSocket` on loopback
// addresses. It has no HTTP server. Here: a native server on the IPv4
// loopback address is not reachable over IPv6, and the reverse.
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  Future<void> failure(String serverHost, InternetAddress clientAddr) async {
    var server = await serve(
      (HttpRequest r) => r.response.close(),
      host: serverHost,
    );
    Socket? socket;
    try {
      socket = await Socket.connect(
        clientAddr,
        server.port,
        timeout: const Duration(seconds: 2),
      );
      fail('Unexpected connection to $serverHost via $clientAddr');
    } on SocketException {
      // expected
    } finally {
      socket?.destroy();
    }
  }

  Future<bool> hasV6() async {
    try {
      (await ServerSocket.bind('::1', 0)).close();
      return true;
    } catch (_) {
      return false;
    }
  }

  test('IPv4 server is not reachable over IPv6', () async {
    if (!await hasV6()) return;
    await failure('127.0.0.1', InternetAddress.loopbackIPv6);
  });
  test('IPv6 server is not reachable over IPv4', () async {
    if (!await hasV6()) return;
    await failure('::1', InternetAddress.loopbackIPv4);
  });
  for (String host in <String>['127.0.0.1', '::1']) {
    test('server on $host answers on $host', () async {
      if (host == '::1' && !await hasV6()) return;
      var server = await serve(
        (HttpRequest r) => r.response.close(),
        host: host,
      );
      HttpClient client = newClient();
      HttpClientRequest request = await client.open(
        'GET',
        host,
        server.port,
        '/',
      );
      expect((await request.close()).statusCode, 200);
    });
  }
}
