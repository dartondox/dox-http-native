// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_parser_test.dart.
// ignore_for_file: unawaited_futures
// The SDK test calls the private `_HttpParser` of dart:io. This port sends the
// same request bytes over a raw socket to the native server (in one piece,
// in pieces of 10 bytes and in pieces of 1 byte, with CRLF and with bare LF
// line ends) and looks at what the handler sees, and at the status. The
// response parser tests are client tests and are not ported.
//
// Added for safety: smuggling inputs (Content-Length with Transfer-Encoding,
// two different lengths, bad lengths), bad chunked bodies, bad header names
// and values, and limits.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';
import 'package:test/test.dart';

import 'util.dart';

class Seen {
  Seen(this.method, this.uri, this.version, this.headers, this.body);
  final String method;
  final String uri;
  final String version;
  final Map<String, List<String>> headers;
  final List<int> body;
}

late NativeHttpServer server;
List<Seen> seen = <Seen>[];

/// Sends [request] (latin1) in pieces of [chunk] bytes (0: one piece) and
/// reads the answer until the server closes, or until it is quiet for
/// [quiet].
Future<String> send(
  String request, {
  int chunk = 0,
  Duration quiet = const Duration(milliseconds: 250),
}) async {
  Socket socket = await Socket.connect('127.0.0.1', server.port);
  socket.setOption(SocketOption.tcpNoDelay, true);
  StringBuffer out = StringBuffer();
  Completer<void> done = Completer<void>();
  Timer? idle;
  void arm() {
    idle?.cancel();
    idle = Timer(quiet, () {
      if (!done.isCompleted) done.complete();
    });
  }

  socket.listen(
    (List<int> d) {
      out.write(latin1.decode(d));
      arm();
    },
    onError: (Object e) {},
    onDone: () {
      if (!done.isCompleted) done.complete();
    },
  );
  List<int> bytes = latin1.encode(request);
  try {
    if (chunk <= 0) {
      socket.add(bytes);
    } else {
      for (int i = 0; i < bytes.length; i += chunk) {
        socket.add(
          bytes.sublist(i, i + chunk > bytes.length ? bytes.length : i + chunk),
        );
        await socket.flush();
      }
    }
    await socket.flush();
  } on SocketException {
    // the server closed: a bad request
  }
  arm();
  await done.future.timeout(const Duration(seconds: 10), onTimeout: () {});
  idle?.cancel();
  socket.destroy();
  return out.toString();
}

const List<int> sizes = <int>[0, 10, 1];

void main() {
  setUpAll(() async {
    server = await NativeHttpServer.bind(
      0,
      address: '127.0.0.1',
      maxBodySize: 1 << 20,
    );
    server.listen((HttpRequest request) async {
      List<int> body = <int>[for (List<int> c in await request.toList()) ...c];
      Map<String, List<String>> headers = <String, List<String>>{};
      request.headers.forEach((String n, List<String> v) => headers[n] = v);
      seen.add(
        Seen(
          request.method,
          request.uri.toString(),
          request.protocolVersion,
          headers,
          body,
        ),
      );
      request.response.write('ok');
      await request.response.close();
    });
  });
  tearDownAll(() => server.close(force: true));
  setUp(() => seen.clear());

  /// A request that the SDK parser accepts: the handler must see [method],
  /// [uri], [version], [headers] (first value) and [bodyLength] bytes.
  void valid(
    String name,
    String request,
    String method,
    String uri, {
    int bodyLength = 0,
    Map<String, String?> headers = const <String, String?>{},
    String version = '1.1',
    bool lf = true,
    String? skip,
  }) {
    for (bool bare in <bool>[false, if (lf) true]) {
      for (int chunk in sizes) {
        test('$name (${bare ? 'LF' : 'CRLF'}, chunk $chunk)', () async {
          String text = await send(
            bare ? request.replaceAll('\r', '') : request,
            chunk: chunk,
          );
          expect(statusOf(text), 200, reason: text);
          expect(seen.length, 1);
          Seen s = seen.single;
          expect(s.method, method);
          expect(s.uri, uri);
          expect(s.version, version);
          expect(s.body.length, bodyLength);
          headers.forEach((String n, String? v) {
            expect(s.headers[n]?[0], v, reason: n);
          });
        }, skip: skip);
      }
    }
  }

  /// A request that must not reach the handler: the status is [statuses]
  /// (or the connection closes with no answer).
  void invalid(
    String name,
    String request, {
    List<int> statuses = const <int>[400],
    bool lf = true,
    String? note,
  }) {
    for (bool bare in <bool>[false, if (lf) true]) {
      for (int chunk in sizes) {
        test('reject: $name (${bare ? 'LF' : 'CRLF'}, chunk $chunk)', () async {
          String text = await send(
            bare ? request.replaceAll('\r', '') : request,
            chunk: chunk,
          );
          expect(statusOf(text), anyOf(<Object>[0, ...statuses]), reason: text);
          expect(seen, isEmpty, reason: 'the handler saw $seen');
          // nothing is parsed from the bytes after the head: no second answer
          expect(
            'HTTP/1.1'.allMatches(text).length,
            lessThanOrEqualTo(1),
            reason: text,
          );
        });
      }
    }
  }

  group('valid requests', () {
    valid('GET /', 'GET / HTTP/1.1\r\n\r\n', 'GET', '/');
    valid(
      'GET /index.html',
      'GET /index.html HTTP/1.1\r\n\r\n',
      'GET',
      '/index.html',
    );
    for (String method in <String>[
      'OPTIONS',
      'HEAD',
      'POST',
      'PUT',
      'DELETE',
      'TRACE',
      'CONNECT',
      'PROPFIND',
      'PROPPATCH',
      'MKCOL',
      'COPY',
      'MOVE',
      'LOCK',
      'UNLOCK',
      'SEARCH',
      'H',
      'HT',
      'HTT',
      'HTTP',
      'HX',
      'HTX',
      'HTTX',
      'HTTPX',
    ]) {
      valid(
        'method $method',
        '$method / HTTP/1.1\r\n\r\n',
        method,
        '/',
        lf: false,
      );
    }
    valid('HTTP/1.0', 'GET / HTTP/1.0\r\n\r\n', 'GET', '/', version: '1.0');
    valid(
      'HTTP/1.0 keep-alive',
      'GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n',
      'GET',
      '/',
      version: '1.0',
    );
    valid(
      'one header',
      'POST /test HTTP/1.1\r\nAAA: AAA\r\n\r\n',
      'POST',
      '/test',
    );
    valid('no header', 'POST /test HTTP/1.1\r\n\r\n', 'POST', '/test');
    valid(
      'header values keep inner spaces',
      'POST /test HTTP/1.1\r\nHeader-A: AAA  aaa\r\nX-Header-B: bbb  BBB\r\n\r\n',
      'POST',
      '/test',
      headers: <String, String?>{
        'header-a': 'AAA  aaa',
        'x-header-b': 'bbb  BBB',
      },
    );
    valid(
      'header values are trimmed',
      'POST /test HTTP/1.1\r\nHeader-A:   \t AAA  aaa \t \r\nX-Header-B:   \t bbb  BBB  \t \r\n\r\n',
      'POST',
      '/test',
      headers: <String, String?>{
        'header-a': 'AAA  aaa',
        'x-header-b': 'bbb  BBB',
      },
    );
    valid(
      'empty header values',
      'POST /test HTTP/1.1\r\nEmpty-Header-1:\r\nEmpty-Header-2:\t  \t \r\n\r\n',
      'POST',
      '/test',
      headers: <String, String?>{'empty-header-1': '', 'empty-header-2': ''},
    );
    valid(
      'latin1 header value',
      'POST /test HTTP/1.1\r\nlatin1:   blåbærgrød\r\n\r\n',
      'POST',
      '/test',
      headers: <String, String?>{'latin1': 'blåbærgrød'},
    );
    valid(
      'Content-Length',
      'POST /test HTTP/1.1\r\nContent-Length: 10\r\n\r\n0123456789',
      'POST',
      '/test',
      bodyLength: 10,
    );
    valid(
      'Connection: close',
      'GET /test HTTP/1.1\r\nConnection: close\r\n\r\n',
      'GET',
      '/test',
    );
    const String chunkedBody = '5\r\n01234\r\n5\r\n56789\r\n0\r\n\r\n';
    valid(
      'chunked',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n$chunkedBody',
      'POST',
      '/test',
      bodyLength: 10,
      lf: false,
    );
    valid(
      'LWS around chunked',
      'POST /test HTTP/1.1\r\nTransfer-Encoding:   \t   chunked  \t \r\n\r\n$chunkedBody',
      'POST',
      '/test',
      bodyLength: 10,
      lf: false,
    );
    valid(
      'upper and lower case chunk size hex',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n'
          '1E\r\n012345678901234567890123456789\r\n1e\r\n012345678901234567890123456789\r\n0\r\n\r\n',
      'POST',
      '/test',
      bodyLength: 60,
      lf: false,
    );
    valid(
      'chunk extensions',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n'
          '1E;xxx\r\n012345678901234567890123456789\r\n1E;yyy=zzz\r\n012345678901234567890123456789\r\n0\r\n\r\n',
      'POST',
      '/test',
      bodyLength: 60,
      lf: false,
    );
    valid(
      'websocket upgrade headers on a plain request',
      'GET /chat HTTP/1.1\r\nHost: server.example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
          'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nOrigin: http://example.com\r\nSec-WebSocket-Version: 13\r\n\r\n',
      'GET',
      '/chat',
      headers: <String, String?>{
        'host': 'server.example.com',
        'upgrade': 'websocket',
      },
    );
  }, skip: false);

  group('folded headers (obs-fold)', () {
    // RFC 9112 5.2: a server MUST reject a folded header, or replace the
    // fold with spaces. dart:io joins the lines. Native: see the checks.
    for (String request in <String>[
      'POST /test HTTP/1.1\r\nHeader-A: h\r\n ell\r\n o\r\nX-Header-B: w\r\n o\r\n r\r\n l\r\n d\r\n\r\n',
      'POST /test HTTP/1.1\r\nEmpty-Header-1:\r\nEmpty-Header-2:\r\n        \r\n\r\n',
    ]) {
      test('obs-fold is rejected or joined, never a second header', () async {
        String text = await send(request);
        if (statusOf(text) == 200) {
          expect(
            seen.single.headers['header-a'],
            anyOf(isNull, <String>['h ell o']),
          );
          expect(seen.single.headers.containsKey('ell'), isFalse);
        } else {
          expect(statusOf(text), anyOf(400, 0));
          expect(seen, isEmpty);
        }
      });
    }
    test(
      'obs-fold is joined like dart:io (h ell o)',
      () async {
        await send('POST /test HTTP/1.1\r\nHeader-A: h\r\n ell\r\n o\r\n\r\n');
        expect(seen.single.headers['header-a'], <String>['h ell o']);
      },
      skip:
          'dox_http_native: a folded header gets 400 (stricter, RFC 9112 5.2)',
    );
  });

  group('transfer encoding and content length (smuggling)', () {
    const String chunkedBody = '5\r\n01234\r\n5\r\n56789\r\n0\r\n\r\n';
    const String smuggled = 'GET /admin HTTP/1.1\r\nHost: backend\r\n\r\n';
    invalid(
      'Transfer-Encoding: gzip with Content-Length',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: gzip\r\nContent-Length: 38\r\n\r\n$smuggled',
      statuses: <int>[400, 501],
    );
    invalid(
      'Transfer-Encoding: chunked, gzip with Content-Length',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: chunked, gzip\r\nContent-Length: 38\r\n\r\n$smuggled',
      statuses: <int>[400, 501],
    );
    invalid(
      'chunked then gzip in two lines',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: gzip\r\nContent-Length: 38\r\n\r\n$smuggled',
      statuses: <int>[400, 501],
    );
    invalid(
      'Transfer-Encoding: x chunked',
      'POST /test HTTP/1.1\r\nContent-Length: 10\r\nTransfer-Encoding: x chunked\r\n\r\n0123456789',
      statuses: <int>[400, 501],
    );
    invalid(
      'Transfer-Encoding: x<TAB>chunked',
      'POST /test HTTP/1.1\r\nContent-Length: 10\r\nTransfer-Encoding: x\tchunked\r\n\r\n0123456789',
      statuses: <int>[400, 501],
    );
    // dart:io accepts these and drops the Content-Length. The native server
    // refuses a request with both (RFC 9112 6.3: it SHOULD be an error).
    invalid(
      'Content-Length and Transfer-Encoding: chunked',
      'POST /test HTTP/1.1\r\nContent-Length: 10\r\nTransfer-Encoding: chunked\r\n\r\n$chunkedBody',
    );
    invalid(
      'Transfer-Encoding: chunked and Content-Length',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 10\r\n\r\n$chunkedBody',
    );
    invalid(
      'CL.TE smuggling: Content-Length 4 hides a request after the chunks',
      'POST / HTTP/1.1\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n'
          '0\r\n\r\n$smuggled',
    );
    invalid(
      'TE.CL smuggling',
      'POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 6\r\n\r\n0\r\n\r\nX$smuggled',
    );
    invalid(
      'Transfer-Encoding: gzip, chunked',
      'POST /test HTTP/1.1\r\nContent-Length: 10\r\nTransfer-Encoding: gzip, chunked\r\n\r\n$chunkedBody',
      statuses: <int>[400, 501],
    );
    invalid(
      'Transfer-Encoding: identity, chunked',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: identity, chunked\r\n\r\n$chunkedBody',
      statuses: <int>[400, 501],
    );
    invalid(
      'Transfer-Encoding in two lines: gzip, chunked',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n$chunkedBody',
      statuses: <int>[400, 501],
    );
    invalid(
      'two Transfer-Encoding: chunked lines',
      'POST /test HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n$chunkedBody',
    );
    invalid(
      'Transfer-Encoding with a leading space in the name',
      'POST /test HTTP/1.1\r\n Transfer-Encoding: chunked\r\n\r\n$chunkedBody',
    );
    invalid(
      'Transfer-Encoding with a space before the colon',
      'POST /test HTTP/1.1\r\nTransfer-Encoding : chunked\r\nContent-Length: 4\r\n\r\n$chunkedBody',
    );
    invalid(
      'Transfer-Encoding: chunked in an HTTP/1.0 request',
      'POST /test HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n$chunkedBody',
    );
    invalid(
      'two different Content-Length',
      'POST /test HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 10\r\n\r\n0123456789',
    );
    invalid(
      'Content-Length list 5, 10',
      'POST /test HTTP/1.1\r\nContent-Length: 5, 10\r\n\r\n0123456789',
    );
    invalid(
      'Content-Length +5',
      'POST /test HTTP/1.1\r\nContent-Length: +5\r\n\r\n01234',
    );
    invalid(
      'Content-Length -1',
      'POST /test HTTP/1.1\r\nContent-Length: -1\r\n\r\n01234',
    );
    invalid(
      'Content-Length 0x5',
      'POST /test HTTP/1.1\r\nContent-Length: 0x5\r\n\r\n01234',
    );
    invalid(
      'Content-Length 5.0',
      'POST /test HTTP/1.1\r\nContent-Length: 5.0\r\n\r\n01234',
    );
    invalid(
      'Content-Length empty',
      'POST /test HTTP/1.1\r\nContent-Length:\r\n\r\n01234',
    );
    invalid(
      'Content-Length x',
      'POST /test HTTP/1.1\r\nContent-Length: x\r\n\r\n01234',
    );
    invalid(
      'Content-Length overflow',
      'POST /test HTTP/1.1\r\nContent-Length: 99999999999999999999999\r\n\r\n01234',
      statuses: <int>[400, 413],
    );
    invalid(
      'Content-Length with a space before the colon',
      'POST /test HTTP/1.1\r\nContent-Length : 5\r\n\r\n01234',
    );
    test('two equal Content-Length: no second request, one body', () async {
      String text = await send(
        'POST /t HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n01234',
      );
      // RFC 9110 8.6: equal values may be merged, or refused. Never two bodies.
      if (statusOf(text) == 200) {
        expect(seen.single.body.length, 5);
      } else {
        expect(statusOf(text), anyOf(400, 0));
        expect(seen, isEmpty);
      }
    });
    test(
      'the bytes after a Content-Length body are the next request',
      () async {
        String text = await send(
          'POST /one HTTP/1.1\r\nContent-Length: 3\r\n\r\nabcGET /two HTTP/1.1\r\n\r\n',
          quiet: const Duration(milliseconds: 400),
        );
        expect('HTTP/1.1 200'.allMatches(text).length, 2);
        expect(
          seen.map((Seen s) => s.uri),
          unorderedEquals(<String>['/one', '/two']),
        );
        expect(
          seen.firstWhere((Seen s) => s.uri == '/one').body,
          utf8.encode('abc'),
        );
      },
    );
    test(
      'a chunked body ends at the last chunk; the rest is the next request',
      () async {
        String text = await send(
          'POST /one HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\nGET /two HTTP/1.1\r\n\r\n',
          quiet: const Duration(milliseconds: 400),
        );
        expect('HTTP/1.1 200'.allMatches(text).length, 2);
        expect(
          seen.map((Seen s) => s.uri),
          unorderedEquals(<String>['/one', '/two']),
        );
      },
    );
  });

  group('bad chunked bodies', () {
    String chunked(String body) =>
        'POST /test HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n$body';
    // the handler may start before the body is whole on some servers, the
    // native server reads the body first: it must never see a bad body
    for (MapEntry<String, String> c in <String, String>{
      'chunk size is not hex': 'xyz\r\nabc\r\n0\r\n\r\n',
      'chunk size with a sign': '+3\r\nabc\r\n0\r\n\r\n',
      'negative chunk size': '-3\r\nabc\r\n0\r\n\r\n',
      'chunk size with 0x': '0x3\r\nabc\r\n0\r\n\r\n',
      'empty chunk size': '\r\nabc\r\n0\r\n\r\n',
      'chunk size too big (overflow)': 'FFFFFFFFFFFFFFFFFF\r\nabc\r\n0\r\n\r\n',
      'chunk longer than its size': '3\r\nabcd\r\n0\r\n\r\n',
      'chunk data not ended by CRLF': '3\r\nabcXY0\r\n\r\n',
      'chunk size with a space inside': '3 3\r\nabc\r\n0\r\n\r\n',
      'bare CR in the chunk size line': '3\rabc\r\n0\r\n\r\n',
      'no last chunk size': '3\r\nabc\r\n',
    }.entries) {
      test('reject: ${c.key}', () async {
        String text = await send(
          chunked(c.value),
          quiet: const Duration(milliseconds: 600),
        );
        // a body that never ends is not answered (waits for more bytes): the
        // client closes. Anything else is 400.
        expect(statusOf(text), anyOf(0, 400), reason: text);
        expect(
          seen,
          isEmpty,
          reason: 'the handler saw ${seen.map((Seen s) => s.body)}',
        );
      });
    }
    test(
      'a very long chunk extension does not hang or grow the memory',
      () async {
        String text = await send(
          chunked('3;${'a' * (2 * 1024 * 1024)}\r\nabc\r\n0\r\n\r\n'),
          quiet: const Duration(milliseconds: 600),
        );
        expect(
          statusOf(text),
          anyOf(0, 400, 413, 431),
          reason: text.length > 200 ? text.substring(0, 200) : text,
        );
        expect(
          seen.where((Seen s) => s.body.isNotEmpty && s.body.length != 3),
          isEmpty,
        );
      },
    );
    test('a chunked body over maxBodySize gets 413', () async {
      String big = 'a' * (1 << 16);
      StringBuffer b = StringBuffer();
      for (int i = 0; i < 20; i++) {
        b.write('${big.length.toRadixString(16)}\r\n$big\r\n');
      }
      b.write('0\r\n\r\n');
      String text = await send(
        chunked(b.toString()),
        quiet: const Duration(seconds: 1),
      );
      expect(
        statusOf(text),
        413,
        reason: text.length > 200 ? text.substring(0, 200) : text,
      );
      expect(seen, isEmpty);
    });
    test('chunk trailers are read and dropped', () async {
      String text = await send(
        chunked('3\r\nabc\r\n0\r\nX-Trailer: 1\r\n\r\n'),
      );
      expect(statusOf(text), 200);
      expect(seen.single.body, utf8.encode('abc'));
      expect(seen.single.headers.containsKey('x-trailer'), isFalse);
    });
  });

  group('bad request lines', () {
    invalid('no version', 'GET /\r\n\r\n');
    invalid('space, no version', 'GET / \r\n\r\n');
    invalid('no method', '/ HTTP/1.1\r\n\r\n');
    invalid('no target', 'GET HTTP/1.1\r\n\r\n');
    invalid('leading space', ' / HTTP/1.1\r\n\r\n');
    invalid('bad method char', '@ / HTTP/1.1\r\n\r\n');
    invalid('bad version name', 'GET / TTP/1.1\r\n\r\n');
    invalid('short version', 'GET / HTTP/1.\r\n\r\n');
    invalid(
      'bad header line',
      'GET / HTTP/1.1\r\nKeep-Alive: False\r\nbadheader\r\n\r\n',
    );
    invalid('two spaces in the request line', 'GET  / HTTP/1.1\r\n\r\n');
    invalid('tab in the request line', 'GET\t/ HTTP/1.1\r\n\r\n');
    invalid('NUL in the target', 'GET /a\u0000b HTTP/1.1\r\n\r\n');
    invalid('control char in the target', 'GET /a\u0001b HTTP/1.1\r\n\r\n');
    invalid('space in the target', 'GET /a b HTTP/1.1\r\n\r\n');
    invalid('lower case http', 'GET / http/1.1\r\n\r\n');
    invalid('HTTP/1.2', 'GET / HTTP/1.2\r\n\r\n', statuses: <int>[400, 505]);
    invalid(
      'HTTP/2.0 line on a plain connection',
      'GET / HTTP/2.0\r\n\r\n',
      statuses: <int>[400, 505],
    );
    invalid('HTTP/0.9', 'GET /\r\n', statuses: <int>[400, 505]);
    invalid(
      'bare CR in the request line',
      'GET /\rHTTP/1.1\r\n\r\n',
      lf: false,
    );
    invalid(
      'bare CR in a header value',
      'GET / HTTP/1.1\r\nX: a\rb\r\n\r\n',
      lf: false,
    );
    invalid('NUL in a header value', 'GET / HTTP/1.1\r\nX: a\u0000b\r\n\r\n');
    invalid('NUL in a header name', 'GET / HTTP/1.1\r\nX\u0000Y: a\r\n\r\n');
    invalid('space before the colon', 'GET / HTTP/1.1\r\nX : a\r\n\r\n');
    invalid('space inside a header name', 'GET / HTTP/1.1\r\nX Y: a\r\n\r\n');
    invalid('header name with a bad char', 'GET / HTTP/1.1\r\nX(Y): a\r\n\r\n');
    invalid('empty header name', 'GET / HTTP/1.1\r\n: a\r\n\r\n');
    invalid(
      'header line starts with a colon and a space',
      'GET / HTTP/1.1\r\n : a\r\n\r\n',
    );
    invalid('header with no colon', 'GET / HTTP/1.1\r\nXYZ\r\n\r\n');
    invalid('DEL in a header name', 'GET / HTTP/1.1\r\nX\u007fY: a\r\n\r\n');
    invalid(
      'control char in a header value',
      'GET / HTTP/1.1\r\nX: a\u0001b\r\n\r\n',
    );
  });

  group('header injection and splitting on the request side', () {
    test('a request header value with a bare LF is a new header line, not a request', () async {
      // lenient on LF (documented): the bytes after the LF are a header line
      String text = await send('GET / HTTP/1.1\nX: a\nY: b\n\n');
      expect(statusOf(text), anyOf(200, 400));
      if (seen.isNotEmpty) {
        expect(seen.single.headers['x'], <String>['a']);
        expect(seen.single.headers['y'], <String>['b']);
      }
    });
    test('a second request in the head bytes is not run twice', () async {
      await send(
        'GET /one HTTP/1.1\r\nX: a\r\n\r\nGET /two HTTP/1.1\r\nConnection: close\r\n\r\n',
        quiet: const Duration(milliseconds: 400),
      );
      expect(
        seen.map((Seen s) => s.uri),
        unorderedEquals(<String>['/one', '/two']),
      );
    });
  });

  group('limits', () {
    test('head over 16 KB gets 431', () async {
      String text = await send('GET / HTTP/1.1\r\nX: ${'a' * 20000}\r\n\r\n');
      expect(statusOf(text), 431);
      expect(seen, isEmpty);
    });
    test('head over 16 KB in pieces of 1000 bytes gets 431', () async {
      String text = await send(
        'GET / HTTP/1.1\r\nX: ${'a' * 20000}\r\n\r\n',
        chunk: 1000,
      );
      expect(statusOf(text), 431);
      expect(seen, isEmpty);
    });
    test('a head that never ends is cut at the limit', () async {
      String text = await send(
        'GET / HTTP/1.1\r\nX: ${'a' * 200000}',
        quiet: const Duration(milliseconds: 500),
      );
      expect(statusOf(text), anyOf(431, 0));
      expect(seen, isEmpty);
    });
    test('very long target gets 414, 431 or 400', () async {
      String text = await send('GET /${'a' * 100000} HTTP/1.1\r\n\r\n');
      expect(statusOf(text), anyOf(414, 431, 400));
      expect(seen, isEmpty);
    });
    test('many headers', () async {
      StringBuffer b = StringBuffer('GET / HTTP/1.1\r\n');
      for (int i = 0; i < 5000; i++) {
        b.write('H$i: v\r\n');
      }
      b.write('\r\n');
      String text = await send(b.toString());
      expect(statusOf(text), anyOf(431, 400, 0));
      expect(seen, isEmpty);
    });
    test('a body over maxBodySize with Content-Length gets 413', () async {
      String text = await send(
        'POST / HTTP/1.1\r\nContent-Length: 2000000\r\nExpect: 100-continue\r\n\r\n',
      );
      expect(statusOf(text), 413);
      expect(seen, isEmpty);
    });
    test(
      'a request that is cut in the middle of the head is never run',
      () async {
        await send('GET / HTTP/1.1\r\nX: a');
        expect(seen, isEmpty);
      },
    );
    test(
      'a request that is cut in the middle of the body is never run',
      () async {
        await send('POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\n012');
        expect(seen, isEmpty);
      },
    );
  });

  group('upgrade', () {
    test('Upgrade: irc/1.2 is a normal request (no protocol switch)', () async {
      String text = await send(
        'GET /irc HTTP/1.1\r\nUpgrade: irc/1.2\r\nConnection: Upgrade\r\n\r\n',
      );
      expect(statusOf(text), 200);
      expect(seen.single.uri, '/irc');
      expect(seen.single.headers['upgrade'], <String>['irc/1.2']);
    });
    test(
      'bytes after an upgrade request are protocol data (unparsedLength 10)',
      () {},
      skip: 'dox_http_native: there is no detachSocket; bytes after a request that is no WebSocket upgrade are parsed as the next request',
    );
  });
}
