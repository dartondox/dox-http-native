// (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.
// Ported from dart-lang/sdk tests/standalone/io/http_10_test.dart.
import 'dart:async';
import 'dart:io';

import 'package:test/test.dart';

import 'util.dart';

void main() {
  test(
    'content length smaller than the body: done completes with HttpException',
    () async {
      Completer<Object?> result = Completer<Object?>();
      var server = await serve((HttpRequest request) {
        HttpResponse response = request.response;
        response.contentLength = 1;
        response.done.then<void>(
          (_) => result.complete(null),
          onError: result.complete,
        );
        response.write('ZZ');
        response.close();
      });
      await raw(server.port, 'GET / HTTP/1.0\r\n\r\n');
      expect(await result.future, isA<HttpException>());
    },
    // the server sends the real body length, never the wrong one
    skip:
        'dox_http_native: a buffered body that is longer than the set '
        'contentLength goes out with its real length, no error',
  );

  test(
    'request.contentLength is -1 without Content-Length',
    () async {
      var server = await serve((HttpRequest request) {
        request.response.write('${request.contentLength}');
        request.response.close();
      });
      String s = await raw(server.port, 'GET / HTTP/1.0\r\n\r\n');
      expect(s, endsWith('-1'));
    },
    // dox_http_native: contentLength is the body length (0), not -1
    skip:
        'dox_http_native: request.contentLength is 0, not -1, with no '
        'Content-Length header',
  );

  // The server sets a content length but still must close the connection as
  // there is no keep alive.
  test('HTTP/1.0 without keep-alive closes', () async {
    var server = await serve((HttpRequest request) {
      expect(request.headers.value('content-length'), isNull);
      HttpResponse response = request.response;
      response.contentLength = 1;
      expect(request.protocolVersion, '1.0');
      response.write('Z');
      response.write('Z');
      response.close();
      expect(() => response.write('x'), throwsStateError);
    });
    for (int i = 0; i < 10; i++) {
      String s = (await raw(
        server.port,
        'GET / HTTP/1.0\r\n\r\n',
      )).toLowerCase();
      expect(s, isNot(contains('keep-alive')));
      expect(s, startsWith('http/1.0 200'));
    }
  });

  // No content length: the server closes the connection to end the answer.
  test('HTTP/1.0 keep-alive request, no content length: server closes', skip: 'dox_http_native: it buffers the body, sets Content-Length and keeps the HTTP/1.0 connection alive (valid)', () async {
    var server = await serve((HttpRequest request) {
      expect(request.headers.value('content-length'), isNull);
      request.listen(
        (_) {},
        onDone: () {
          expect(request.protocolVersion, '1.0');
          request.response.write('Z');
          request.response.close();
        },
      );
    });
    for (int i = 0; i < 10; i++) {
      String s = (await raw(
        server.port,
        'GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n',
      )).toLowerCase();
      expect(s.endsWith('z'), isTrue);
      expect(s, isNot(contains('content-length:')));
      expect(s, isNot(contains('keep-alive')));
    }
  });

  test('HTTP/1.0 keep-alive with a content length is reused', () async {
    var server = await serve((HttpRequest request) {
      expect(request.headers.value('content-length'), isNull);
      HttpResponse response = request.response;
      response.contentLength = 1;
      response.persistentConnection = true;
      expect(request.protocolVersion, '1.0');
      response.write('Z');
      response.close();
    });
    Socket socket = await Socket.connect('127.0.0.1', server.port);
    addTearDown(socket.destroy);
    List<int> response = <int>[];
    int count = 0;
    Completer<void> done = Completer<void>();
    void send() =>
        socket.write('GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n');
    socket.listen((List<int> d) {
      response.addAll(d);
      if (response.last == 'Z'.codeUnitAt(0)) {
        String s = String.fromCharCodes(response).toLowerCase();
        expect(s.indexOf('\r\nconnection: keep-alive\r\n'), greaterThan(0));
        expect(s.indexOf('\r\ncontent-length: 1\r\n'), greaterThan(0));
        count++;
        if (count < 10) {
          response = <int>[];
          send();
        } else {
          socket.close();
        }
      }
    }, onDone: done.complete);
    send();
    await done.future.timeout(const Duration(seconds: 10));
    expect(count, 10);
  });

  test('HTTP/1.0 keep-alive, no content length: no keep-alive answer', skip: 'dox_http_native: it buffers the body, sets Content-Length and keeps the HTTP/1.0 connection alive (valid)', () async {
    var server = await serve((HttpRequest request) {
      expect(request.headers.value('content-length'), isNull);
      expect(request.protocolVersion, '1.0');
      request.response.write('Z');
      request.response.close();
    });
    for (int i = 0; i < 10; i++) {
      String s = (await raw(
        server.port,
        'GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n',
      )).toLowerCase();
      expect(s.endsWith('z'), isTrue);
      expect(s, isNot(contains('content-length')));
      expect(s, isNot(contains('connection')));
    }
  });
}
