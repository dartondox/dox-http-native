import 'dart:math';

import 'package:dox_http_native/src/request.dart';
import 'package:test/test.dart';

/// what `NativeRequest.uri` gives for a target (same code path)
String _uriPath(String target) {
  bool plain =
      target.startsWith('/') &&
      !target.startsWith('//') &&
      !target.contains('?') &&
      !target.contains('#');
  return (plain ? Uri(path: target) : Uri.parse(target)).path;
}

void main() {
  group('NativeRequest.plainPath', () {
    test('plain targets give the text before ?', () {
      expect(NativeRequest.plainPath('/'), '/');
      expect(NativeRequest.plainPath('/pipeline'), '/pipeline');
      expect(NativeRequest.plainPath('/a/b.json?x=1&y=2'), '/a/b.json');
      expect(NativeRequest.plainPath('/a?'), '/a');
      expect(
        NativeRequest.plainPath("/a-._~!\$&'()*+,;=:@b"),
        "/a-._~!\$&'()*+,;=:@b",
      );
      expect(NativeRequest.plainPath('/a/?b=/../c'), '/a/');
    });

    test('other targets need the full parse', () {
      for (String t in <String>[
        '',
        'a',
        '*',
        '//a',
        '/a//b',
        '/.',
        '/..',
        '/a/../b',
        '/a/./b',
        '/.well-known/x',
        '/a%2Fb',
        '/a%41',
        '/a b',
        '/a"b',
        '/a<b>',
        '/a#b',
        '/a?b#c',
        '/é',
        '/a\\b',
        '/a{b}',
        '/a|b',
        '/a^b',
        '/a`b',
        'http://h/a',
      ]) {
        expect(NativeRequest.plainPath(t), isNull, reason: t);
      }
    });

    test('a plain path is always the same as uri.path', () {
      // characters that matter for the parse, a lot of them
      const String chars = "/ab.%?#-_~!\$&'()*+,;=:@ \"<>\\^`{|}é09AZ";
      Random r = Random(7);
      int plain = 0;
      for (int i = 0; i < 200000; i++) {
        int len = 1 + r.nextInt(12);
        StringBuffer b = StringBuffer('/');
        for (int k = 0; k < len; k++) {
          b.write(chars[r.nextInt(chars.length)]);
        }
        String t = b.toString();
        String? p = NativeRequest.plainPath(t);
        if (p == null) continue;
        plain++;
        String want;
        try {
          want = _uriPath(t);
        } on FormatException {
          fail('plainPath took $t, but Uri refuses it');
        }
        expect(p, want, reason: t);
      }
      expect(plain, greaterThan(1000));
    });
  });
}
