// A small HTTP/2 server for h2spec in CI (see .github/workflows/ci.yaml).
// Run: dart run tool/h2spec_server.dart <port>
import 'dart:io';

import 'package:dox_http_native/dox_http_native.dart';

Future<void> main(List<String> args) async {
  NativeHttpServer server = await NativeHttpServer.bind(
    int.parse(args.first),
    http2: true,
  );
  server.listen((HttpRequest req) async {
    await req.drain<void>();
    HttpResponse res = req.response;
    if (req.uri.path == '/stream') {
      // a streamed answer in parts: h2spec 6.9.2/2 needs DATA under a
      // small window
      for (int i = 0; i < 4; i++) {
        res.add(List<int>.filled(1024, 0x61));
        await res.flush();
      }
    } else {
      res.write('ok');
    }
    await res.close();
  });
  stdout.writeln('listening on ${server.port}');
}
