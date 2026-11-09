// Ships the native library of dox_http_native as a code asset.
//
// Where the library comes from, first match wins:
// 1. The `lib_path` user define: a path to a ready library (for dev).
//    In the root pubspec.yaml of the app:
//      hooks:
//        user_defines:
//          dox_http_native:
//            lib_path: /path/to/libdox_http_native.so
// 2. A local build in `native/target/release` (for dev, same OS and CPU).
// 3. The ready-built library from the GitHub release, checked with sha256.
// 4. `cargo build` from source, when cargo is installed.
// Outside the pub cache (this repo, or a path or git dependency), 4 comes
// before 3, because the source may be newer than the release.
// When none works, no asset is added. Dox then stops at start and says
// why. The build itself never fails because of this package.
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';
import 'package:hooks/hooks.dart';

const String _releases =
    'https://github.com/dartondox/dox-http-native/releases/download';

const int _attempts = 3;

/// Seconds to wait before the 2nd and the 3rd try.
const List<int> _waitSeconds = <int>[1, 3];

/// sha256 of each release asset. The release workflow
/// (.github/workflows/ci.yaml) fills it from `SHA256SUMS` of the release:
/// `dart run tool/native_hashes.dart v<version>`
const Map<String, String> _sha256 = <String, String>{
  'dox_http_native-linux-arm64.so':
      'e0b13f3f91254888bc69176b57a18edeff59d9827600bba7e5c364091a5417ab',
  'dox_http_native-linux-x64.so':
      'd932286ced2abf1d3ac038f99b159b135474396ba262490107ccb41caed8753a',
  'dox_http_native-macos-arm64.dylib':
      '43f74ed64c0ebe05d32988da87c41dd1491daaf08c06623fb253b9c85adabad2',
  'dox_http_native-macos-x64.dylib':
      '69af91818795a83bb5d9c979e8d153ce857928e3b0926ed059535951f6720604',
  'dox_http_native-windows-x64.dll':
      '05dfe436188854f3a88d0b19d5b04e46220238504201b8923577e812589bcfac',
};

void main(List<String> args) async {
  await build(args, (BuildInput input, BuildOutputBuilder output) async {
    if (!input.config.buildCodeAssets) return;
    CodeConfig code = input.config.code;
    OS os = code.targetOS;
    Architecture arch = code.targetArchitecture;
    String fileName = os.dylibFileName('dox_http_native');
    bool host = os == OS.current && arch == Architecture.current;
    Uri native = input.packageRoot.resolve('native/');
    // From pub.dev the source matches the release, so download first. In the
    // repo, or as a path or git dependency, the source may be newer than the
    // release (eg. a new ABI), so build it with cargo first.
    bool published = input.packageRoot.pathSegments.contains('hosted');

    Uri? lib =
        _given(input) ??
        (host ? _local(native, fileName) : null) ??
        (published ? await _download(input, os, arch, fileName) : null) ??
        (host ? await _cargo(input, native, fileName, output) : null) ??
        (published ? null : await _download(input, os, arch, fileName));

    if (lib == null) {
      stderr.writeln(
        'dox_http_native: no native library for ${os.name}-${arch.name}. '
        'Install Rust (cargo) to build it from source.',
      );
      return;
    }
    output.dependencies.add(lib);
    output.assets.code.add(
      CodeAsset(
        package: input.packageName,
        name: 'src/bindings.dart',
        linkMode: DynamicLoadingBundled(),
        file: lib,
      ),
    );
  });
}

Uri? _given(BuildInput input) {
  String? path = input.userDefines['lib_path'] as String?;
  if (path == null || path.isEmpty) return null;
  File file = File(path);
  if (!file.existsSync()) {
    throw StateError('dox_http_native: $path does not exist');
  }
  return file.absolute.uri;
}

Uri? _local(Uri native, String fileName) {
  Uri lib = native.resolve('target/release/$fileName');
  return File.fromUri(lib).existsSync() ? lib : null;
}

Future<Uri?> _download(
  BuildInput input,
  OS os,
  Architecture arch,
  String fileName,
) async {
  String ext = fileName.substring(fileName.lastIndexOf('.'));
  String asset = 'dox_http_native-${os.name}-${arch.name}$ext';
  String? hash = _sha256[asset];
  if (hash == null) return null; // no release for this platform (yet)

  // the release tag has the package version, eg. dox_http_native-v3.0.0
  String pubspec = File.fromUri(input.packageRoot.resolve('pubspec.yaml'))
      .readAsStringSync();
  String version = RegExp(
    r'^version:\s*(\S+)',
    multiLine: true,
  ).firstMatch(pubspec)![1]!;
  File file = File.fromUri(input.outputDirectoryShared.resolve(asset));
  if (file.existsSync() && _sha(file.readAsBytesSync()) == hash) {
    return file.uri;
  }
  // GitHub sometimes answers 503 or 504. Try up to 3 times, with a short wait.
  for (int attempt = 1; attempt <= _attempts; attempt++) {
    if (attempt > 1) {
      await Future<void>.delayed(Duration(seconds: _waitSeconds[attempt - 2]));
    }
    String tried = '(attempt $attempt of $_attempts)';
    // a stalled download must not hang the build forever. Each try has one
    // time limit for the whole thing: connect, response head and body.
    DateTime deadline = DateTime.now().add(_tryLimit);
    HttpClient client = HttpClient()
      ..connectionTimeout = const Duration(seconds: 30);
    try {
      HttpClientRequest req = await client
          .getUrl(Uri.parse('$_releases/v$version/$asset'))
          .timeout(_left(deadline));
      HttpClientResponse res = await req.close().timeout(_left(deadline));
      if (res.statusCode != 200) {
        stderr.writeln(
          'dox_http_native: download $asset: ${res.statusCode} $tried',
        );
        if (shouldRetryStatus(res.statusCode)) continue;
        return null;
      }
      List<int> bytes = <int>[
        for (List<int> chunk in await res.toList().timeout(_left(deadline)))
          ...chunk,
      ];
      if (_sha(bytes) != hash) {
        // the body may be an error page, so try again
        stderr.writeln(
          'dox_http_native: sha256 of $asset does not match $tried',
        );
        continue;
      }
      file.parent.createSync(recursive: true);
      file.writeAsBytesSync(bytes);
      return file.uri;
    } on Exception catch (error) {
      stderr.writeln('dox_http_native: download $asset failed: $error $tried');
    } finally {
      client.close(force: true);
    }
  }
  return null;
}

/// The longest time one download try may take, from connect to the last byte.
const Duration _tryLimit = Duration(seconds: 60);

/// Time left until [deadline]. Zero or less makes the next wait time out now.
Duration _left(DateTime deadline) => deadline.difference(DateTime.now());

/// True when a new try may help: a server error (5xx) or "too many requests".
/// A 404 means the asset does not exist, so it is not tried again.
bool shouldRetryStatus(int status) => status == 429 || status >= 500;

String _sha(List<int> bytes) => sha256.convert(bytes).toString();

Future<Uri?> _cargo(
  BuildInput input,
  Uri native,
  String fileName,
  BuildOutputBuilder output,
) async {
  String home =
      Platform.environment['HOME'] ?? Platform.environment['USERPROFILE'] ?? '';
  String cargo = 'cargo';
  File inHome = File('$home/.cargo/bin/cargo');
  if (home.isNotEmpty && inHome.existsSync()) cargo = inHome.path;

  Uri target = input.outputDirectoryShared.resolve('cargo/');
  try {
    ProcessResult result = await Process.run(cargo, <String>[
      'build',
      '--release',
      '--manifest-path',
      File.fromUri(native.resolve('Cargo.toml')).path,
      '--target-dir',
      Directory.fromUri(target).path,
    ]);
    if (result.exitCode != 0) {
      stderr.writeln('dox_http_native: cargo build failed\n${result.stderr}');
      return null;
    }
  } on ProcessException {
    return null; // no cargo
  }
  // every Rust file, so a change in any of them builds the library again
  output.dependencies.add(native.resolve('Cargo.toml'));
  output.dependencies.add(native.resolve('Cargo.lock'));
  for (FileSystemEntity file in Directory.fromUri(
    native.resolve('src/'),
  ).listSync(recursive: true)) {
    if (file is File && file.path.endsWith('.rs')) {
      output.dependencies.add(file.uri);
    }
  }
  Uri lib = target.resolve('release/$fileName');
  return File.fromUri(lib).existsSync() ? lib : null;
}
