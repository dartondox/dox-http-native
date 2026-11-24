import 'dart:ffi';

/// FFI calls into the Rust library. The build hook (hook/build.dart) ships
/// the library as the code asset of this file.

/// The ABI version the Dart side expects. Must match `ABI_VERSION` in Rust.
const int abiVersion = 1;

@Native<Int32 Function()>(symbol: 'dox_client_version', isLeaf: true)
external int doxClientVersion();

@Native<Int32 Function(Pointer<Void>)>(symbol: 'dox_client_init')
external int doxClientInit(Pointer<Void> dlData);

@Native<Pointer<Uint8> Function(Int64)>(
  symbol: 'dox_client_alloc',
  isLeaf: true,
)
external Pointer<Uint8> doxClientAlloc(int size);

@Native<Void Function(Pointer<Uint8>, Int64)>(
  symbol: 'dox_client_free',
  isLeaf: true,
)
external void doxClientFree(Pointer<Uint8> ptr, int size);

/// Makes an HTTP client (see native/src/client.rs). Its idle helper posts
/// to [sendPort]. [maxPerHost] 0 is no limit of connections to one address.
/// [maxBody] 0 is no limit of an answer body. A connection with no request
/// for [idleMs] closes (0: never). [ca] ([caLength] bytes, PEM) has more
/// root certificates to trust for `https://`. Returns a handle for
/// [doxClientRun], or -3 when [ca] is not correct.
@Native<Int64 Function(Int64, Int64, Int64, Int64, Pointer<Uint8>, Int64)>(
  symbol: 'dox_client_new',
)
external int doxClientNew(
  int sendPort,
  int maxPerHost,
  int maxBody,
  int idleMs,
  Pointer<Uint8> ca,
  int caLength,
);

/// Gives buffer [which] of a client (0: the requests for [doxClientRun],
/// 1: its answers) a size of [size] bytes and returns it. A buffer that
/// grows keeps its bytes. Rust frees the buffers with the client.
@Native<Pointer<Uint8> Function(Int64, Int32, Int64)>(
  symbol: 'dox_client_buffer',
)
external Pointer<Uint8> doxClientBuffer(int handle, int which, int size);

/// Sends the request records in [out], reads the finished answers into
/// [inBuf]. Never waits. Returns the answer bytes, 0 for none (Rust then
/// posts to the port when there are some), or -size when one answer needs
/// a bigger [inBuf].
@Native<Int64 Function(Int64, Pointer<Uint8>, Int64, Pointer<Uint8>, Int64)>(
  symbol: 'dox_client_run',
)
external int doxClientRun(
  int handle,
  Pointer<Uint8> out,
  int outLength,
  Pointer<Uint8> inBuf,
  int inCapacity,
);

/// Closes a client and frees its handle. Also the finalizer of a client.
@Native<Void Function(Pointer<Void>)>(symbol: 'dox_client_close')
external void doxClientClose(Pointer<Void> handle);
