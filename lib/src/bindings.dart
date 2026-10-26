import 'dart:ffi';

/// FFI calls into the Rust library. The build hook (hook/build.dart) ships
/// the library as the code asset of this file.

/// The ABI version the Dart side expects. Must match `ABI_VERSION` in Rust.
const int abiVersion = 8;

@Native<Int32 Function()>(symbol: 'dox_version', isLeaf: true)
external int doxVersion();

@Native<Int32 Function(Pointer<Void>)>(symbol: 'dox_init')
external int doxInit(Pointer<Void> dlData);

/// Binds [port] and starts the idle helper, which posts to [sendPort].
/// [family] 0 is all interfaces, 4 is IPv4 (in [addrLo]), 6 is IPv6.
/// Returns a handle for [doxRun].
@Native<
  Int64 Function(Int32, Int64, Int64, Int64, Int64, Int32, Uint64, Uint64)
>(symbol: 'dox_bind')
external int doxBind(
  int port,
  int sendPort,
  int maxBody,
  int maxMultipart,
  int bodyTimeoutMs,
  int family,
  int addrHi,
  int addrLo,
);

@Native<Int32 Function(Int64)>(symbol: 'dox_bound_port', isLeaf: true)
external int doxBoundPort(int handle);

/// Writes the answers in [out], reads new requests into [inBuf]. Waits up
/// to [budgetUs] for them. Returns the request bytes, 0 for none (Rust
/// then posts to the port when there are some), or -size when one request
/// needs a bigger [inBuf]. Not a leaf call: it may wait on the sockets.
@Native<
  Int64 Function(Int64, Pointer<Uint8>, Int64, Pointer<Uint8>, Int64, Int64)
>(symbol: 'dox_run')
external int doxRun(
  int handle,
  Pointer<Uint8> out,
  int outLength,
  Pointer<Uint8> inBuf,
  int inCapacity,
  int budgetUs,
);

/// Stop accepting. Open connections close after their current answer,
/// WebSockets get 1001. Rust posts `[2]` to the port when all are closed.
@Native<Void Function(Int64)>(symbol: 'dox_shutdown')
external void doxShutdown(int handle);

@Native<Void Function(Int64)>(symbol: 'dox_unbind')
external void doxUnbind(int handle);

@Native<Pointer<Uint8> Function(Int64)>(symbol: 'dox_alloc', isLeaf: true)
external Pointer<Uint8> doxAlloc(int size);

@Native<Void Function(Pointer<Uint8>, Int64)>(symbol: 'dox_free', isLeaf: true)
external void doxFree(Pointer<Uint8> ptr, int size);

/// Frees an external body (the finalizer of its typed list).
@Native<Void Function(Pointer<Void>)>(symbol: 'dox_free_body', isLeaf: true)
external void doxFreeBody(Pointer<Void> token);
