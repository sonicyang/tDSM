# tDSM (Transparent Distributed Shared Memory)

A transparent distributed shared memory implementation based on userfaultfd of Linux.

WARNING: THIS SOFTWARE ONLY ROUGHLY TESTED!

## Requirements

 * CMake
 * GCC or clang supporting C++20
 * git

## Compiling

```bash
$ cmake -S . -B build
$ cmake --build build --target all
```

## Using

 * Write you program as usual.

 * Include the `include/swapper.hpp`
 * `tDSM::swapper.memory()` returns a shared memory region that is visible to all other programs.
 * `tDSM::swapper.size()` the size of the shared memory region.
 * `tDSM::swapper.get_sem(x)`, `tDSM::swapper.put_sem(x)` operate a semaphore `x`, which `x` is a arbitrary 64-bits value, initialized to 0.

 * Include `include/rpc_adapter.hpp`
 * `tDSM::rpc::simple_rpc_adapter<function> stub` makes an RPC of the function `function` named `stub`.
     * Current implementation only support trivially copyable parameters and return values.
 * Set `stub.remote` to the ID of the remote node for processing the RPC.
 * `ret = stub(...)` calls the RPC as calling the `function` remotely, return value returns.

 * Link you program using the bfd linker, adding `linker_script/rdma.ld` to the list of objects for linking.
     * Do not use flags `-T`, use pass it as typical object files.

 * Each node executes a instance of the compiled program.
     * The ID of each node is set automatically by the order of execution, starting from 1.
 * There must be one and only one directory node. The directory node can be started by setting environment variable `DIRECTORY=1`.
 * A node can be used to server RPCs only, the main function will not execute. This can be started by setting environment variable `RPC_SERVER=1`.
 * The IP address and port for communication must be set by environment variable `ADDRESS="IP ADDRESS"` and `PORT="PORT"`.
 * The IP address of the directory node must be set by environment variable `DIRECOTRY_ADDRESS="DIRECTORY IP ADDRESS"`. This can be omitted by the directory node.
 * LZ4 compression can be enabled by setting environment variable `COMPRESSION=1` on the directory node. Other nodes will automatically inherit the setting.

## Testing

Assume `/proc/sys/vm/unprivileged_userfaultfd` is set to `1`.
Otherwise, you need to run everything as root.

### Console 1
```
$ DIRECTORY=1 PORT=7000 ./build/bin/memtest
...
```

### Console 2
```
$ RPC_SERVER=1 PORT=7001 ./build/bin/memtest
...
```

### Console 3
```
$ RPC_SERVER=1 PORT=7002 ./build/bin/memtest
...
```

After you start the third `memtest`, the memtest in console 1 will proceed.
If no error happened, the memtest in console 1 will output `OK`.
For now, the `memset` in console 2 and 3 will stuck and you need to manually terminate them.

## Coherence protocol

TBD

## Limitations

 * Only one directory node can execute on a machine for now.

## LICENSE

See [COPYRIGHT](./COPYRIGHT)
