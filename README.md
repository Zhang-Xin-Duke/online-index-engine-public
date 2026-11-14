# Online Index Engine

A C++17 in-process key-value lookup module with partitioned snapshots and online refresh. Applications load a snapshot, look up records by ID, and replace partitions while readers continue using protected records.

The engine provides four table backends, a chunk allocator, epoch-based reclamation, and sequential N+1 refresh. It does not fetch missing records from a remote database or implement a network server.

## Demo on NCShare

[![NCShare terminal demo](assets/demo/ncshare-demo.gif)](assets/demo/ncshare-demo.mp4?raw=1)

[Download the full-resolution MP4](assets/demo/ncshare-demo.mp4?raw=1).

A scripted terminal session captured in a real NCShare Slurm allocation using 4 CPU cores and no GPU. The executable was compiled on the cluster before recording. The clip shows snapshot construction, a lookup hit, a lookup miss, and the before/after results of N+1 refresh with the `neighbor-pool` backend and 8 partitions.

The input consists of real [MovieLens-1M](https://grouplens.org/datasets/movielens/1m/) rating events: 800,168 records in the old snapshot and 1,000,209 in the new snapshot. Keys are chronological event IDs; original movie and rating fields are retained in the values. The newly added key `800168` is absent before refresh and available afterward.

## Build

Requires a C++17 compiler, CMake 3.20 or later, and a POSIX environment. Linux and macOS are supported. Public dependencies are downloaded during configuration: Abseil, Protobuf, and FlatBuffers. CMake 3.31.6 is used by the Linux build workflow.

```bash
git clone https://github.com/Zhang-Xin-Duke/online-index-engine-public.git
cd online-index-engine-public
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 2
```

To use installed Protobuf and Abseil packages, configure with `-DENGINE_BUNDLED_DEPS=OFF`. FlatBuffers remains a pinned build dependency. `-DENGINE_SANITIZE=ON` enables address and undefined-behavior instrumentation on compilers that support it.

## Input and commands

Input is a UTF-8 TSV file without a header. Each row contains three fields:

```text
id<TAB>generation<TAB>value
```

`id` and `generation` are unsigned 64-bit decimal integers. IDs must be unique in a snapshot. The value may be empty, but cannot contain literal tabs or newlines. Additional columns, if present, are ignored. An empty file represents an empty snapshot.

```bash
build/online_index build records.tsv snapshot-v1 8
build/online_index query neighbor snapshot-v1 1001
build/online_index refresh neighbor snapshot-v1 snapshot-v2 1001
```

- `build INPUT_TSV OUTPUT_DIR SHARDS` creates a snapshot. The output directory must not already exist. `SHARDS` must be a power of two from 1 through 1024.
- `query BACKEND SNAPSHOT_DIR KEY` loads a snapshot and prints the matching record as JSON. A miss prints `"found":false` and `"status":"not_found"`.
- `refresh BACKEND OLD_DIR NEW_DIR KEY` loads the old snapshot, prints the selected record with `"phase":"before"`, refreshes the table, and prints the record with `"phase":"after"`. Both snapshots must have the same partition count. This command runs in one process and does not change the source snapshot directories or a running external service.

Successful operations, including lookup misses, exit with status 0. Invalid command syntax exits with status 2; invalid input, unsupported backends, and load/refresh failures exit with status 1 and an error on stderr. Each JSON result is written on its own stdout line. `--help` lists the commands.

## Backends

| Name | Index layout | Record storage |
|---|---|---|
| `bucket` | Bucket directory and contiguous key/pointer nodes | Individually allocated records |
| `neighbor` | Compact buckets with relative collision links | Individually allocated records |
| `bucket-pool` | Bucket directory and contiguous key/pointer nodes | Chunk-allocated versioned records |
| `neighbor-pool` | Compact buckets with relative collision links | Chunk-allocated versioned records |

Compact nodes store a 64-bit key, a signed 16-bit neighbor offset, and a 48-bit pointer. They require addresses representable in that pointer field; out-of-range pointers are rejected. Pool variants retain version metadata as well as changing allocation strategy.

## Data flow and lifecycle

```text
TSV records -> partitioned index/data files -> loaded tables -> key lookup
                                                |
new snapshot -> load one partition -> publish -> retire old partition
                                                |
                                  wait for readers -> reclaim -> next partition
```

The builder hashes an ID, selects a partition, and emits bucket-ordered key/offset nodes alongside serialized records. Loading constructs runtime `Record` objects, including owned strings. Querying uses the hash to locate a partition and its table entry.

`src/engine.h` contains the record format, snapshot builder, metadata loader, and four backend aliases in the `index_engine` namespace. `core/index_engine` contains the table, allocator, and refresh implementation. `compat` supplies the runtime adapters. `core/proto` defines the snapshot metadata and table header formats.

For embedded use, `ShardedReloadable<T>` provides `Load`, `GetShard`, `ResetDir`, and `Reload`. Acquire an epoch handle **before** retrieving a partition pointer, and release it **after** the last access to the returned record. Do not retain record pointers beyond that guard. The CLI demonstrates scoped guards and ensures its controller outlives the table.

## Refresh contract

Initial loading can construct partitions concurrently. Refresh has one writer: it loads a new partition, publishes its pointer, retires the previous partition, waits for protected readers to finish, and then advances to the next partition.

For similarly sized partitions and stable data volume, the live-table overhead is approximately one additional partition. Temporary construction buffers, strings, and allocator retention also consume memory. A long-lived reader can delay reclamation and stall refresh progress.

Readers are protected from premature reclamation. A request spanning partitions can observe different snapshot generations; an epoch handle does not establish a cross-partition transaction snapshot. A failure after some partitions were replaced can leave a mixture of old and new partitions, without automatic whole-refresh rollback.

Snapshot files must come from a trusted producer. Metadata and file-size checks are provided; complete validation of arbitrary untrusted binary files is outside this module. Streaming ingestion, distributed replica coordination, and service-level rollback belong to the integrating application.
