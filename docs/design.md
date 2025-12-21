# PackIce Design & Implementation

## Objective
Refine the PackIce architecture to achieve better abstraction and decoupling. The goal is to separate the core logic (Peer, Lease, Object) from the implementation details (Backends, Transport), enabling flexible composition of nodes (e.g., HTTP+FS, UDS+Memfd) and easier future extensions (e.g., Redis-based Lease, S3-based Blob).

## Architecture Overview

The system is divided into four distinct layers:

1.  **Core Layer**: The logical heart. Manages resources, state, and lifecycle.
2.  **Backends Layer**: Concrete implementations for storage (Blob) and metadata (Lease).
3.  **Transport Layer**: The "Mover". Adapts the Core to network protocols.
4.  **Interface Layer**: The "Consumer". Provides user-facing SDK and CLI.

---

## 1. Core Layer (The Logic)

Located in `packice/core/`.

### Peer (`core/peer.py`)
The central coordinator.
- **Role**: Manages the lifecycle of Objects and Leases.
- **API**: `acquire()`, `seal()`, `release()`.
- **Return Values**: Returns `(Lease, Object)` tuples. The `Object` contains raw `Blob`s, which expose low-level **Handles** (e.g., file paths or FDs).
- **Dependency Injection**: Accepts `BlobFactory` and `LeaseFactory` at initialization.

### Object (`core/object.py`)
The unit of management.
- Contains a list of `Blob`s and metadata.
- Manages state: `CREATING` -> `SEALED`.
- **Decoupling**: Holds Blobs but knows nothing about Leases.

### Lease (`core/lease.py`)
Represents the right to access an Object.
- **Decoupling**: Holds `object_id` (string) instead of a direct reference to `Object`.
- **Attributes**: `lease_id`, `object_id`, `access` (READ/CREATE), `ttl`.

### Blob (`core/blob.py`)
Abstracts a contiguous chunk of data.
- **Interface**: `read()`, `write()`, `seal()`, `get_handle()`.
- **Handle**: An opaque identifier (str path or int FD) used by the Transport layer.

---

## 2. Backends Layer (The Implementations)

Located in `packice/backends/`.

### File System (`backends/fs.py`)
- **FileBlob**: Stores data in a local file system. Handle is the file path.

### Memory (`backends/memory.py`)
- **MemBlob**: Stores data in memory (using `memfd_create` on Linux or `tempfile` on others). Handle is the file descriptor (FD).
- **MemoryLease**: Stores lease state in Python memory. Generates UUIDs internally.

---

## 3. Transport Layer (The Mover)

Located in `packice/transport/`.

**Role**: Adapts the Core Peer to specific network protocols.
**Key Design Principle**: Transports are **Adapters**, not Consumers. They use the `Peer` API directly to get handles and pass them to the client. They do **not** use the SDK Client.

### HTTP Transport (`transport/http.py`)
- **Protocol**: JSON over HTTP.
- **Mechanism**: Returns file paths (handles) in JSON.
- **Use Case**: Networked nodes, shared storage (NFS/Volume).
- **Components**: `HttpServer`, `HttpTransportClient`.

### UDS Transport (`transport/uds.py`)
- **Protocol**: JSON over Unix Domain Sockets.
- **Mechanism**: Uses `SCM_RIGHTS` to pass File Descriptors (FDs) between processes.
- **Use Case**: Local high-performance IPC, container sidecars.
- **Components**: `UdsServer`, `UdsTransportClient`.

---

## 4. Interface Layer (The Consumer)

Located in the root `packice/` package.

### Node (`node.py`)
- **Role**: Encapsulates the logic of assembling a Peer with specific Backend and Transport components.
- **Responsibility**: Handles configuration, initialization, and lifecycle management (start/stop) of the server.

### Client (`client.py`)
- **Unified Entry Point**: `packice.connect(target)`.
- **Auto-Detection**:
    - `connect()`: Creates a private, isolated in-memory Peer.
    - `connect("memory://name")`: Connects to a shared in-process Peer (DuckDB style).
    - `connect("http://...")`: Connects to a remote HTTP Peer.
    - `connect("/tmp/...")`: Connects to a local UDS Peer.
- **Direct Access**: Can wrap a `Peer` instance directly (`DirectTransportClient`) for zero-overhead in-process usage.

### CLI (`cli.py`)
- **Role**: The command-line interface for starting the server.
- **Usage**: `python -m packice.cli --impl fs --transport http`

---

## Usage Patterns

### 1. In-Process (DuckDB Style)
Ideal for single-process applications or testing. No network overhead.

```python
import packice

# Private instance
client = packice.connect()

# Shared instance (between modules)
client_a = packice.connect("memory://shared")
client_b = packice.connect("memory://shared")
```

### 2. Multi-Process (Networked)
Ideal for multi-process applications or distributed systems. Requires a Server process.

**Server (Process A):**
```bash
# Start a UDS node with Memory storage
python3 -m packice.cli --impl mem --transport uds --socket /tmp/packice.sock
```

**Client (Process B):**
```python
import packice

# Connects to UDS socket, handles JSON and FD reception transparently
client = packice.connect("/tmp/packice.sock")

lease = client.acquire(intent="create")
with lease.open("wb") as f:
    f.write(b"data")
```
