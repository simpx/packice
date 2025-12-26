# Fruina Design

## Definition
Fruina is a flexible, batteries-included peer-to-peer cache system. Its flexible architecture separates the core logic (Peer, Lease, Object) from the implementation details (Backends, Transport), making it convenient to define new modules and functionalities. Being batteries-included, it provides ready-to-use implementations like in-memory Fruina and Redis versions out of the box.

## Architecture Overview

The system is divided into six distinct layers:

1.  **Core Layer**: The logical heart. Defines the abstract `Peer` interface and resource types (Object, Lease, Blob).
2.  **Backends Layer**: Concrete implementations for storage (Blob) and metadata (Lease).
3.  **Peers Layer**: Concrete `Peer` implementations and compositions (e.g., `MemoryPeer`, `TieredPeer`).
4.  **P2P Layer**: Distributed capabilities (Tracker, Gossip, P2P Transport).
5.  **Transport Layer**: Adapts a `Peer` to network protocols.
6.  **Interface Layer**: Groups user-facing components (CLI, Client, Integrations).

---

## 1. Core Layer

Located in `fruina/core/`.

### Blob (`core/blob.py`)
Abstracts a contiguous chunk of data. Represents a self-contained data source.
- **Self-Contained**: A Blob encapsulates all necessary information to access its data, whether it's a local memory address, a file path, or a remote network location (IP + necessary information).
- **Interface**: `read()`, `write()`, `seal()`, `memoryview()`.
- **Polymorphism**: Different implementations support different access patterns (e.g., `MemBlob` for local RAM, `RemoteBlob` for network fetch).

### Object (`core/object.py`)
The unit of management.
- Contains a list of `Blob`s and metadata.
- Manages state: `CREATING` -> `SEALED`.
- **Lifecycle**: Manages the lifecycle of its underlying `Blob`s.

### Lease (`core/lease.py`)
Represents the right to access an Object.
- **Attributes**: `lease_id`, `object_id`, `access_flags` (READ/CREATE/WRITE), `ttl`.
- **TTL**: Some leases have a TTL (Time To Live), while others do not and require explicit release.

### Peer (`core/peer.py`)
The central coordinator.
- **Role**: Manages the lifecycle of Objects and Leases.
- **API**: `acquire()`, `seal()`, `discard()`, `release()`.
- **Return Values**: Returns `(Lease, Object)` tuples. The `Object` contains `Blob`s, and different Blob types provide different access methods.

---

## 2. Backends Layer

Located in `fruina/backends/`.

### File System (`backends/fs.py`)
- **FileBlob**: Stores blob in a local file system.

### Memory (`backends/memory.py`)
- **MemBlob**: Stores data in memory (using `memfd_create` on Linux or `tempfile` on others).
- **MemoryLease**: Stores lease state in Python memory.

### Redis (`backends/redis.py`)
- **RedisLease**: Stores lease state in Redis.

---

## 3. Peers Layer

Located in `fruina/peers/`.

**Role**: Provides concrete implementations of the `Peer` interface. This is where "business logic" and "composition" happen.

### Standard Peers
- **MemoryPeer**: A Peer that stores everything in memory (MemBlob + MemoryLease).
- **FileSystemPeer**: A Peer that stores data on disk (FileBlob + MemoryLease).

### Composite Peers
- **TieredPeer**: A Peer that manages a "Hot" Peer and a "Cold" Peer, implementing LRU eviction and data movement between them.

---

## 4. P2P Layer (Planned)

Located in `fruina/p2p/`.

**Role**: Manages distributed capabilities, transforming single nodes into a loose P2P network.

- **Tracker**: Lightweight metadata service for object discovery.
- **Gossip**: Node discovery and state propagation.
- **P2P Transport**: Specialized transport for efficient peer-to-peer data transfer.

---

## 5. Transport Layer

Located in `fruina/transport/`.

**Role**: Adapts the Core Peer to specific network protocols. Exposes Peer capabilities through different interfaces.
**Key Design Principle**: Transports are **Adapters**, not Consumers. They use the `Peer` API directly to get handles and pass them to the client. They do **not** use the SDK Client.

### HTTP Transport (`transport/http.py`)
- **Protocol**: JSON over HTTP.
- **Mechanism**: Returns response in JSON.
- **Use Case**: Networked nodes, shared storage (NFS/Volume).
- **Components**: `HttpServer`, `HttpTransport`.

### UDS Transport (`transport/uds.py`)
- **Protocol**: JSON over Unix Domain Sockets.
- **Mechanism**: Uses `SCM_RIGHTS` to pass File Descriptors (FDs) between processes.
- **Use Case**: Local high-performance IPC, container sidecars.
- **Components**: `UdsServer`, `UdsTransport`.

### Direct Transport (`transport/direct.py`)
- **Protocol**: Direct Python function calls.
- **Mechanism**: Wraps a `Peer` instance directly.
- **Use Case**: In-process usage, testing.
- **Components**: `DirectTransport`.

---

## 6. Interface Layer

Located in `fruina/interface/`.

This layer groups all user-facing components, including the CLI, Client SDK, and Integrations.

### CLI (`interface/cli.py`)
- **Role**: The command-line interface for starting the server.
- **Responsibility**: Instantiates the appropriate `Peer` (e.g., `MemoryPeer` or `FileSystemPeer`) and wraps it with a Transport (HTTP or UDS).
- **Usage**: `python -m fruina.interface.cli --impl fs --transport http`

### Client (`interface/client.py`)
- **Unified Entry Point**: `fruina.connect(target)`.
- **Auto-Detection**:
    - `connect()`: Creates a private, isolated in-memory Peer.
    - `connect("memory://name")`: Connects to a shared in-process Peer (DuckDB style).
    - `connect("http://...")`: Connects to a remote HTTP Peer.
    - `connect("/tmp/...")`: Connects to a local UDS Peer.
- **Direct Access**: Can wrap a `Peer` instance directly (`DirectTransport`) for zero-overhead in-process usage.

### Integrations (`interface/integrations/`)
- **Role**: Bridges the gap between user applications and Fruina.
- **Examples**: PyTorch Dataset loaders, vLLM model loaders.