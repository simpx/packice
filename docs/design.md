# PackIce Design

## System Snapshot (8–10 bullets)
- PackIce is a minimal lease-based peer-to-peer object cache that relies on composition instead of new protocol surfaces.
- Nodes only expose three hard APIs: `Acquire`, `Seal`, and `Release`; all higher-level behavior is built from them.
- Objects are immutable once sealed; lifecycle is controlled locally by the node that owns the lease.
- Leases define both correctness guarantees and the lifetime of an object copy on a node.
- Object transfers are not a protocol call; bytes move by copying between data attachments held under active leases.
- A soft-state resolver (e.g., Redis) only suggests candidate nodes; successful `Acquire` still depends on the target node’s state.
- Directory/resolver entries may be stale; correctness is enforced by lease rules rather than by directory authority.
- Objects may form chains using `prev_objid`, but each link’s lifecycle is independent per node.
- Sealing publishes an object within a node; releasing relinquishes local custody and may trigger eviction.

## Assumptions
- Resolver only returns node candidates and is not authoritative; failure to acquire from a suggested node is expected.
- Lease durations are finite and renewed explicitly through re-acquisition rather than implicit background refresh.
- Data attachments are opaque byte stores associated with leases and are writable only while the lease is unsealed and active.

## Core Concepts
- **Object**: Identified by `objid`; includes optional metadata and may reference `prev_objid` to form a chain. Mutable only while in CREATING state under an unsealed lease.
- **Lease**: Time-bounded, node-local right to create or access an object copy. Encapsulates attachment handle, lease ID, flags, and optional metadata returned by `Acquire`.
- **Node**: Hosts object copies and enforces lease semantics. Only exposes `Acquire`, `Seal`, and `Release`; orchestrates attachment lifecycle.
- **Directory / Resolver**: Soft-state mapping from `objid` to candidate nodes. Provides hints for fetch-on-miss; correctness does not rely on accuracy.
- **Data Attachment**: Byte buffer tied to a lease. Writable during creation; read-only after sealing. Used for direct copy between leases to move data.

## Node API Semantics
- **Acquire(objid, flag, meta?) → lease (+ data attachment)**
  - Flags cover intent (e.g., create vs. read). Returns lease ID, attachment handle, and any returned metadata.
  - For create intent, initializes object in CREATING state and grants write access to the attachment.
  - For read intent, may fail if the node lacks a sealed copy or denies the lease.
  - Lease lifetime is bounded; caller must hold the lease while mutating or reading attachments.
- **Seal(lease_id)**
  - Transitions the associated object from CREATING to SEALED on the node.
  - Freezes attachment bytes and metadata; object becomes immutable.
  - May register the sealed object in the resolver as a candidate, but registration is soft-state.
- **Release(lease_id)**
  - Terminates the caller’s rights and attachment access for the lease.
  - Allows the node to reclaim storage or drop the object if no other leases hold it.

## Object State Machine
- **CREATING**
  - Entered by `Acquire` with create intent; attachment writable.
  - Valid transitions:
    - `Seal` → **SEALED** (freeze bytes, publish locally).
    - `Release` → object discarded if unsealed; lease ends.
- **SEALED**
  - Attachment is read-only; object immutable.
  - `Release` ends the lease; node may evict sealed copy according to local policy.

## Transfer as Composition
- Transfer is not a distinct API. A caller copies bytes between two active attachments:
  - Acquire a writable lease on the destination node (create intent).
  - Acquire a readable lease on the source node (read intent).
  - Stream bytes from source attachment to destination attachment.
  - Seal the destination lease to freeze the copy.
  - Release leases when finished.

## Composite Flows
### Write New Object
1. Client calls `Acquire(objid, create_flag, meta)` on local node; receives lease and writable attachment (state: CREATING).
2. Client writes object bytes into the attachment.
3. Client calls `Seal(lease_id)` to transition to SEALED and make the object immutable on that node.
4. Client optionally updates resolver with this node as a candidate holder (soft-state).
5. Client calls `Release(lease_id)` when done; node may evict later per policy.

### Read Miss (fetch-on-miss via resolver + copy + seal)
1. Client calls `Acquire(objid, read_flag)` on local node; it fails because the object is absent.
2. Client queries resolver for candidate nodes holding `objid`.
3. For each candidate node:
   - Attempt `Acquire(objid, read_flag)` on candidate; skip on failure.
   - If successful, also `Acquire(objid, create_flag)` on local node to obtain a writable attachment.
   - Copy bytes from candidate’s attachment to local writable attachment.
   - Call `Seal` on the local lease to finalize the copy.
   - Release both leases. Stop after first successful copy.
4. If all candidates fail, the miss propagates upward (caller may retry or declare not found).

## Design Invariants
- Only three hard APIs exist: `Acquire`, `Seal`, `Release`; all workflows compose these.
- Objects are writable only while in CREATING under an unsealed lease; SEALED objects are immutable.
- Lease ownership is required for any attachment access; actions without an active lease are invalid.
- Directory/resolver data is advisory; failure to acquire from a suggested node is expected and safe.
- Transfer never bypasses lease control; bytes move only through attachments associated with active leases.
- Sealing is a local, node-scoped operation; object lifecycle is not global across nodes.
- Releasing a lease ends rights and can permit eviction, but eviction does not break sealed immutability guarantees.

## Glossary
- **Acquire**: API call to obtain a lease (read or create) and its data attachment.
- **Attachment**: The byte buffer bound to a lease; writable during creation, read-only after sealing.
- **Lease**: Time-bounded right to access an object copy on a node, identified by `lease_id`.
- **Object**: Addressed by `objid`; immutable after sealing; may reference `prev_objid`.
- **Resolver / Directory**: Soft-state service mapping `objid` to candidate nodes; advisory only.
- **Seal**: API call that freezes a lease’s attachment, moving the object to SEALED state on that node.
- **Release**: API call that ends a lease and frees attachment access; may enable eviction.
