# Governed Session creation uses the owning durable operation

The normal Session workflow passes its persisted operation UUID through the
opaque ActorCreation envelope before the first RPC. The owning Substrate API
atomically stores the original suspended response with creation. A lost Create
reply therefore does not erase cleanup identity. Every failed Create/Resume
attempt retires that same operation before exact-UID destructive termination.
Retirement is irreversible and serialized with creation and native Resume.
A retry of possibly issued governed Create is cleanup-only; neither an old
response nor current friendly-name lookup grants execution authority.

No enforcer policy, commands, receipts or inventory enter this client. Existing
policy-free operations retain behavior. Requires separate Substrate atomic
Create-operation/retirement API patch. Checkpoint forks remain refused when a
requirement is present.

Tests include lost committed Create response, lost successful Resume response,
partial cleanup, replacement preservation, retirement before delayed Create,
wire-field and frozen-input propagation, and real PostgreSQL Session operation
retention across fresh workflow instances. These are test doubles for compute;
only the database and lifecycle admission are real in those unit tests. Native
compute, normal-controller and public Kaimahi acceptance are separate obligations.

A cleanup-only retry that confirms original operation retirement and exact
termination now completes only a DELETED tombstone, never READY. The store
CAS checks original generation and current executor, preserves the creation
receipt, releases references, and clears execution authority. Replacement or
uncertain cleanup errors remain pending. A PostgreSQL workflow test covers
terminal recovery after lost Create, stale executor refusal and no recreation.

## Governed idle settlement

Explicit governed checkpoints and restores remain refused before compute changes.
Automatic post-Task idle settlement must therefore retain the protected READY
Actor, not claim an unsupported checkpoint forever. The claimed settled event
is completed without a snapshot, after exact requirement, Session lifecycle,
Task state and executor checks under the Session row lock. No execution
authority or target identity is changed. Normal deletion then stops the owned
Actor and retires its protection. A native production app.Run test covers
ordinary reconciliation, golden readiness, public Task execution and deletion;
no manual status writes or reconciler invocation is used. PostgreSQL and
workflow regressions cover stale claims, immutable requirements and no snapshot.
