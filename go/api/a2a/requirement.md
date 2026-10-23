# Whole-execution requirement on public A2A sends

`SendMessageRequest.metadata["kagent.dev/execution-requirement/v1"]` is a
standard padded-base64 string containing the exact opaque JSON requirement
accepted by `SessionService.CreateSession.execution_requirement`.

This request input is not a proof, enforcer selection or host target. The
Session workflow owns target creation; Substrate owns pre-execution activation.
The carrier is optional only for policy-free Sessions. Every governed send,
including message retry or task/context continuation, must repeat the exact
stored bytes. Omitted, changed, malformed, oversized, noncanonical-base64 and
message/part-scoped carriers refuse. A legacy Session cannot be upgraded by a
continuation. No accepted carrier establishes enforcement by itself.

Push callbacks execute in the API worker outside the Actor boundary and are
unsupported for governed Sessions, both inline and through separate callback
registration. Ordinary policy-free behavior is unchanged. Governed checkpoint,
warm attachment, restore and fork support must not be inferred from send
support.

Tests exercise real public JSON-RPC and gRPC transports with normal services;
storage and runtime are doubles. These are routing and refusal tests, not
native enforcement or current-version Kaimahi qualification.
