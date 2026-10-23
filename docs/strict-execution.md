# Whole-requirement execution and explicit cold start

For the qualified isolated microVM lane, author `Harness.spec.substrate.snapshotPolicy.coldStartOnly: true`. Default is false, retaining existing snapshot preparation. The flag is part of immutable revision identity and copied to Substrate SnapshotConfig. Cold start does not prove activation: the owning runtime still creates non-running target, binds complete requirement, validates active success, then starts it. No golden readiness is fabricated. Governed restore/fork/warm reuse remains explicitly refused until qualified.

All network-v2 endpoints, including model/proxy/control traffic, must be authored in the immutable shared policy. kagent must not parse Guardd policies or silently add additive EgressPolicy origins to a strict ceiling.
