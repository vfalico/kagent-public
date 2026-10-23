# Governed Resume uses the owning Actor UID precondition

For a nonempty opaque execution requirement, both Create-with-Resume and Resume
require the owning Control client optional ResumeActorExact capability. The
client sends the already-observed original Actor UID in ResumeActorRequest;
there is no name-only fallback. Existing post-result policy and identity checks
remain necessary but are not a substitute for this precondition. Ordinary
policy-free callers keep their existing ResumeActor behavior.

Requires the separate selected Substrate-fork exact-Resume API patch first.
Canonical modules and imports are preserved. For unpublished companion builds,
use the declared sibling checkout layout from the authoritative patch bundle
(kagent/go and substrate-kagent) or an explicit version-specific go.work
replacement as shown in task evidence. No publication was performed.

Race tests exercise matching control, replacement between preparation and
mutation, unsupported exact capability, and request-field propagation. These
are unit/regression observations, not final native/public Kaimahi qualification.
Lost Create replies and name-based retirement recovery remain separate gaps.
