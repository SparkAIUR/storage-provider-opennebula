# Decisions

## 2026-09-09, CephFS recovery

- HUMAN: Keep AMD's shared CephFS PVC and `/data/tmp`; do not migrate temp storage to `emptyDir`.
- HUMAN: Drain the affected Frauditor global-search worker before replacing CSI on `hplbravoxla02`.
- HUMAN: Release the validated fix as v0.5.28.
- AI: Preserve existing CSI response contracts and v0.5.27 session files. Use kubelet's adjacent `vol_data.json` and mountinfo identities to establish ownership.
- AI: Use foreground FUSE clients so startup cancellation can terminate the client and the driver can reap and report exits. Application containers retaining old mount namespaces need storage liveness probes or a controlled restart.
- AI: Keep chart publication separate from the Bravo canary until wildcard consumers are held on their existing version.
- HUMAN: Use hplmon as the test cluster because hplcsi is retired. Use subagent reviews; skip Claude and Oracle.
- AI: Limit hplmon validation to an immutable node canary and newly created test volumes. Preserve its controller and StorageClasses while its failed Helm release is held.

## 2026-09-10, filesystem expansion and v0.5.29

- HUMAN: Proceed with remediation, release v0.5.29, and upgrade all configured clusters after approximately 12 or more hours of the prior observation window. This authorizes advancing the release; it does not establish a completed 24-hour observation.
- AI: Reuse Kubernetes mount-utils filesystem geometry checks and require full outer device capacity. Preserve CephFS recovery behavior and existing PVC identities.
- AI: Use hplmon for release-candidate validation, then upgrade Bravo, hpldb, and hplsvc with controlled node replacements and consumer checks.

## 2026-09-10, release review follow-up

- HUMAN: Fix all seven follow-up findings, including the previously deferred corrections. The review phase changes source only; the outer executor owns validation gates, native checks, publication and rollout.
- AI: Remove metadata-attached automatic detach until a node/controller no-mount handoff exists. Require positive typed attachment-absence evidence for direct attachment.
- AI: Use the terminal local-device report as the sole durable authority for runtime-attachment repair. Retain its episode token and derive repair guards from fresh reads, eliminating the second repair-marker write.
- AI: Use independent per-node snapshot workers with two-second API deadlines and five delayed retries per version. Keep per-volume history write ordering outside the cache lock.
- AI: Treat existing benchmark resources for the current run generation as durable admission evidence after restart.
