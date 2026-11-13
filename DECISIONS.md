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
- HUMAN: Fix all four subsequent review findings. Use a per-node annotation token for diagnostic snapshot ordering, retain uncertain provider attempts, distinguish legacy marker persistence from derived report evidence, and preserve ownership checks while excluding unrelated foreign CSI mounts.
- HUMAN: Raw-block NodePublish must enforce the existing recovery authority protocol for new and already-bound targets. Block NodeStage may remain a no-format, no-mount operation; block expansion is unchanged. The root executor owns the active Oracle review and final native validation.


## OpenNebula7.4 candidate boundary

The first0.6.0 RC carries safety and dependency changes together. XML-RPC, CSI identities, CRDs, handles, maintenance keys and persisted holds remain compatible. New disk snapshots and detached growth fail closed; CephFS remains independent. Stable promotion reuses the qualified image digest and RC binary VendorVersion. Inventory request batching and status-write reduction follow recovery in a separate branch. Native provider tests and24h observation per cluster are release gates, not claims established by offline fixtures. See docs/opennebula-74-upgrade.md.

## 2026-11-12, local lint toolchain

- AI: Pin golangci-lint to 2.15.0 for Go 1.27 analysis. Default Makefile tools use versioned binaries under `bin/`, with Helm 3.21.4 built for the host platform. Explicit `HELM` and `GOLANGCI_LINT` overrides remain supported; local commands no longer select system Helm automatically.

## 2026-11-13, RC2 Ceph bridge identity and failed hook retention

- HUMAN: Existing authorization covers a live rollout to hpldb only. The user will request a review after its completed 24-hour observation before considering hplsvc; later clusters remain gated. This record does not claim completed native qualification or authorize progression to another cluster.
- AI: Configure reviewed, versioned endpoint-to-host bindings through `driver.cephBridgeIdentities` and `ONE_CSI_CEPH_BRIDGE_IDENTITIES`. Bind each datastore to the deterministic non-secret `CephBridgeConfigFingerprint`; bridge or monitor order, host ID or exact name drift invalidates authorization.
- AI: Fetch fresh provider observations for every prerequisite check and require every configured bridge to resolve uniquely to a currently Monitored registered host. The upstream host-filter failure path can select the original list, so one healthy endpoint cannot excuse an unknown or disabled member. This verifies identity and registered-host eligibility, not Ceph authentication or allocation permissions.
- AI: Treat bindings as durable operator inventory without a renewal TTL. Do not infer identity through DNS or SSH in the service. IP reassignment outside OpenNebula remains the operator's inventory responsibility.
- AI: Scope preflight and reconciliation Jobs, bootstrap RBAC and reconciliation input ConfigMaps to the release revision. Retain failed Jobs and their inputs across later revisions, preserve finite deadlines and existing hook order, and do not replay a failed revision.
- AI: Prepare v0.6.0-rc.2 as a new immutable candidate. Preserve RC1 artifacts and receipts. Local protocol, chart and lifecycle fixtures are offline evidence; publication, production bindings, native lifecycle qualification and ordered cluster observations remain separate gates owned by the root operator.

## 2026-11-13, RC2 review corrections

- AI: Preserve BRIDGE_LIST exactly at provider extraction so fingerprinting and prerequisite checks reject unsupported whitespace across runtime, preflight and inventory. Only Bash default IFS separators may be normalized by tokenization.
- AI: Check selected datastore prerequisites and authorization before writing clone identity metadata on either a new source or an existing retry target. Retain ownership, durable origin, capacity checks and idempotent normalization. Synthetic XML-RPC regressions cover denied writes and successful normalization; native acceptance remains outside this review phase.

## 2026-11-13, node expansion dependency contract correction

- AI: Replace the earlier mount-utils geometry assumption with the [node expansion verification contract](docs/node-expansion.md#verification-contract) for the pinned dependency, including RPC cancellation through command execution and retry waits. Preserve the earlier decision as history; its geometry claim does not describe this dependency. Provider capacity, attachment identity and reattach policies remain unchanged.
- AI: Keep this source correction separate from immutable RC2 artifacts. Offline fixtures and tests do not complete native expansion, ordinary reattach qualification or the 24-hour observation. Existing hpldb-only authorization and the required later review remain unchanged.

## 2026-11-13, node expansion mount binding correction

- HUMAN: Apply the functional mount/device binding correction in the source pipeline. OpenNebula 7.4.0 remains the target; this round authorizes no direct OpenNebula host actions or provider, package, service, GitOps or upgrade changes.
- AI: Resolve published and supplied staging bindings together, compare OS backing and mounted-filesystem identities rather than path strings, and retain that identity through device-capacity waits and reads. Remove the staging-resolution fallback and its unused helper. Unknown or changed identity rejects before filesystem resize. Keep exact requested capacity, mount-utils convergence, fatal statfs availability errors and cancellation behavior.
- AI: Preserve the original expansion regressions and add executable binding, command-boundary and local ext4 image RPC checks. Local image checks use a synthetic mount table and do not qualify native mounted volumes. Copilot's monthly-quota HTTP 402 remains a failed external check under the standing exception, not a source fix or clean pass.

## 2026-11-13, Btrfs mounted-device association and attribution correction

- HUMAN: OpenNebula 7.4.0 remains the target, with no direct OpenNebula host actions. The user authorized implementation work; this does not authorize provider, package, service, GitOps or upgrade actions.
- AI: Correct the attribution in the earlier mount binding entry. The functional binding design came from AI Oracle/root source review, not a specific human technical choice. Preserve the earlier entry as history; its HUMAN line records implementation authorization and operating constraints.
- AI: Use read-only Linux FS_INFO on the mounted directory and kernel sysfs block membership to associate Btrfs with the measured device. Require one device, maximum device ID 1 and exactly one block-class sysfs member with matching canonical major/minor. The default mount-utils Btrfs resize command targets device ID 1; ambiguous layouts cannot authorize that command. This is an identity proof rule, not a user feature waiver.
- AI: Pin FSID and mounted st_dev for the request as well as the backing device, reobserving after capacity reads and waits before resize. Unknown, malformed, failed or canceled observations fail closed. Add no cache, external binary, image dependency or mount configuration.
- AI: Positive mounted Btrfs records in local tests are kernel/sysfs boundary fixtures. Actual ioctl rejection and local ext4 tool checks do not qualify mounted Btrfs, provider lifecycle, deployment or release. Native qualification and the 24-hour observation remain held.

## 2026-11-13, Btrfs admission review correction

- AI: Root source review confirmed that source-kind shortcuts could bypass Btrfs association. Check every observed Btrfs target before those shortcuts and require a block-device source. Regular-file or character-device aliases fail before capacity commands, format probes or resize. Preserve regular-image behavior for other filesystem tests and valid block aliases.
- AI: This Linux admission correction does not change the pre-existing non-Linux success shortcut. That unsupported-platform/test-convenience behavior remains deferred outside this plan; do not claim cross-platform fail-closed behavior or native Btrfs acceptance.
