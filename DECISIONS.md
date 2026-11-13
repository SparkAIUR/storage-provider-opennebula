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

## 2026-11-13, provider growth metadata prerequisite

- HUMAN: Require 7.4.1+ and prepare the provider upgrade. Provider package installation, host remotes synchronization and service restarts remain a separate operational review. Existing hpldb-only authorization and the required review after its completed 24-hour observation remain unchanged.
- AI: Gate real persistent disk growth, including restoration growth, on fresh provider identity and canonical version observations. Persistent SYSTEM/SELF transfer targets require 7.4.1 or a later canonical patch within 7.4. Unknown persistence, clone mode, transfer target or provider version cannot authorize growth.
- AI: Preserve basic 7.4.0 Probe/readiness and already-satisfied retries. A positively identified persistent NONE target may grow on canonical 7.4.0 because upstream updates its image size on growth completion. Do not infer safety from a datastore display name or apply a persistent attach SIZE override.
- AI: Keep terminal-state, capacity, attachment identity and detach protections unchanged. This guard does not repair existing expanded volumes, qualify native lifecycle cases or start the 24-hour observation. Preserve published RC2 and the separately reviewed node completion correction.

## 2026-11-13, current 7.4.0 target supersedes upgrade preparation

- HUMAN: "do not perform any action on OpenNebula hosts directly. We're on 7.4.0 so maintain work with that." This supersedes the earlier HUMAN choice to require 7.4.1+ and prepare a provider upgrade. Preserve that earlier entry as historical context; provider upgrade preparation is no longer the current direction.
- AI: Keep basic 7.4.0 support, Probe/readiness, already-satisfied retries and positively identified NONE-target growth. Retain the operation-specific denial of known-unsafe persistent SYSTEM/SELF growth pending a qualified 7.4.0 API-only path. Do not raise the global provider minimum or infer approval for a feature waiver.
- AI: Keep work implementation-only, with no direct host actions or assumed backend upgrade. This restriction does not establish positive affected-LVM expansion/reattach qualification on 7.4.0, complete native acceptance or start the 24-hour observation.

## 2026-11-13, publish restoration prerequisite propagation

- HUMAN: Approve the controller error mapping and CSI regressions beyond the original nine-file scope, plus correction of the growth-phase documentation guarantee. Keep OpenNebula 7.4.0 as the target and preserve the prohibition on direct host and native actions.
- AI: Map restoration prerequisite denials to actionable FailedPrecondition at the shared controller boundary for already-attached, queue-completed, post-attach and same-node sticky publish paths. Preserve cancellation and deadline classification and Internal for other restoration failures.
- AI: Limit the compatibility guard's mutation guarantee to disk growth and capacity metadata. Publish may already have attached or detached a sticky attachment before the guard runs; no new lifecycle containment is introduced. Native affected-growth/reattach qualification and the 24-hour observation remain open.

## 2026-11-13, growth observation context precedence

- AI: Check the operation context immediately after each fresh VM, image and version observation. Cancellation and deadline expiry take precedence over either successful replies or prerequisite denials, preserving their error identity for all controller restoration consumers. Synthetic transport regressions cover interrupted observations during expansion and restoration without growth or capacity metadata mutations. OpenNebula 7.4.0 remains the target; native qualification and the 24-hour observation remain open.

## 2026-11-13, queued embedded restoration context errors

- AI: Retain cancellation and deadline error identity through both direct restoration and AttachVolume's embedded restoration. Convert context errors to CSI status only after queue dispatch can classify its active timeout. Preserve prerequisite, capacity, conflict and unknown-error mappings without changing attachment policy or success bookkeeping. Synthetic transport regressions exercise real attach/restoration observations with a still-connected CSI caller; they do not establish native acceptance or start the 24-hour observation.

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

## 2026-11-13, real mounted-filesystem source admission

- AI: Root reproduced the same source-kind bypass for XFS after the Btrfs-only correction. Apply block-source admission to every nonempty observed mount type before regular-file or character-device shortcuts. Real file-backed mounts expose loop block devices; blank mount types remain an explicit synthetic local-image contract. Preserve the Btrfs association witness and ext4/XFS device checks.
- AI: Keep the earlier Btrfs tests and receipts, add full-handler ext3/ext4/XFS/Btrfs regressions, and rerun root's unchanged XFS overlay. This is the final scoped review revision, with no new filesystem feature, provider policy or non-Linux shortcut change. No native qualification is claimed.

## 2026-11-13, binding checks across filesystem commands

- AI: The pre-Resize observation cannot authorize a later filesystem command because mount-utils first awaits a format probe. Check the pinned published/staging association and exact captured source at child execution, after the resizer and after statfs. Keep checks synchronous and local to the RPC; preserve binding failures across mount-utils error formatting and give cancellation precedence after each observation.
- AI: Exercise actual pinned mount-utils with block-source and kernel-boundary fixtures for ext3/ext4/XFS/Btrfs across probing, command construction, resize and statfs. Valid aliases and successful no-ops retain requested capacity. These fixtures do not qualify mounted Btrfs or native OpenNebula 7.4.0 expansion; provider, rollout and 24-hour gates remain held.

## 2026-11-13, reviewed source integration

- AI: Combine reviewed provider source PR7 at 2eb6e4a and node source PR8 at 641d317 through ordinary local merges. Resolve only the three overlapping documentation files by retaining both contracts and their decision history. All code, test and Helm blobs remain exact component versions.
- AI: Preserve the current HUMAN 7.4.0/no-direct-host instruction and the superseded status of earlier upgrade preparation. This source integration neither changes the operation-specific growth prerequisite nor grants a native acceptance or feature waiver. Native qualification and the 24-hour observation remain open.

## 2026-11-13, node expansion deadline fixture synchronization

- AI: Capture the actual operation context at the first capacity command and await its cancellation before injecting the deadline observation error. Waiting for the caller context alone does not synchronize child cancellation. Retain all filesystem formats, command boundaries, deadline status and mutation assertions; production context handling is unchanged.
- AI: Preserve the independent Plan007 runtime failure receipt alongside the corrected fixture results. Local validation does not qualify native expansion or reattach, waive acceptance, or start the 24-hour observation. The current OpenNebula 7.4.0/no-direct-host instruction remains in force.
