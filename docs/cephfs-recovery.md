# CephFS recovery and v0.5.28 rollout

## Release and rollout status

[`v0.5.28`](https://github.com/SparkAIUR/storage-provider-opennebula/releases/tag/v0.5.28)
was released on September 9, 2026 from source
`fa2d08077ac3b7d443937d06061add863f17ee2d`.
[Release workflow 34391722704](https://github.com/SparkAIUR/storage-provider-opennebula/actions/runs/34391722704)
completed successfully. The hplmon failure test and the drained Bravo canary
passed. At that September 9 checkpoint, the staged production rollout was in
progress and the 24-hour observation period was pending. For the subsequent
authorization to advance before that window completed, see the
[September 10 release decision](../DECISIONS.md#2026-09-10-filesystem-expansion-and-v0529).
That authorization does not establish completion of the observation period.

| Artifact | Verified digest |
| --- | --- |
| Docker Hub and GHCR `v0.5.28` image index | `sha256:0a9b5132f54e0e1ce983bd34314a63a1956fc69f976bc81b15ae8e30987e3f30` |
| Linux AMD64 image manifest in both registries | `sha256:6f23bb1d4716c891dd63aae199f03cb067007e0b71d98e6e9c172da035959fef` |
| Helm chart `0.5.28`, app version `v0.5.28` | `sha256:02cc064df5fd1c94bd15cfea36dd0f535a2e91768ce06b706fdb0a5e08bee492` |

GHCR was inspected independently and matched the Docker Hub image index and
AMD64 manifest. The freshly pulled released AMD64 image passed the musl stat
diagnostic regression.

## Incident evidence

On 2026-09-09, AMD worker 0 on `hplbravoxla02` was unable to create a container because its shared CephFS bind returned `ENOTCONN`. The staging mount was absent and a disconnected pod bind remained. The FUSE client started with the pod on September 1 and exited with status 255. Retained logs do not establish why it exited.

The v0.5.27 stale-publish path holds the shared-volume mutex and calls recovery, which reacquires that same mutex. A regression test reproduced this deadlock. The node's observed lack of recovery progress is consistent with this defect, but no live goroutine dump proved its current blocked stack.

The investigation also found generic FUSE source matching could attribute targets to the wrong volume, and orphan cleanup could call recursive deletion after an unsuccessful unmount. No incident data deletion was established.

## Recovery contract

Stage, publish, unpublish, unstage, and background repair serialize per volume. Two recovery workers skip busy volumes; the two-minute sweep retries persisted sessions. Each attempt has a 59-second operation deadline and a one-second child-reaping allowance. Filesystem probes and mount commands run in subprocesses, and cancellation kills the subprocess group. An unreaped process retains its volume lock while a separate waiter observes its termination, freeing the recovery worker to handle other volumes. An uninterruptible kernel task can still require operator intervention.

Recovery validates the exact handle from kubelet metadata next to the staging and pod paths. SHA-256 stage directories are resolved through that metadata. Mount comparisons use filesystem type, device major/minor, and root instead of the generic `ceph-fuse` source. Unknown ownership aborts recovery without unmounting anything.

Persisted record keys and filenames must agree with their volume handles.
Published targets survive discovery and restaging. Session writes sync the file and parent directory. Unpublish tombstones are saved before cleanup, survive reconstruction, and can only be cleared by an explicit publish or completed unstage. Unstage intent also survives restart. Recovery never recreates a target whose kubelet metadata is gone. Unmount failures retain session records. Cleanup rechecks the mount table and removes only empty directories. It never recursively removes a mountpoint or deletes volume contents.

Foreground FUSE clients are reaped and report their exit status. `opennebula_csi_cephfs_recovery_pending_volumes` reports unresolved background recovery failures. Alert when it remains above zero for ten minutes. A host bind repaired by CSI does not replace a bind already held in an application's private mount namespace. Use a storage liveness check to restart that container, or drain and recreate it deliberately.

## Release gates

1. Run the Go suite, race regressions, Helm lint, rendered-manifest checks, and `hack/validate-chart-version-alignment.sh v0.5.28`.
2. Build the candidate for Linux AMD64 with its exact Git commit in build metadata.
3. Complete independent subagent review and the two-volume CephFS failure test on `hplmon`, the user-authorized replacement for the retired `hplcsi` lab. Stop only the FUSE process matching the test volume's exact subpath. Verify host-stage and target recovery, unchanged bytes and mount identity on the other volume, then a fresh container reading the recovered volume.
4. Pin existing wildcard chart consumers before publishing a new semantic chart. Tag publication automatically publishes images, the chart index, and a GitHub release.
5. On Bravo, pin the chart and use a node-only image override with `OnDelete` updates. Hold AMD's pending rollout and scheduled restart. Drain the healthy Frauditor FUSE consumer on `hplbravoxla02` before replacing that node plugin.
6. Recreate affected application containers, verify queue registration and successful jobs, then advance one Bravo node at a time after draining its affected FUSE clients.
7. Verify Flux readiness and actual image IDs. Restore maintenance settings and observe for 24 hours. Stop advancement on ownership ambiguity, failed recovery, or integrity differences.

Never delete AMD's PVC/PV, force-delete its pod, or recursively clean its mount directories. CSI replacement also terminates healthy FUSE clients owned by that container, so a blind plugin restart is not an isolated AMD repair.

## hplmon test procedure

The user selected `hplmon` for live validation and requested subagents instead of Claude or Oracle review. The retired lab endpoint and the expired Claude session are no longer release prerequisites.

1. Read the live HelmRelease, node DaemonSet, pods, mount tables, and PVC consumers. The September 9 preflight found the hplmon node image at v0.5.21 while the HelmRelease had failed attempts through chart 0.5.27; its storage-class reconciliation hook had failed. Do not treat the attempted chart version as the deployed version or run a full Helm upgrade to establish this test.
2. Hold automatic chart reconciliation and use an `OnDelete` node update with the exact candidate manifest digest. Select a node without existing FUSE clients, checking the process and mount tables immediately before replacing its plugin. If there are existing FUSE clients, drain their consumers before replacement. Preserve the existing controller and storage classes. Confirm the plugin's readiness and actual image ID after the candidate starts.
3. Select a distinct Ready, schedulable peer node. Run the following command with the verified node names and registry manifest digest:

   ```bash
   rtk proxy python3 hack/validate-cephfs-recovery.py \
     --cluster hplmon --expected-context spark-hplmon \
     --node <candidate-node> --peer-node <peer-node> \
     --expected-image-digest sha256:<manifest-digest>
   ```

4. Save the JSON result with the candidate source commit and deployment evidence. The script checks process-file-descriptor support before creating a unique namespace containing only two new RWX claims and three test consumers. It checks claim ownership, exact driver image identity, checksums on a second node, and the PID/start time/subpath of the FUSE client before killing it through a process file descriptor. The healthy test volume's client, mount identities, pod UID, and container restart count must remain unchanged, and the candidate plugin must not restart during the test. A new write after recovery must also be visible from the peer node.
5. The script deletes only its own labeled namespace after passing and retains it on failure. Wait for its test PVC/PV and FUSE cleanup, then verify existing hplmon workloads and storage health. Keep reconciliation held until the desired node image and update strategy are deliberately restored or advanced; resuming the previously failed HelmRelease can trigger its pending upgrade.

This test proves isolated FUSE recovery and data preservation for the candidate. It does not prove that a full hplmon Helm upgrade succeeds or replace the drained Bravo canary and observation period.

## Validation and review

Validation covered the full Go suite, shared-filesystem race tests, Helm lint,
chart-version alignment, Linux AMD64 builds, and container regression tests.
The release also passed the live hplmon failure test described below.

Reviews found ownership, interrupted cleanup, partial-bind permissions,
leaf-symlink, missing-session stale-stage, and read-only flag override issues.
Each finding was fixed with regression coverage. Independent subagent rereviews
found no remaining blockers. The live hplmon test then exposed a musl errno
translation defect, which was fixed and independently reviewed before release.

Publish checks effective read-only, nosuid, nodev, and noexec flags on existing
and newly created binds. A partial bind with incorrect flags requires repair.
Unmounted leaves must be absent or real directories, and successful commands
must establish the expected mount at the requested path. Kubelet path ancestors
must remain trusted; concurrent privileged path replacement is outside this
contract.

## First hplmon candidate test

Candidate source `ba7d01f806e9e042c2e72fb61027c056fe8cf400`, image manifest
`sha256:8363d0dc024f700c1be3e4a5ba0cdd535ee90e1074fa5b41c8718694e395d434`,
ran on hplmonw03 with zero plugin restarts. The peer was hplmonw02.
The live test killed only its own volume A FUSE client. Recovery stopped
because Alpine/musl stat reported `Socket not connected`, which the candidate
did not classify as ENOTCONN. This candidate did not pass the release gate.
Both test volume checksums remained unchanged and readable from the peer;
volume B remained readable on the candidate node. The test namespace, claims,
PVs and mounts were removed normally after diagnosis.

The follow-up maps exact stat diagnostics for the probed path to typed ENOTCONN
for both musl and glibc, with a fixed C locale. Unknown errors and cancellation
remain failures. The corrected candidate passed the complete failure test
described below.

During preflight Flux reverted plain kubectl-patch suspension. The effective
maintenance hold uses the resource reconciliation-disabled annotation and the
`flux-client-side-apply` field manager, plus suspension. Both the CSI Flux
Kustomization and HelmRelease holds survived an explicit root reconciliation.
The hplmon context name is `spark-hplmon`; its kc alias is `hplmon`.

## Passing hplmon validation

Runtime source `c381e1f2c44ff8e62e9c698c5a0ab8ba8bc4bcc1` passed the full
two-volume test on September 9. Image manifest
`sha256:63c8e03db77e5c2c4d97e383edbdc5d10a61d635e49e415ba45597d56374b7ef`
ran on hplmonw03 with peer hplmonw02. Host mounts recovered in 1.985 seconds.
The healthy volume client, mount identities and container stayed unchanged,
and the plugin had zero restarts. Old and new data matched from both nodes.

The first post-recovery write took 53.375 seconds. Earlier MDS operation history
proved a 50.644-second write-lock wait for this test pattern, with the killed
client stale and the replacement open. The live filesystem session timeout is
60 seconds. The harness now bounds the remote first write to 120 seconds and
reports its duration. This is MDS lock waiting after an abrupt client exit,
not a stuck CSI recovery worker.

The test deleted only its own namespace after passing.

## Bravo canary evidence

At 19:07 UTC on September 9, the v0.5.28 canary on `hplbravoxla02` had
automatically recovered AMD's missing staging mount and disconnected pod target.
AMD worker 0 became Ready, and both API pods were Ready. Actual async jobs
finished on each worker and preserved the Bravo response contract.

| Worker | Completed job |
| --- | --- |
| AMD worker 0 | `7e916665-af3e-4b8d-9077-99651067c45c` |
| AMD worker 1 | `9cbbaf14-e2bd-4f30-9c3d-2b7ec82c90ca` |

The Frauditor global-search writer was drained before CSI replacement. It exited
with status 130 and resumed with its checkpoint matching the pre-drain baseline.

This proves recovery on the affected Bravo node. Advance only after each node's
affected FUSE clients are drained and its storage and application checks pass.
See [release and rollout status](#release-and-rollout-status) for the observation
checkpoint and subsequent release decision.
