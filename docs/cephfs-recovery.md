# CephFS recovery and v0.5.28 rollout

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
     --cluster hplmon --expected-context hplmon \
     --node <candidate-node> --peer-node <peer-node> \
     --expected-image-digest sha256:<manifest-digest>
   ```

4. Save the JSON result with the candidate source commit and deployment evidence. The script checks process-file-descriptor support before creating a unique namespace containing only two new RWX claims and three test consumers. It checks claim ownership, exact driver image identity, checksums on a second node, and the PID/start time/subpath of the FUSE client before killing it through a process file descriptor. The healthy test volume's client, mount identities, pod UID, and container restart count must remain unchanged, and the candidate plugin must not restart during the test. A new write after recovery must also be visible from the peer node.
5. The script deletes only its own labeled namespace after passing and retains it on failure. Wait for its test PVC/PV and FUSE cleanup, then verify existing hplmon workloads and storage health. Keep reconciliation held until the desired node image and update strategy are deliberately restored or advanced; resuming the previously failed HelmRelease can trigger its pending upgrade.

This test proves isolated FUSE recovery and data preservation for the candidate. It does not prove that a full hplmon Helm upgrade succeeds or replace the drained Bravo canary and observation period.

## Candidate validation status

Earlier runtime source checkpoint: `1562b16e52a203b269a98bf7970de4b065aee7be` on
`fix/cephfs-recovery`. The full Go suite, shared-filesystem race tests, Helm
lint, chart-version alignment, and Linux AMD64 build passed. Earlier Linux
container runs covered the main recovery suite; the final image also exercises
the added partial-bind and symlink regressions.

Local image: `opennebula-csi:cephfs-1562b16`.
Local image ID:
`sha256:b8989701550c1cc72708efbb64df6f4b68657f44d6a920d7c6b17337fe7ce247`.
The image embeds `v0.5.28-candidate` and the exact source commit. A local image
ID is not a registry manifest digest or deployed image. Nothing was pushed.

Oracle reviews `amd-cephfs-v0528-review` and `amd-cephfs-final-review` found
ownership, interrupted cleanup, partial-bind permissions, and leaf-symlink
issues. The final candidate addresses every reported finding with regression
coverage. This is remediation verified locally; no subsequent external review
has issued a passing verdict on the final commit.

Publish checks effective read-only, nosuid, nodev, and noexec flags on existing
and newly created binds. A partial bind with incorrect flags requires repair.
Unmounted leaves must be absent or real directories, and successful commands
must establish the expected mount at the requested path. Kubelet path ancestors
must remain trusted; concurrent privileged path replacement is outside this
contract.

The no-mistakes run `01M23HTXVQTTDBATCNHXDD3KMQ` stopped before code review
because its configured Claude runner reported an expired OAuth session.
It returned branch custody without changing the submitted commit. The user
subsequently requested independent subagent review and explicitly skipped
Claude and further Oracle review. The replacement reviews found a missing-session stale-stage false success
and a read-only flag override. Both are fixed with regressions that failed
before the changes. Independent rereview found no remaining blockers. No PR
was created by the failed runner.

The hplmon two-volume failure test and drained Bravo canary have not run.
The chart/version metadata is prepared for v0.5.28; no semantic release or
production recovery is claimed.
