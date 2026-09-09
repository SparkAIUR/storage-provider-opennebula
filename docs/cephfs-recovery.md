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
3. Complete the existing hplcsi release lab and a two-volume CephFS failure test. Stop only the FUSE process matching the test volume's exact subpath. Verify host-stage and target recovery, unchanged bytes and mount identity on the other volume, then a fresh container reading the recovered volume.
4. Pin existing wildcard chart consumers before publishing a new semantic chart. Tag publication automatically publishes images, the chart index, and a GitHub release.
5. On Bravo, pin the chart and use a node-only image override with `OnDelete` updates. Hold AMD's pending rollout and scheduled restart. Drain the healthy Frauditor FUSE consumer on `hplbravoxla02` before replacing that node plugin.
6. Recreate affected application containers, verify queue registration and successful jobs, then advance one Bravo node at a time after draining its affected FUSE clients.
7. Verify Flux readiness and actual image IDs. Restore maintenance settings and observe for 24 hours. Stop advancement on ownership ambiguity, failed recovery, or integrity differences.

Never delete AMD's PVC/PV, force-delete its pod, or recursively clean its mount directories. CSI replacement also terminates healthy FUSE clients owned by that container, so a blind plugin restart is not an isolated AMD repair.

## Candidate validation status

The complete Go suite, shared-filesystem race tests, Helm lint, chart-version alignment, and Linux AMD64 container helper checks passed locally. Regression coverage includes the original nested-lock failure, failed unmount and state writes, interrupted unstage, foreign mount identities, missing kubelet target metadata, v0.5.27 session loading, in-flight recovery events, and unreaped child isolation.

The live two-volume failure test has not run. The saved hplcsi endpoint was unreachable on September 9. No semantic tag or production rollout is claimed by this change.

Earlier build checkpoint: `993fdfc98ec9cf5a56a2b7db8cc6d7b91e1e51fc`.
Subsequent record-ownership and interrupted-GC fixes require a fresh build.
The local Linux AMD64 image `opennebula-csi:cephfs-993fdfc` built successfully
with `VERSION=v0.5.28-candidate` and that exact commit; local image ID is
`sha256:80f5be9cc4eb7c8eab93ab5a173f583854f96559caae4769e7dfc17280ddbb82`.
This is a local image ID, not a registry manifest digest or deployed image.
Linux AMD64 regression tests also passed inside the Alpine runtime.

The no-mistakes run `01M23HTXVQTTDBATCNHXDD3KMQ` stopped before code review
because the configured Claude runner reported an expired OAuth session.
It returned branch custody without changing the submitted commit. No branch
was pushed and no PR was created. Automated review must be rerun after runner
authentication is restored. Oracle follow-up `amd-cephfs-final-review` is the
independent review of that source checkpoint. It completed in 28 minutes and
reported partial-bind flag validation, leaf symlinks, and interrupted orphan
cleanup as blockers. The later candidate addresses all three, with regressions
for read-only/security flags after failed or cancelled binds, symlink stage and
target leaves, and interrupted cleanup replay. This is verified remediation of
the findings, not a claim of a subsequent passing external review.

The latest source checkpoint, `1da640c6a8e4f482092ec164119bb6dd40aa72da`,
passed the full Go suite and shared-filesystem race checks. Its local Linux
AMD64 image is `opennebula-csi:cephfs-1da640c`, image ID
`sha256:700ff62757f2805e555cd761202250858ec9e4c54c92e304e9f9ec5e2c3ec8da`.
The added session-key and orphan-cleanup regressions also passed inside that
image. No registry manifest digest exists yet because no image was pushed.

The final publish path checks effective read-only, nosuid, nodev, and noexec
flags on existing and newly created binds. Recovery classifies a partial bind
with incorrect flags as requiring repair. Missing or symlink mount leaves are
checked before mounting, and successful mount commands must establish the
expected mount at the literal requested path. Host kubelet path ancestors must
remain trusted; concurrent privileged path replacement is outside this contract.
