# Changelog

## v0.6.0-rc.1, unreleased

- Target OpenNebula7.4.x with the7.4 GoCA SDK, Go1.27.4, Kubernetes0.36.6 libraries, CSI1.13 and current storage sidecars.
- Preserve holds on uncertain provider evidence; pin known IDs, observe nonforced deletion, verify attached resize completion and restore verified capacity on reattach.
- Reconcile clone retries through atomically inherited durable origin metadata. Reject new disk snapshots and detached growth until lifecycle qualification.
- Revalidate datastore authority at allocation and report disabled/unknown Ceph bridge feasibility.
- Add digest images, configurable probes, webhook readiness/self-exclusion, retained mandatory preflight failures, supervised OnDelete rollout and release qualification gates.
- Verify capacity on publish retries, reconcile clone targets before source growth limits, bind qualification readbacks to each cluster, and pin stable release tags to the qualified source.
- Render snapshotter v8.7.0 or its frozen digest while preserving ordinary legacy overrides; keep node replacement OnDelete.
- Native qualification and ordered24h cluster observation remain pending. See [upgrade runbook](docs/opennebula-74-upgrade.md). Inventory batching/write reduction is a separate follow-up.

## v0.5.29, unreleased

- Apply recovery authority and identity verification to raw-block publishing, including already-bound targets, before confirming recovery or exposing the device.
- Preserve recovery authority after uncertain provider failures, retire verified released legacy repair markers conditionally, advance snapshot revisions for empty clears, and exclude unrelated foreign CSI mounts from CephFS discovery.
- Block automatic detach of metadata-attached disks; retain wrong-device evidence and bind confirmation to the observed recovery episode.
- Reconstruct owned CephFS sibling binds across absent stages and older superblocks before recovery.
- Persist hotplug snapshots independently per node with deadlines, ordering and bounded retries; keep history API writes outside the shared cache lock.
- Derive runtime repair from durable device reports, require current bounded manual mode for forced reprobe, and preserve benchmark admission across restart.

- Verify mounted filesystem expansion using filesystem geometry instead of statfs capacity, which excludes ext4 metadata. This fixes fully expanded 40 GiB volumes remaining in NodeResizeError and blocking pod startup.
- Require the full requested block-device size before running the filesystem resizer. The legacy byte-tolerance setting remains accepted but is ignored.
- Fail closed when resize or geometry inspection fails; retain bounded retries for incomplete growth.
- Add a regression using the exact Frauditor disk and statfs sizes, plus undersized-device and error-path tests.

For candidate checks and evidence, see [node expansion verification](docs/node-expansion.md#validation).

## v0.5.28, 2026-09-09

- Fix recursive locking during stale CephFS publishing and serialize shared-volume lifecycle operations.
- Translate both Alpine/musl and glibc stat diagnostics into typed ENOTCONN errors; unknown probe failures do not authorize recovery.
- Run independent recovery attempts with deadlines and executable filesystem probes.
- Enforce explicit read-only publishes even when capability flags contain conflicting `rw` options.
- Reject symlink mount leaves and verify effective read-only/security flags after partial binds.
- Reconstruct missing session records before stale-stage recovery and require a mounted, healthy stage before reporting success.
- Verify kubelet volume metadata and mount identities before recovering shared mounts. Preserve known targets across restaging and discovery.
- Stop cleanup on unmount failure and remove only empty mountpoint directories.
- Persist cleanup intent across driver restarts and failed state writes; reject unstage while published targets remain.
- Retain the affected volume lock for an unreaped process without occupying a recovery worker.
- Supervise foreground CephFS clients, reap exited processes, and record their exits.
- Export a gauge for volumes with unresolved recovery failures.

See [release and rollout evidence](docs/cephfs-recovery.md) for publication, native validation, and observation status.
