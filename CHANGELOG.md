# Changelog

## v0.5.29, 2026-09-10

- Verify mounted filesystem expansion using filesystem geometry instead of statfs capacity, which excludes ext4 metadata. This fixes fully expanded 40 GiB volumes remaining in NodeResizeError and blocking pod startup.
- Require the full requested block-device size before running the filesystem resizer. The legacy byte-tolerance setting remains accepted but is ignored.
- Fail closed when resize or geometry inspection fails; retain bounded retries for incomplete growth.
- Add a regression using the exact Frauditor disk and statfs sizes, plus undersized-device and error-path tests.

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

Published after independent subagent review and a passing two-volume FUSE failure test on hplmon. The drained Bravo canary on `hplbravoxla02` recovered the AMD mount, and actual async jobs completed on both AMD workers. The staged production rollout remains in progress; the 24-hour observation period is pending. See [release and rollout evidence](docs/cephfs-recovery.md).
