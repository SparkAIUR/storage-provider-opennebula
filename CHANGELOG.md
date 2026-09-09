# Changelog

## v0.5.28, unreleased

- Fix recursive locking during stale CephFS publishing and serialize shared-volume lifecycle operations.
- Run independent recovery attempts with deadlines and executable filesystem probes.
- Reject symlink mount leaves and verify effective read-only/security flags after partial binds.
- Verify kubelet volume metadata and mount identities before recovering shared mounts. Preserve known targets across restaging and discovery.
- Stop cleanup on unmount failure and remove only empty mountpoint directories.
- Persist cleanup intent across driver restarts and failed state writes; reject unstage while published targets remain.
- Retain the affected volume lock for an unreaped process without occupying a recovery worker.
- Supervise foreground CephFS clients, reap exited processes, and record their exits.
- Export a gauge for volumes with unresolved recovery failures.

Lab validation, image publication, and the drained Bravo canary remain release gates.
