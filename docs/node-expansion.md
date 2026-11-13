# Node expansion verification

## v0.5.29 regression

Frauditor rqlite requested 40 GiB. Its ext4 superblock contained 10,485,760 blocks of 4,096 bytes, but statfs excluded 193,188 overhead clusters and reported 42,158,374,912 bytes. The prior required-minus-512-MiB comparison falsely timed out after successful resize2fs calls.

## Verification contract

For mounted disk filesystems on Linux, the driver waits for the device to reach the full requested byte capacity before calling Kubernetes mount-utils `Resize`. Success requires a recognized filesystem and a `true` result without error. The pinned mount-utils 0.36.6 `NeedResize` returns true for ext3, ext4 and XFS even after growth; it indicates eligibility and delegates convergence to `resize2fs` or `xfs_growfs`. It cannot verify completion. Existing Btrfs resize support is retained.

Statfs byte values are diagnostic and may be lower than device capacity because of filesystem overhead. Statfs errors, format-observation errors, unsupported or unformatted filesystems, failed resize commands and `Resize=false` still fail the request. Short devices retain bounded retries; the retry interval applies only while waiting for device capacity. The verification timeout bounds that wait and external commands. RPC cancellation or an earlier RPC deadline interrupts commands and retry waits, and the driver checks the context after external calls and before success. Timeout returns `DeadlineExceeded`; cancellation returns `Canceled`.

`driver.nodeExpand.sizeToleranceBytes` and `ONE_CSI_NODE_EXPAND_SIZE_TOLERANCE_BYTES` are deprecated and ignored as of v0.5.29. Existing configuration remains accepted but cannot relax the device-capacity or resize-result checks. The implementation is in [NodeExpandVolume](../pkg/csi/driver/nodeserver.go). Raw-block and CephFS behavior is unchanged.

## Validation

Current regression coverage in [node_expand_validation_test.go](../pkg/csi/driver/node_expand_validation_test.go), [nodeserver_test.go](../pkg/csi/driver/nodeserver_test.go) and the [Linux mount-utils fixtures](../pkg/csi/driver/nodeexpand_mountutils_linux_test.go) covers the exact 40 GiB/statfs case, devices one byte or 512 MiB short despite configured tolerance, delayed device growth, failed format observations and resize commands, unsupported and unformatted devices, false resize results, fatal statfs errors, cancellation and deadlines. Linux fixtures exercise the actual pinned dependency with synthetic command output, not real volumes. Earlier synthetic post-resize geometry-progress and geometry-error assumptions were replaced with these dependency-contract checks.

Current native qualification remains pending under the [OpenNebula 7.4 upgrade gates](opennebula-74-upgrade.md). Historical evidence below does not qualify this source correction.

### Historical v0.5.29 evidence

The v0.5.29 candidate required a native 10 GiB to 40 GiB ext4 expansion on hplmon, checksum preservation, post-resize writes, normal pod replacement and cleanup. Candidate evidence below records those checks; it does not establish release publication or a rollout to all clusters.

A Linux AMD64 candidate built from source 9f0f12d1c454ed351f2c7d2b917a065b94e60a38 passed native validation on hplmonw03. A disposable ext4 volume grew from 10 GiB to 40 GiB, reproduced the exact production statfs size, and converged in one NodeExpandVolume attempt. The 16 MiB payload SHA256 survived expansion and normal pod replacement. Post-resize writes succeeded. CSI removed the disposable PV after normal detach, and the namespace and test StorageClass were removed.

The test uses a compatible local datastore because hplmonw03's system datastore 105 cannot attach datastore 124. This proves the node filesystem path on a real attached disk; Bravo's Ceph-backed PVCs require separate live recovery evidence.

See [the sanitized hplmon evidence](node-expansion-hplmon-validation.json). Rerun with `python3 hack/validate-node-expansion.py --cluster hplmon --node hplmonw03 --storage-class one --expected-image-digest sha256:<digest> --output /tmp/node-expansion.json` after installing the candidate on that lab node. On failure the unique test resources remain for inspection.
