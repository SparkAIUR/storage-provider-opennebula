# Node expansion verification

## v0.5.29 regression

Frauditor rqlite requested 40 GiB. Its ext4 superblock contained 10,485,760 blocks of 4,096 bytes, but statfs excluded 193,188 overhead clusters and reported 42,158,374,912 bytes. The prior required-minus-512-MiB comparison falsely timed out after successful resize2fs calls.

## Verification contract

The driver checks that the device reaches the full requested byte capacity, runs the filesystem resizer, and asks Kubernetes mount-utils `NeedResize` to verify that filesystem geometry fills the device within one filesystem block. Ext4 geometry includes metadata. XFS uses its data-block geometry. The statfs capacity is diagnostic information and is not a capacity gate. Unsupported or unformatted filesystems, failed resizers, failed statfs reads, and failed geometry reads do not produce success. Incomplete growth retains bounded retries and returns retryable `DeadlineExceeded` with requested, device, and filesystem byte context when the timeout expires.

`driver.nodeExpand.sizeToleranceBytes` and `ONE_CSI_NODE_EXPAND_SIZE_TOLERANCE_BYTES` are deprecated and ignored as of v0.5.29. Existing configuration remains accepted but cannot relax device-capacity or filesystem-geometry verification. The implementation is in [NodeExpandVolume](../pkg/csi/driver/nodeserver.go), with regression coverage in [node_expand_validation_test.go](../pkg/csi/driver/node_expand_validation_test.go).

## Validation

Before tagging this release, validate a native 10 GiB to 40 GiB ext4 expansion on hplmon, checksum preservation, post-resize writes, normal pod replacement, and cleanup. Candidate evidence below records those checks; it does not establish release publication or a rollout to all clusters.

The exact 40 GiB regression failed against the old implementation and passes after the fix. Go tests also cover a device one byte short, a device 512 MiB short despite a 1 GiB configured tolerance, resize errors, geometry errors, unformatted devices, delayed growth, and non-converging filesystems.

A Linux AMD64 candidate built from source 9f0f12d1c454ed351f2c7d2b917a065b94e60a38 passed native validation on hplmonw03. A disposable ext4 volume grew from 10 GiB to 40 GiB, reproduced the exact production statfs size, and converged in one NodeExpandVolume attempt. The 16 MiB payload SHA256 survived expansion and normal pod replacement. Post-resize writes succeeded. CSI removed the disposable PV after normal detach, and the namespace and test StorageClass were removed.

The test uses a compatible local datastore because hplmonw03's system datastore 105 cannot attach datastore 124. This proves the node filesystem path on a real attached disk; Bravo's Ceph-backed PVCs require separate live recovery evidence.

See [the sanitized hplmon evidence](node-expansion-hplmon-validation.json). Rerun with `python3 hack/validate-node-expansion.py --cluster hplmon --node hplmonw03 --storage-class one --expected-image-digest sha256:<digest> --output /tmp/node-expansion.json` after installing the candidate on that lab node. On failure the unique test resources remain for inspection.
