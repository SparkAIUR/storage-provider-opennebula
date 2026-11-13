# Node expansion verification

## v0.5.29 regression

Frauditor rqlite requested 40 GiB. Its ext4 superblock contained 10,485,760 blocks of 4,096 bytes, but statfs excluded 193,188 overhead clusters and reported 42,158,374,912 bytes. The prior required-minus-512-MiB comparison falsely timed out after successful resize2fs calls.

## Verification contract

For mounted disk filesystems on Linux, the driver waits for the device to reach the full requested byte capacity before calling Kubernetes mount-utils `Resize`. Success requires a recognized filesystem and a `true` result without error. The pinned mount-utils 0.36.6 `NeedResize` returns true for ext3, ext4 and XFS even after growth; it indicates eligibility and delegates convergence to `resize2fs` or `xfs_growfs`. It cannot verify completion. Existing Btrfs resize support is retained.

Capacity and resize evidence must refer to the same backing device. The published mount and any supplied staging mount must expose observable device identities that agree. Device aliases are compared by kernel device number, and synthetic regular backing files by filesystem device and inode under the [source requirements below](#real-mounted-filesystem-sources). Block-backed mounts must also expose matching kernel filesystem identities. For ext3/ext4/XFS, that identity must match the block device number; Btrfs requires the [mounted-to-device association below](#btrfs-association). Missing, ambiguous, unobservable or mismatched bindings fail before resize. The driver pins that identity for the request and refreshes both mount bindings around each capacity read, including after retry waits, before invoking the resizer. A changed device requires a fresh request; capacity observed on the old device cannot authorize resizing the new one.

A successful statfs read is a required availability check, not a growth measurement. Its byte values are diagnostic and may be lower than device capacity because of filesystem overhead. Statfs errors, format-observation errors, unsupported or unformatted filesystems, failed resize commands and `Resize=false` still fail the request. Short devices retain bounded retries; the retry interval applies only while waiting for device capacity. The verification timeout bounds that wait and external commands. RPC cancellation or an earlier RPC deadline interrupts commands and retry waits, and the driver checks the context after external calls and before success. Timeout returns `DeadlineExceeded`; cancellation returns `Canceled`.

`driver.nodeExpand.sizeToleranceBytes` and `ONE_CSI_NODE_EXPAND_SIZE_TOLERANCE_BYTES` are deprecated and ignored as of v0.5.29. Existing configuration remains accepted but cannot relax the device-capacity or resize-result checks. The implementation is in [NodeExpandVolume](../pkg/csi/driver/nodeserver.go). Raw-block and CephFS behavior is unchanged.

## Real mounted-filesystem sources

Any nonempty filesystem type in the Linux mount observation requires a block-device source before the regular-file or character-device capacity shortcuts. This applies to ext3, ext4, XFS and Btrfs, including staging-only observations and requests with no staging path. A real file-backed mounted filesystem exposes a loop block device, not its regular backing image as the mounted source. Invalid source kinds reject before external capacity, format or resize commands.

Blank mount types remain the explicit synthetic regular-image contract used by isolated local ext4 image/gRPC tests. They do not establish real mounted-volume identity. Valid block aliases retain existing mounted-device checks, with the additional Btrfs witness below.

Mount-utils awaits `blkid` inside `Resize` before starting the filesystem resizer. The per-RPC command adapter revalidates the pinned association at child execution, including after that probe and before `resize2fs`, `xfs_growfs` or `btrfs`. It also checks the captured source path, so a valid new mount alias cannot conceal a stale path passed to the resizer. The handler reobserves the binding after resize and after statfs before success. Unknown or changed bindings return `FailedPrecondition`; cancellation or deadline expiry takes precedence after every observation. These observations do not make mount replacement atomic with command execution; unobserved replacement races still require external serialization.

## Btrfs association

Btrfs subvolumes use an [anonymous mounted device number](https://btrfs.readthedocs.io/en/stable/btrfs-subvolume.html#mount-options), so mounted st_dev cannot be equated with block st_rdev. Agreement between staging and published mounts is insufficient: both may still refer to filesystem A after their shared source alias points at device B.

The [mounted-filesystem source requirements](#real-mounted-filesystem-sources) apply before Btrfs association checks.

For a block-backed Btrfs mount, the driver opens the mounted directory read-only and issues the fixed-size [BTRFS_IOC_FS_INFO](https://github.com/torvalds/linux/blob/v6.17/fs/btrfs/ioctl.c#L2764). The kernel result supplies the actual mounted FSID, device count and maximum device ID. The driver requires one device and maximum ID 1, then reads `/sys/fs/btrfs/<FSID>/devices`. That directory must contain exactly one symlink to a kernel block-class member whose canonical `dev` major/minor matches the capacity source's st_rdev. [Kernel sysfs code](https://github.com/torvalds/linux/blob/v6.17/fs/btrfs/sysfs.c#L2188) creates those links from actual block-device objects. No identity is inferred from the current `/dev` alias or a block superblock UUID.

The association uses bounded directory and attribute reads and checks cancellation before and after kernel observations. It pins both FSID and mounted st_dev, together with the backing device, across the request. Staging and published paths must agree. The driver reobserves the binding after capacity reads and waits immediately before the resizer. Missing sysfs, failed ioctl, zero FSID, extra/missing devices, malformed attributes, changed identity and cancellation cannot authorize resizing. Linux metadata syscalls are read-only observations; these checks do not claim to interrupt a stuck kernel syscall.

The single-device/default-ID requirement matches the existing mount-utils `btrfs filesystem resize max` call, which [implicitly targets device ID 1](https://btrfs.readthedocs.io/en/stable/btrfs-filesystem.html#subcommand). Multiple-device or other-ID layouts are ambiguous for that command and fail closed. This preserves the driver's single-device Btrfs behavior without asserting a qualified multi-device resize contract or a human feature waiver. Ext4/XFS mounted-device equality and regular-image test behavior remain intact.

The new observer needs no binary or image/mount change. Existing local runtime inspection found `/sys` and `/sys/class/block`, but no `btrfs` binary; visibility of actual mounted Btrfs sysfs records and real Btrfs resize remains a qualification gate. No cluster or host was inspected to establish that visibility. Unknown runtime availability fails closed.

## Validation

Current regression coverage in [node_expand_validation_test.go](../pkg/csi/driver/node_expand_validation_test.go), [nodeserver_test.go](../pkg/csi/driver/nodeserver_test.go) and the [Linux mount-utils fixtures](../pkg/csi/driver/nodeexpand_mountutils_linux_test.go) covers the exact 40 GiB/statfs case, devices one byte or 512 MiB short despite configured tolerance, delayed device growth, failed format observations and resize commands, unsupported and unformatted devices, false resize results, fatal statfs errors, cancellation and deadlines. Linux fixtures exercise the actual pinned dependency with synthetic command output, not real volumes. Earlier synthetic post-resize geometry-progress and geometry-error assumptions were replaced with these dependency-contract checks.

The [binding regressions](../pkg/csi/driver/nodeexpand_binding_test.go) reject different devices, missing identities and changes during capacity waits while accepting valid aliases. The [Linux binding checks](../pkg/csi/driver/nodeexpand_binding_linux_test.go) exercise actual mount-utils with command-boundary fixtures and cancellation after a real format-probe process. Their opt-in `ONE_CSI_LOCAL_FILESYSTEM_TEST=1` test sends CSI gRPC requests against a disposable ext4 image using real filesystem tools, requires a subsequent explicit-size resize command to report a no-op, repeats an already-grown request, and rejects a mismatched image without mutation. This is local filesystem evidence with a synthetic mount table, not provider or native mounted-volume qualification.

The command-boundary regression matrix executes pinned mount-utils for ext3/ext4/XFS/Btrfs with explicit block-source and kernel observation fixtures. Source, published/staging mount, mounted identity and Btrfs FSID changes during probing, command construction, resize or statfs must reject without a CSI success response. Changes observed before mutation require zero mutating children; post-resize failures must precede statfs. Stable aliases and recognized successful no-ops must succeed, while canceled or expired observations retain their context status even through mount-utils error formatting.

Current native qualification remains pending under the [OpenNebula 7.4 upgrade gates](opennebula-74-upgrade.md). Historical evidence below does not qualify this source correction.

### Historical v0.5.29 evidence

The v0.5.29 candidate required a native 10 GiB to 40 GiB ext4 expansion on hplmon, checksum preservation, post-resize writes, normal pod replacement and cleanup. Candidate evidence below records those checks; it does not establish release publication or a rollout to all clusters.

A Linux AMD64 candidate built from source 9f0f12d1c454ed351f2c7d2b917a065b94e60a38 passed native validation on hplmonw03. A disposable ext4 volume grew from 10 GiB to 40 GiB, reproduced the exact production statfs size, and converged in one NodeExpandVolume attempt. The 16 MiB payload SHA256 survived expansion and normal pod replacement. Post-resize writes succeeded. CSI removed the disposable PV after normal detach, and the namespace and test StorageClass were removed.

The test uses a compatible local datastore because hplmonw03's system datastore 105 cannot attach datastore 124. This proves the node filesystem path on a real attached disk; Bravo's Ceph-backed PVCs require separate live recovery evidence.

See [the sanitized hplmon evidence](node-expansion-hplmon-validation.json). Rerun with `python3 hack/validate-node-expansion.py --cluster hplmon --node hplmonw03 --storage-class one --expected-image-digest sha256:<digest> --output /tmp/node-expansion.json` after installing the candidate on that lab node. On failure the unique test resources remain for inspection.

### Btrfs follow-up evidence boundary

Linux binding regressions create disposable local block-device nodes to exercise actual st_rdev identity while modeling mounted stat, FS_INFO and sysfs records at explicit external boundaries. They cover valid aliases and anonymous identities, both stale sources, no staging path, FSID or mounted-device changes during reads/waits, unavailable or ambiguous membership and cancellation/deadline. The real FS_INFO call rejects a local procfs directory. These are not actual mounted Btrfs positives. Existing real local ext4 image/gRPC/tool tests remain separate from synthetic mount observations, and all native/provider/rollout gates remain open.

Review correction: the initial Btrfs witness was reached only inside the block-source branch, permitting a Btrfs mount with retargeted regular-file or character-device sources to bypass it. Full-handler regressions reproduce that false success and now require FailedPrecondition, no response and zero capacity/resize commands. The correction is scoped to Linux; the pre-existing non-Linux success shortcut is deferred and is not evidence of cross-platform fail-closed behavior.

Final admission review: the Btrfs-only source-kind check was insufficient for the original all-filesystem binding contract. Full-handler regressions now retain Btrfs coverage and add ext3/ext4/XFS cases for both source aliases, no staging and staging-only observed types. Every denied case requires FailedPrecondition, nil response and zero capacity/format/resize commands. Root's independent XFS overlay is executed unchanged. The non-Linux shortcut remains deferred; no cross-platform or native acceptance is implied.
