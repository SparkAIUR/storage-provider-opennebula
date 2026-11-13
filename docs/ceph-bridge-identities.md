# Ceph bridge identity bindings and RC2 hooks

RC1 publication completed, but hpldb's mandatory pre-upgrade preflight rejected its Ceph datastore because BRIDGE_LIST used IP endpoints while registered hosts used names. That hook failed before any CSI workload update. RC2 corrects identity authorization without changing backend configuration or treating unknown bridges as safe. Existing RC1 artifacts, failed Job/Pod and hook inputs remain separate evidence. Native rollout acceptance is still open.

## Binding configuration

The only chart setting is `driver.cephBridgeIdentities`. Its default is an empty object, rendered as an empty `ONE_CSI_CEPH_BRIDGE_IDENTITIES` environment value. A configured value is a JSON object in that environment variable:

```yaml
driver:
  cephBridgeIdentities:
    version: 1
    datastores:
      - datastoreID: 42
        configFingerprint: sha256:803e93cd0835b28d721ac07e4d33a3b7cd753514b5c3f5760d26e503eb4abe95
        endpoints:
          - endpoint: 192.0.2.10
            hostID: 7
            hostName: bridge-a.example.invalid
```

This is a fictional example, using documentation-only addresses and names. Its fingerprint corresponds to the canonical bytes below, not to any production datastore. Production bindings must come from independently reviewed operator inventory and current provider readback.

The parser rejects unknown versions/fields, duplicate JSON object keys, repeated datastore IDs or endpoints, conflicting host ID/name pairs, missing IDs, invalid fingerprints and incomplete entries. Explicit hostID 0 is valid; an omitted hostID is invalid. A nonempty object must have version 1 and a datastores array. Empty environment configuration permits only unambiguous literal registered host names. There is no allow-unregistered switch or alternate configuration key.

Bindings are durable reviewed configuration. They are parsed once per component and passed explicitly to the persistent-disk provider, preflight and inventory. Every prerequisite decision reads a fresh host pool. Current host ID and exact name must agree in both directions, uniquely. Datastore configuration drift invalidates its fingerprint; rename or ID reuse invalidates a binding. Extra binding endpoints that disappeared from BRIDGE_LIST also fail. The driver performs no SSH, DNS lookup, suffix inference or evidence-URI fetch. IP reassignment outside OpenNebula remains the operator's inventory responsibility. There is no arbitrary binding expiry, monitoring timestamp requirement or periodic renewal.

## Fingerprint contract

`opennebula.CephBridgeConfigFingerprint` exports the deterministic fingerprint. Preflight emits `datastore_ID_bridge_fingerprint` with the current non-secret digest even when identity eligibility fails. Root can run the reviewed candidate's existing read-only preflight to obtain that digest before authorizing activation. It does not prove the proposed endpoint mapping.

The digest is lowercase SHA256 of the compact `encoding/json` representation of the following versioned fields, in this exact order:

```json
{"version":1,"datastoreID":42,"category":"IMAGE","backend":"ceph-rbd","dsMad":"ceph","tmMad":"ceph","diskType":"RBD","bridges":["192.0.2.10"],"monitors":["monitor.example.invalid:6789"],"pool":"volumes","user":"csi","rbdFormat":"","ecPool":""}
```

Datastore ID is numeric. Category and disk type are trimmed and uppercased; driver names are trimmed and lowercased. Ceph/rbd/ceph-rbd backend aliases canonicalize to ceph-rbd. Bridge tokens use Bash's default space, tab and newline separators, in their original order. Supported tokens are bare IPv4/IPv6 addresses without zones or brackets, or ASCII host/SSH aliases of at most 253 bytes beginning with a letter/digit and containing only letters/digits, dot, underscore or hyphen. Named endpoint ports, usernames, commas, shell metacharacters, globs, quotes, slashes, control characters and non-IFS whitespace are unsupported and rejected. Repeated endpoints are rejected. The parser, fingerprint and checker share this validator. The monitor list uses the driver's comma/semicolon/whitespace normalization and preserves order. Pool, user, RBD format and EC-pool name are trimmed and remain case-sensitive. An empty monitor list encodes as an array; an empty bridge list is rejected. Bridge or monitor reordering changes the digest. Capacity, provider state, timestamps, secret references/values, key values and configuration-file paths/contents are excluded. No credential or secret material is printed by this diagnostic.

## Every selectable endpoint must be verified

OpenNebula 7.4.0 and the pinned 7.4.1 code try to remove literal registered NAMEs in err, dsbl or off state. If the onehost CLI query fails, remove_off_hosts emits the original BRIDGE_LIST, and get_destination_host does not reject that fallback. A successful driver host-pool query cannot prove that later CLI filtering succeeds. RC2 therefore verifies EVERY configured endpoint, regardless of literal/alias spelling. Every identity must be unique and currently MONITORED state 2; disabled, monitoring-disabled, monitoring-error, offline, unknown and ambiguous members all block allocation even beside a healthy member. See the [pinned libfs.sh](https://raw.githubusercontent.com/OpenNebula/one/108d8922c2073b0950281bc3f8ed86782e527fff/src/datastore_mad/remotes/libfs.sh).

The remaining endpoints can be selected by image ID modulo count or randomly. No filtering exemption is used. A canceled or failed host observation blocks allocation. The success diagnostic says "bridge identities and current registered-host eligibility verified". That is not proof of Ceph authentication, pool permissions or a successful allocation. Native lifecycle tests remain required.


## Failed hook retention

Preflight Job, ServiceAccount, ClusterRole and ClusterRoleBinding use a release-revision suffix. Reconcile Job and its paired input ConfigMap use the same revision suffix. Truncation preserves the suffix within 63 characters. The Job's SA/role references and reconcile input argument point to that exact revision. Chart version does not affect these names.

Hook Jobs use only hook-succeeded deletion policy and no finished-job TTL. Bootstrap/input hooks retain their unique revision through before-hook-creation, without hook-succeeded. This matters because Helm 3.21.4 deletes earlier successful hooks carrying hook-succeeded when a later Job fails. The nonmandatory preflight's normal resources use resource-policy keep and no TTL, so later revisions do not prune their failure evidence. See [Helm hook execution](https://raw.githubusercontent.com/helm/helm/v3.21.4/pkg/action/hooks.go).

Both Jobs retain backoffLimit 0 and a 300-second deadline. Do not automatically retry or reuse a failed release revision. Retained old Jobs, bootstrap resources and input ConfigMaps require explicit operator cleanup only after their evidence has been captured and reviewed. RC2 does not delete the fixed-name RC1 hook or its inputs.

Prepare a new immutable v0.6.0-rc.2 image/chart and candidate receipts after source review. Do not retag or overwrite RC1 or relabel its qualification receipts as RC2. Root owns credentials, reviewed production bindings, publication, hook execution and every live action. Full native qualification and ordered 24-hour cluster observations remain open gates.

A clearly named Helm lifecycle fixture parses real rendered hooks through Helm's manifest sorter, seeds the fixed-name RC1 failed Job/Pod/bootstrap/input state, then models Helm 3.21.4 cleanup after later hook failures across RC2 revisions. It proves retained inputs and references without claiming a live Helm run. Long-name and multi-digit revision render checks are separate. Root's read-only post-attempt comparison found existing hpldb StorageClass specs unchanged; this repair preserves hook order. The unrelated queue regression fixture seeds its intended empty ConfigMap and bounds its Patch barrier, while retaining all admission, cancellation and completion-order assertions. Production queue behavior is unchanged.
