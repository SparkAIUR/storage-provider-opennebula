# OpenNebula release compatibility

The installed validation target is OpenNebula **7.4.0**. Treat **7.4.2** as a new target that needs its own native evidence. A CSI release carries a compatibility claim for one exact provider patch and the recorded artifacts and profile.

| Publication | Canonical CSI tag | Helm chart version | Immutable discovery reference | Record status |
| --- | --- | --- | --- | --- |
| RC4 on the 7.4.0 target | `v0.6.0-rc.4` | `0.6.0-rc.4` | `7.4.0-csi.0.6.0-rc.4` | `target-only` |
| Stable after native qualification on 7.4.0 | `v0.6.0` | `0.6.0` | `7.4.0-csi.0.6.0` | `native-verified` |

Candidate publication establishes artifact custody and target intent. Stable promotion retains the qualified RC image digest and binary `VendorVersion`. The stable chart retains the RC `appVersion` and uses the stable chart version. GitOps pins the exact Helm chart version and the OCI image digests for the driver and sidecars. Release readback separately verifies the served chart package and its index entry against the recorded SHA-256 before operators approve the GitOps pins.

The profile `opennebula-74-upgrade/v1` follows the [native qualification contract](opennebula-74-upgrade.md). Detached disk growth and disk-path snapshots are excluded from positive support and retain their rejection tests. On 7.4.0, affected persistent SYSTEM/SELF growth and growth on reattach are guarded. The candidate record lists this as a further exclusion. The current stable gate still requires positive attached growth and ordinary reattach on the affected path, with verified capacity and data. A successful guard rejection cannot satisfy that gate. Stable also requires all other native results and separate ordered 24-hour observations for hpldb, hplsvc, and Bravo. Review the approved provider-version readback for each qualifying environment. The receipt must report the exact observed patch.

Use a provider-qualified discovery reference to locate its immutable record. `7.4.0-stable` is an optional mutable discovery channel for an exact 7.4.0 qualified release. Keep it out of GitOps pins, Helm versions, and the RC publication workflow. Before moving it, require protected approval of the existing destination, the new immutable record, the exact artifact digests, and the complete native contract. Read it back after an approved update and verify its destination against the record.

## Read back the published release

Run these checks after the appropriate [candidate publication](../.github/workflows/release-csi.yaml) or [stable promotion](../.github/workflows/promote-csi.yaml) finishes. Record the exact workflow run and attempt, and confirm success, the expected workflow and event, the source commit, the release identity, and the required-reviewer environment approval. For stable, also verify the successful exact-head [native qualification](../.github/workflows/native-qualification.yaml) run and attempt referenced by the promotion. In the controlled workspace, retrieve the exact `candidate.json` and `native.json` used by that promotion. If run, attempt, and artifact bytes cannot be tied to the protected execution, stop.

Set `csi_tag`, `release_run`, `release_attempt`, `assets`, and `channel` from that readback. Use `candidates` for an RC or `qualified` for stable. Use a fresh local `assets` directory. Use the readback values only after the entire block succeeds. The tag fetch updates local `FETCH_HEAD` only.

```sh
(
  set -eu
  repo=SparkAIUR/storage-provider-opennebula
  chart=opennebula-csi-${csi_tag#v}.tgz
  pages=https://sparkaiur.github.io/storage-provider-opennebula/charts/$channel
  rtk proxy gh api --hostname github.com "repos/$repo/actions/runs/$release_run/attempts/$release_attempt" \
    --jq '{path,event,head_sha,conclusion,run_attempt}'
  rtk proxy git fetch --no-tags origin "refs/tags/$csi_tag"
  rtk proxy git rev-parse 'FETCH_HEAD^{commit}' 'FETCH_HEAD^{tree}'
  rtk proxy gh release download "$csi_tag" --repo "github.com/$repo" --dir "$assets" --pattern "$chart"
  rtk proxy curl -fsSL "$pages/$chart" -o "$assets/served-chart.tgz"
  rtk proxy cmp "$assets/$chart" "$assets/served-chart.tgz"
  rtk proxy curl -fsSL "$pages/index.yaml" -o "$assets/index.yaml"
  ghcr_digest=$(rtk proxy docker buildx imagetools inspect \
    "ghcr.io/sparkaiur/opennebula-csi:$csi_tag" --format '{{.Manifest.Digest}}')
  dockerhub_digest=$(rtk proxy docker buildx imagetools inspect \
    "docker.io/nudevco/opennebula-csi:$csi_tag" --format '{{.Manifest.Digest}}')
  test -n "$ghcr_digest" && test "$ghcr_digest" = "$dockerhub_digest" || exit 1
  if [ "$channel" = candidates ]; then
    rtk proxy gh release download "$csi_tag" --repo "github.com/$repo" --dir "$assets" \
      --pattern image.txt --pattern source-commit.txt --pattern source-tree.txt --pattern SHA256SUMS
    for asset in image.txt source-commit.txt source-tree.txt SHA256SUMS; do
      test -f "$assets/$asset"
    done
  fi
  rtk proxy helm show chart "$assets/$chart"
  rtk proxy shasum -a 256 "$assets/$chart"
  printf 'OCI index digest %s\n' "$ghcr_digest"
)
```

Confirm the matching `opennebula-csi` index entry has the exact version, served archive URL and digest. For stable, require the promoted digest to match the qualified image and retain the successful promotion chart-equivalence result. Use the native receipt's exact version and confirm it against approved provider readback.

## Author and verify the record

Save `release-readback.json` in controlled storage with these fields. Values in angle brackets below are placeholders for the observed values. `runId` and `runAttempt` are positive JSON integers. `chartSHA256` and `evidence.sha256` are lowercase hex without a `sha256:` prefix.

```json
{
  "csiTag": "v0.6.0-rc.4",
  "openNebulaVersion": "7.4.0",
  "profile": "opennebula-74-upgrade/v1",
  "exclusions": ["detached-disk-growth", "disk-snapshots", "affected-persistent-system-self-growth-reattach"],
  "sourceCommit": "<tag commit>",
  "sourceTree": "<tag tree>",
  "imageDigest": "sha256:<matching OCI index digest>",
  "chartSHA256": "<downloaded and served chart SHA-256>",
  "publication": {"runId": 0, "runAttempt": 0},
  "evidence": {"sha256": "<evidence SHA-256>"}
}
```

For a candidate, the helper derives the evidence URL from `publication`. Set `evidence.sha256` to the SHA-256 of the downloaded `SHA256SUMS` bytes. Include the affected SYSTEM/SELF exclusion only for a 7.4.0 candidate. For stable, change `csiTag` to `v0.6.0`, use only the first two exclusions, add the protected qualification `runId` and `runAttempt` to `evidence`, and set `evidence.sha256` to the raw-byte SHA-256 of `native.json`. An authenticated reviewer approves the exact readback file hash in the protected release record after verifying the scope, authenticity, and publication checks. Retrieve `approved_readback_sha256` from that approval.

The authoritative discovery location is the asset named `release-compatibility-<discovery-reference>.json` on the already-published canonical CSI GitHub Release. After protected approval, run the following Bash block from a checkout containing the helper, with authenticated `gh` and `jq` available. Set `records` and `published_records` to separate existing local directories and use an empty `published_records` for each readback. For stable, set `qualification` to the controlled directory holding the two qualification inputs.

```bash
(
  set -euo pipefail
  repo=SparkAIUR/storage-provider-opennebula
  if [ "$channel" = qualified ]; then
    set -- --qualification "$qualification"
  else
    set --
  fi
  compatibility_record() {
    rtk proxy uv run --no-project --with PyYAML==6.0.3 python hack/release_compatibility.py \
      --readback release-readback.json --approved-readback-sha256 "$approved_readback_sha256" \
      --release-assets "$assets" "$@"
  }

  record_path=$(compatibility_record author --record-dir "$records" "$@")
  record_name=${record_path##*/}
  csi_tag=$(jq -er '.csiTag' "$record_path")
  release_id=$(rtk proxy gh api --hostname github.com "repos/$repo/releases/tags/$csi_tag" \
    --jq 'select(.draft == false) | .id | select(type == "number" and . > 0)')
  : "${release_id:?expected an existing published CSI release}"

  lookup_asset() {
    rtk proxy gh api --hostname github.com --paginate --slurp "repos/$repo/releases/$release_id/assets" |
      jq -er --arg name "$record_name" '
        if type != "array" or length == 0 or any(.[]; type != "array")
        then error("invalid release asset listing") else . end |
        [.[][] | select(.name == $name)] |
        if length == 0 then ""
        elif length == 1 and .[0].state == "uploaded" then .[0].id | select(type == "number" and . > 0)
        else error("ambiguous or incomplete compatibility asset") end'
  }

  asset_id=$(lookup_asset)
  if [ -z "$asset_id" ]; then
    if ! rtk proxy gh release upload "$csi_tag" --repo "github.com/$repo" "$record_path"; then
      printf '%s\n' 'Upload failed. Checking whether an identical asset was published.' >&2
    fi
    asset_id=$(lookup_asset)
  fi
  : "${asset_id:?expected a published compatibility asset}"
  rtk proxy gh api --hostname github.com -H 'Accept: application/octet-stream' \
    "repos/$repo/releases/assets/$asset_id" > "$published_records/$record_name"
  compatibility_record verify --record-dir "$published_records" "$@"
)
```

A failed or ambiguous lookup stops publication. If the exact asset already exists, the block verifies its bytes against the approved readback and skips the upload. A confirmed absence permits a create-only upload. Readback after the upload attempt also covers a concurrent publisher winning the race for the same asset name. Retries succeed only when the downloaded bytes match. Keep earlier assets in place and send changed scope, source, digests, run attempt or evidence back for review. A changed artifact needs fresh custody and native qualification before any stable claim. See the [GitHub release asset API](https://docs.github.com/en/rest/releases/assets) and [upload command](https://cli.github.com/manual/gh_release_upload).

The helper checks local bytes, exact identity and version, and the complete native receipt contract for stable. Protected operators and workflows authenticate approvals, workflow runs, artifact origin, runtime version, and the actual native test results. Local fixtures cannot provide those facts. Keep credentials, operational receipts, and controlled qualification inputs in their approved storage. The existing promotion workflow attaches its qualification inputs to the GitHub release. Before promotion, verify whether those inputs are cleared for that release's audience and contain only approved material. If the workflow would expose controlled material, hold promotion until its publication step is addressed under separate review.
