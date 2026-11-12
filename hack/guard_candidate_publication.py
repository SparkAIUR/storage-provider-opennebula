#!/usr/bin/env python3
"""Refuse RC replacement before any push. Structured HTTP absence only; unknown fails closed."""

import argparse
import base64
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from upgrade_evidence import require


def fetch(url, headers=None):
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, headers=headers or {}), timeout=30
        ) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read()


def assert_manifest_absent(registry, repository, tag, credentials=None):
    endpoint = f"https://{registry}/v2/{repository}/manifests/{urllib.parse.quote(tag, safe='')}"
    headers = {
        "Accept": "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"
    }
    status, reply, _ = fetch(endpoint, headers)
    if status == 401:
        challenge = reply.get("WWW-Authenticate", "")
        require(challenge.startswith("Bearer "), "unknown registry authentication")
        fields = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
        realm = fields["realm"]
        require(
            urllib.parse.urlparse(realm).netloc in ("ghcr.io", "auth.docker.io")
            and realm.startswith("https://"),
            "untrusted token authority",
        )
        url = (
            realm
            + "?"
            + urllib.parse.urlencode(
                {k: fields[k] for k in ("service", "scope") if k in fields}
            )
        )
        auth = {}
        if credentials:
            auth["Authorization"] = (
                "Basic " + base64.b64encode(credentials.encode()).decode()
            )
        code, _, body = fetch(url, auth)
        require(code == 200, "registry authority unavailable")
        token = json.loads(body).get("token") or json.loads(body).get("access_token")
        require(bool(token), "missing registry token")
        headers["Authorization"] = "Bearer " + token
        status, _, _ = fetch(endpoint, headers)
    require(
        status == 404,
        f"refuse candidate replacement or uncertain registry response ({status})",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--stable", action="store_true")
    a = parser.parse_args()
    require(
        re.fullmatch(
            r"v\d+\.\d+\.\d+" if a.stable else r"v\d+\.\d+\.\d+-rc\.\d+", a.tag
        ),
        "valid release tag required",
    )
    token = os.environ["GH_TOKEN"]
    repo = os.environ["GITHUB_REPOSITORY"]
    headers = {
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json",
    }
    status, _, body = fetch("https://api.github.com/repos/" + repo, headers)
    require(
        status == 200 and json.loads(body)["full_name"].lower() == repo.lower(),
        "repository visibility unproven",
    )
    status, _, _ = fetch(
        "https://api.github.com/repos/" + repo + "/releases/tags/" + a.tag, headers
    )
    require(status == 404, "release exists or release visibility unknown")
    assert_manifest_absent(
        "ghcr.io",
        "sparkaiur/opennebula-csi",
        a.tag,
        os.environ["GITHUB_ACTOR"] + ":" + token,
    )
    assert_manifest_absent("registry-1.docker.io", "nudevco/opennebula-csi", a.tag)
    print(
        "RC tag/release absent in both registries; existing candidates cannot be overwritten"
    )


if __name__ == "__main__":
    main()
