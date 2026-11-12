import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import yaml


class SnapshotterRenderTest(unittest.TestCase):
    def test_snapshotter_selection_and_supervised_rollout(self):
        chart = Path(__file__).resolve().parents[1] / "helm/opennebula-csi"
        digest = "sha256:" + "b" * 64
        for overrides, expected in [
            ({}, "registry.k8s.io/sig-storage/csi-snapshotter:v8.7.0"),
            ({"sidecarImages.snapshotter.digest": digest}, "registry.k8s.io/sig-storage/csi-snapshotter@" + digest),
            ({"snapshotter.image": "registry.example/snapshotter:legacy", "sidecarImages.snapshotter.digest": digest}, "registry.example/snapshotter:legacy"),
        ]:
            with self.subTest(overrides=overrides):
                command = ["helm", "template", "fixture", str(chart), "--set", "credentials.existingSecret.name=fixture"]
                for key, value in overrides.items():
                    command += ["--set-string", key + "=" + value]
                rendered = subprocess.check_output(command, text=True)
                objects = [o for o in yaml.safe_load_all(rendered) if o]
                controller = next(o for o in objects if o["kind"] == "StatefulSet")
                images = {c["name"]: c["image"] for c in controller["spec"]["template"]["spec"]["containers"]}
                self.assertEqual(images["csi-snapshotter"], expected)
                node = next(o for o in objects if o["kind"] == "DaemonSet")
                self.assertEqual(node["spec"]["updateStrategy"], {"type": "OnDelete"})


class StableSourceCustodyTest(unittest.TestCase):
    def test_workflow_executes_against_qualified_commit_and_existing_tags(self):
        workflow = yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/promote-csi.yaml").read_text())
        script = next(s["run"] for s in workflow["jobs"]["promote"]["steps"] if s.get("name") == "Verify exact source and promote image without rebuilding")
        for tag_state in ("absent", "matching", "annotated", "different", "remote_failure"):
            with self.subTest(tag_state=tag_state), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                origin = root / "origin"
                origin.mkdir()

                def git(directory, *args):
                    return subprocess.check_output(["git", "-C", str(directory), "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", *args], text=True, stderr=subprocess.PIPE).strip()

                git(origin, "init", "-q")
                (origin / "source").write_text("qualified candidate")
                git(origin, "add", "source")
                git(origin, "commit", "-qm", "qualified candidate")
                qualified = git(origin, "rev-parse", "HEAD")
                tree = git(origin, "rev-parse", "HEAD^{tree}")
                (origin / "source").write_text("later default branch")
                git(origin, "commit", "-qam", "later default branch")
                if tag_state in ("matching", "different"):
                    git(origin, "tag", "v0.6.0", qualified if tag_state == "matching" else "HEAD")
                if tag_state == "annotated":
                    git(origin, "tag", "-a", "v0.6.0", qualified, "-m", "qualified")
                checkout = root / "checkout"
                checkout.mkdir()
                git(checkout, "init", "-q")
                git(checkout, "remote", "add", "origin", str(origin))
                git(checkout, "fetch", "origin", qualified)
                git(checkout, "checkout", "--detach", "FETCH_HEAD")
                if tag_state == "remote_failure":
                    git(checkout, "remote", "set-url", "origin", str(root / "missing"))
                (checkout / "qualification").mkdir()
                (checkout / "qualification/candidate.json").write_text(json.dumps({"sourceCommit": qualified, "sourceTree": tree, "images": {"opennebula-csi": "registry/driver@sha256:" + "a" * 64}}))
                (checkout / "hack").mkdir()
                (checkout / "hack/guard_candidate_publication.py").write_text("pass\n")
                binaries = root / "bin"
                binaries.mkdir()
                wrapper = "#!" + sys.executable + "\n" + '''import json, os, sys
from pathlib import Path
name = Path(sys.argv[0]).name
with open(os.environ["CALLS"], "a") as out:
    out.write(json.dumps([name, *sys.argv[1:]]) + "\\n")
if name == "docker" and sys.argv[1:4] == ["buildx", "imagetools", "inspect"]:
    print("sha256:" + "a" * 64)
'''
                for name in ("uv", "docker", "gh"):
                    executable = binaries / name
                    executable.write_text(wrapper)
                    executable.chmod(0o755)
                python = shutil.which("python3")
                self.assertIsNotNone(python)
                (binaries / "python3").symlink_to(python)
                calls = root / "calls.jsonl"
                env = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"], CALLS=str(calls), GITHUB_SHA=qualified, GITHUB_ACTOR="fixture", GH_TOKEN="fixture", STABLE_TAG="v0.6.0")
                result = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script], cwd=checkout, env=env, text=True, capture_output=True)
                commands = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
                if tag_state in ("different", "remote_failure"):
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(commands, [])
                else:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    release = next(c for c in commands if c[:3] == ["gh", "release", "create"])
                    self.assertEqual(release[3], "v0.6.0")
                    self.assertEqual(release[release.index("--target") + 1], qualified)


if __name__ == "__main__":
    unittest.main()
