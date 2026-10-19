import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

import yaml


ACTION = Path(__file__).resolve().parents[1] / "action.yaml"


class HandleJobTest(unittest.TestCase):
    def run_job(self, statuses):
        action = yaml.safe_load(ACTION.read_text())
        script = next(
            step["run"] for step in action["runs"]["steps"]
            if step["name"] == "Handle Job"
        )
        inputs = {
            "waitForJob": "rspec",
            "namespace": "test",
            "timeoutMinuteStartContainer": "10",
            "deleteNamespace": "false",
        }
        script = re.sub(
            r"\$\{\{ inputs\.(\w+) \}\}",
            lambda match: inputs[match[1]], script,
        )
        # Keep the action's Bash and jq real; replace cluster I/O and delays.
        stubs = r'''
kubectl() {
  case "$*" in
    "get job "*) printf 'extractor rspec' ;;
    *" wait "*) return 1 ;;
    *" logs "*) return 0 ;;
    *" get pod "*containerStatuses*)
      local count
      count=$(cat "$STATUS_DIR/count")
      if [[ ! -f "$STATUS_DIR/$count" ]]; then
        echo "Action polled beyond the supplied statuses" >&2
        return 99
      fi
      cat "$STATUS_DIR/$count"
      echo $((count + 1)) > "$STATUS_DIR/count"
      ;;
    *" get pod "*metadata.name*) printf 'rspec-test' ;;
    "cp "*|"exec "*) return 0 ;;
    *) echo "Unexpected kubectl call: $*" >&2; return 98 ;;
  esac
}
sleep() { :; }
date() {
  if [[ "$1" == "-d" ]]; then
    printf '01/01 00:10'
  else
    printf '01/01 00:00'
  fi
}
'''
        with tempfile.TemporaryDirectory() as directory:
            status_dir = Path(directory)
            (status_dir / "count").write_text("0")
            for index, status in enumerate(statuses):
                (status_dir / str(index)).write_text(status)
            return subprocess.run(
                ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c",
                 stubs + script],
                env={**os.environ, "STATUS_DIR": directory},
                capture_output=True, text=True, timeout=5,
            )

    @staticmethod
    def container_status(reason=None):
        state = {"running": {}} if reason is None else {
            "terminated": {"reason": reason}
        }
        return json.dumps([
            {"name": "extractor", "state": {"running": {}}},
            {"name": "rspec", "state": state},
        ])

    def test_waits_for_missing_container_statuses_before_success(self):
        extractor_only = json.dumps([
            {"name": "extractor", "state": {"running": {}}},
        ])
        for pending in ("", "[]", extractor_only):
            with self.subTest(pending=pending):
                result = self.run_job([
                    pending, self.container_status("Completed"),
                ])
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_waits_for_running_container_before_success(self):
        result = self.run_job([
            self.container_status(), self.container_status("Completed"),
        ])
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_reports_terminal_failure_after_missing_status(self):
        result = self.run_job(["", self.container_status("Error")])
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("fetching terminate status for job: Error", result.stdout)


if __name__ == "__main__":
    unittest.main()
