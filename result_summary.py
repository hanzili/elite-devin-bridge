"""Structured task result summaries.

Only whitelisted fields ever reach Slack: completion status, workspace-
relative modified filenames, test names/results, denial counts and a bounded
model-provided remaining_issues string. Full logs, file contents, tool
output, environment and credentials never qualify for release — the outbox
gate re-validates the rendered form before any send.
"""
from dataclasses import dataclass, field
import json
import re

from bridge_core import Rejected, _unique_keys


SUMMARY_MARKER = "bridge.summary.v1"
MAX_ISSUES = 400
MAX_FILES = 50
MAX_TESTS = 30

STATUSES = {"done", "failed", "cancelled"}
TEST_RESULTS = {"pass", "fail", "skip", "unknown"}

# Instruction appended to every task prompt. The model is asked for a
# bounded machine-readable block; parsing failure simply omits model fields.
SUMMARY_CONTRACT = (
    "\n\nWhen you finish, end your reply with exactly one fenced ```json block "
    "in this shape (no other text inside the fence):\n"
    '{"bridge_result_v1": {"tests": [{"name": "...", "result": "pass|fail|skip"}], '
    '"remaining_issues": "one line, <400 chars"}}')


@dataclass
class Trace:
    """Observed (not model-claimed) facts for one prompt, collected in memory."""
    files: list = field(default_factory=list)       # workspace-relative paths
    tests: dict = field(default_factory=dict)       # command -> status string
    denials: int = 0
    denial_kinds: set = field(default_factory=set)

    def record_file(self, rel_path):
        if rel_path not in self.files and len(self.files) < MAX_FILES:
            self.files.append(rel_path)

    def record_test(self, command, status="approved"):
        if command not in self.tests and len(self.tests) < MAX_TESTS:
            self.tests[command] = status
        elif command in self.tests:
            self.tests[command] = status

    def record_denial(self, kind):
        self.denials += 1
        self.denial_kinds.add(kind)


def parse_model_summary(text):
    """Extract the LAST fenced bridge_result_v1 object, schema-validated."""
    if not isinstance(text, str):
        return None
    matches = re.findall(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
    for candidate in reversed(matches):
        try:
            payload = json.loads(candidate, object_pairs_hook=_unique_keys)
        except (ValueError, TypeError, RecursionError):
            continue
        body = payload.get("bridge_result_v1") if isinstance(payload, dict) else None
        if not isinstance(body, dict) or set(body) - {"tests", "remaining_issues"}:
            continue
        tests = body.get("tests")
        out_tests = []
        if tests is not None:
            if not isinstance(tests, list) or len(tests) > MAX_TESTS:
                continue
            ok = True
            for entry in tests:
                if (not isinstance(entry, dict) or set(entry) - {"name", "result"}
                        or not isinstance(entry.get("name"), str)
                        or len(entry["name"]) > 120
                        or entry.get("result") not in {"pass", "fail", "skip"}):
                    ok = False
                    break
                out_tests.append({"name": entry["name"], "result": entry["result"]})
            if not ok:
                continue
        issues = body.get("remaining_issues")
        if issues is not None:
            if not isinstance(issues, str) or len(issues) > MAX_ISSUES or "\n" in issues:
                continue
        return {"tests": out_tests, "remaining_issues": issues}
    return None


def render_summary(status, trace: Trace, model=None):
    """Fixed-schema render. Only whitelisted fields; no raw output."""
    if status not in STATUSES:
        raise Rejected("invalid summary status")
    lines = [SUMMARY_MARKER, f"status: {status}"]
    files = trace.files[:MAX_FILES]
    lines.append("files_modified: " + (", ".join(files) if files else "none"))
    tests = []
    seen = set()
    if model:
        for entry in model["tests"]:
            tests.append(f"{entry['name']} -> {entry['result']}")
            seen.add(entry["name"])
    for command, st in list(trace.tests.items())[:MAX_TESTS]:
        if command in seen:
            continue
        result = "unknown"
        if st in ("completed", "pass"):
            result = "pass"
        elif st in ("failed", "fail"):
            result = "fail"
        tests.append(f"{command} -> {result}")
    lines.append("tests: " + ("; ".join(tests) if tests else "none"))
    lines.append(f"scope_denials: {trace.denials}"
                 + (f" ({','.join(sorted(trace.denial_kinds))})" if trace.denial_kinds else ""))
    issues = (model or {}).get("remaining_issues")
    lines.append("remaining_issues: " + (issues if issues else "not reported"))
    return "\n".join(lines)


_LINE = re.compile(r"^[a-z_]+: ")
_NAME_RESULT = re.compile(r"^[^\x00-\x1f]{1,160} -> (?:pass|fail|skip|unknown)$")


def is_valid_rendered(text):
    """Outbox gate: re-validate a rendered summary before it may be sent."""
    if not isinstance(text, str) or len(text.encode("utf-8")) > 3000:
        return False
    lines = text.split("\n")
    if len(lines) != 6 or lines[0] != SUMMARY_MARKER:
        return False
    keys = [line.split(":", 1)[0] for line in lines[1:]]
    if keys != ["status", "files_modified", "tests", "scope_denials", "remaining_issues"]:
        return False
    if not lines[1].startswith("status: ") or lines[1][8:] not in STATUSES:
        return False
    if not _LINE.match(lines[5]) or "\x00" in text or "\r" in text:
        return False
    return True
