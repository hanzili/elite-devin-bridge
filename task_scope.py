"""Per-task authorization scope. Pure policy evaluation — this module is NOT
a sandbox and provides no OS-level containment. It only decides whether a
permission request devin sends over ACP is inside the task's approved scope.

In-scope rule checks only:
  - file writes: every path must resolve inside the task workspace AND match
    a declared relative writable glob
  - test commands: the command's token sequence must exactly equal one of the
    declared allowed-test entries. Prefix matching is deliberately NOT used —
    a command name prefix is not a sandbox and cannot prove anything about
    arguments, redirection or shell composition.

Anything unrecognized or out of scope is denied; the engine records the
denial locally and pauses further work on that command's scope.
"""
from dataclasses import dataclass
import os
import re
import shlex

from bridge_core import Rejected


MAX_SCOPE_ITEMS = 32
MAX_ITEM_LEN = 300

# Shell control tokens. An allowed-test entry containing one of these could
# smuggle a second command past exact matching, so such entries are invalid.
SHELL_CONTROL = {"&&", "||", ";", "|", ">", "<", ">>", ">&", "<&", "&", "`",
                 "$(", "${", "(", ")", "{", "}", "*", "?", "[", "]", "!"}

WRITE_KINDS = {"edit", "delete", "move"}
EXEC_KINDS = {"execute"}

_GLOB_SPECIAL = re.compile(r"[*?\[\]{}\\]")
_PATH_CHARS = re.compile(r"^[A-Za-z0-9_./~@+-]+$")


def _glob_to_regex(pattern: str) -> re.Pattern:
    """Relative POSIX glob -> regex. `**` crosses directories, `*` does not."""
    out = []
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "*":
            if pattern[i:i + 2] == "**":
                # `**/` or trailing `**`: zero or more path segments
                if pattern[i:i + 3] == "**/":
                    out.append("(?:[^/]+/)*")
                    i += 3
                    continue
                out.append(".*")
                i += 2
                continue
            out.append("[^/]*")
        elif c == "?":
            out.append("[^/]")
        elif c == "[":
            j = pattern.find("]", i + 1)
            if j == -1:
                raise Rejected("invalid glob bracket")
            out.append(pattern[i:j + 1])
            i = j
        else:
            out.append(re.escape(c))
        i += 1
    return re.compile("^" + "".join(out) + "$")


def _validate_rel_glob(pattern: str) -> str:
    if (not isinstance(pattern, str) or not pattern or len(pattern) > MAX_ITEM_LEN
            or pattern.startswith("/") or pattern.startswith("~")
            or "\x00" in pattern or "\n" in pattern):
        raise Rejected("invalid writable pattern")
    norm = os.path.normpath(pattern)
    if norm == "." or norm.startswith("..") or os.path.isabs(norm):
        raise Rejected("writable pattern must stay inside the workspace")
    if not all(_PATH_CHARS.match(part) or _GLOB_SPECIAL.search(part)
               for part in norm.split("/")):
        raise Rejected("writable pattern has unsupported characters")
    return norm


def _tokenize(command: str):
    try:
        tokens = shlex.split(command, posix=True)
    except ValueError:
        return None
    return tokens or None


def _validate_test_entry(command: str) -> tuple:
    if not isinstance(command, str) or not command.strip() or len(command) > MAX_ITEM_LEN:
        raise Rejected("invalid test command")
    tokens = _tokenize(command)
    if tokens is None:
        raise Rejected("test command does not parse")
    if any(t in SHELL_CONTROL or any(m in t for m in ("&&", "||", ";", "|", ">", "<",
                                                     "`", "$(", "${", "\n", "\r", "\x00"))
           for t in tokens):
        raise Rejected("test command contains shell control syntax")
    return tuple(tokens)


@dataclass(frozen=True)
class ScopeDecision:
    approved: bool
    kind: str            # "exec" | "write" | "unrecognized" | "blocked"
    paths: tuple = ()    # workspace-relative paths (write ops only)
    command: str | None = None


@dataclass(frozen=True)
class TaskScope:
    """What THIS task may change. Absent keys mean nothing is allowed."""
    writable: tuple = ()        # validated relative globs
    tests: tuple = ()           # validated allowed test command strings
    _test_tokens: tuple = ()

    @classmethod
    def parse(cls, raw):
        if raw is None:
            return cls()
        if not isinstance(raw, dict) or set(raw) - {"writable", "tests"}:
            raise Rejected("unknown scope keys")
        writable = raw.get("writable") or []
        tests = raw.get("tests") or []
        if (not isinstance(writable, list) or not isinstance(tests, list)
                or len(writable) > MAX_SCOPE_ITEMS or len(tests) > MAX_SCOPE_ITEMS):
            raise Rejected("scope limits exceeded")
        return cls(
            writable=tuple(_validate_rel_glob(p) for p in writable),
            tests=tuple(t.strip() for t in tests),
            _test_tokens=tuple(_validate_test_entry(t) for t in tests),
        )

    def file_allowed(self, workspace: str, path: str) -> str | None:
        """Return the workspace-relative path if writable, else None."""
        if not isinstance(path, str) or not path:
            return None
        if not os.path.isabs(path):
            path = os.path.join(workspace, path)
        norm = os.path.normpath(path)
        root = os.path.normpath(workspace)
        if norm != root and not norm.startswith(root + os.sep):
            return None
        rel = os.path.relpath(norm, root)
        if any(_glob_to_regex(g).match(rel) for g in self.writable):
            return rel
        return None

    def command_allowed(self, command: str) -> bool:
        """Exact token-sequence equality only. NOT a prefix/sandbox check."""
        tokens = _tokenize(command) if isinstance(command, str) else None
        return tokens is not None and tuple(tokens) in self._test_tokens

    def evaluate(self, workspace: str, tool_call: dict) -> ScopeDecision:
        """Classify a session/request_permission toolCall against this scope."""
        if not isinstance(tool_call, dict):
            return ScopeDecision(False, "unrecognized")
        raw = tool_call.get("rawInput")
        if raw is not None and not isinstance(raw, dict):
            raw = None
        kind = tool_call.get("kind")
        command = (raw or {}).get("command") or (raw or {}).get("cmd")
        if kind in EXEC_KINDS or isinstance(command, str):
            if isinstance(command, str) and self.command_allowed(command):
                return ScopeDecision(True, "exec", command=command)
            return ScopeDecision(False, "exec",
                                 command=command if isinstance(command, str) else None)
        paths = []
        for loc in tool_call.get("locations") or []:
            if isinstance(loc, dict) and isinstance(loc.get("path"), str):
                paths.append(loc["path"])
        for key in ("file_path", "path", "new_path", "old_path"):
            value = (raw or {}).get(key)
            if isinstance(value, str):
                paths.append(value)
        if kind in WRITE_KINDS or paths:
            allowed = []
            for path in set(paths):
                rel = self.file_allowed(workspace, path)
                if rel is None:
                    return ScopeDecision(False, "write")
                allowed.append(rel)
            if allowed or kind in WRITE_KINDS:
                return ScopeDecision(bool(allowed), "write", paths=tuple(allowed))
        return ScopeDecision(False, "unrecognized")
