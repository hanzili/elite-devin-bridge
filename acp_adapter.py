"""Explicit local ACP stdio adapter; importing this module starts nothing.

No credential lookup, workspace-trust mutation or CLI entry point is provided.
The launch function is source for later authorized local integration ONLY.
"""
from dataclasses import dataclass, field
import json
import os
import select
import signal
import subprocess
import time
from bridge_core import Rejected
from slack_adapter import AdapterFailure


ENV_ALLOWLIST = frozenset({"HOME", "PATH", "LANG", "LC_ALL", "TMPDIR", "TERM", "NO_COLOR"})


@dataclass(frozen=True)
class LaunchPlan:
    executable: str
    workspace: str
    environment: dict = field(repr=False)
    io_timeout: float = 5.0

    def validate(self):
        for path in (self.executable, self.workspace):
            if (not isinstance(path, str) or not path.startswith("/") or "\x00" in path
                    or os.path.normpath(path) != path):
                raise Rejected("launch paths must be canonical approved absolute paths")
        if not isinstance(self.environment, dict) or set(self.environment) - ENV_ALLOWLIST:
            raise Rejected("child environment contains a non-allowlisted variable")
        for key, value in self.environment.items():
            if (not isinstance(value, str) or "\x00" in value or "\n" in value or "\r" in value
                    or len(value) > 8192):
                raise Rejected("invalid child environment value")
        if not {"HOME", "PATH"} <= set(self.environment):
            raise Rejected("explicit approved HOME and PATH are required")
        if type(self.io_timeout) not in (int, float) or not 0 < self.io_timeout <= 30:
            raise Rejected("bounded I/O timeout required")


class FdPipeIO:
    """Nonblocking owned pipes with bounded read/write waits; no logging."""
    def __init__(self, process, timeout):
        self.input_fd = process.stdin.fileno()
        self.output_fd = process.stdout.fileno()
        self.timeout = timeout
        os.set_blocking(self.input_fd, False)
        os.set_blocking(self.output_fd, False)

    def write(self, data, timeout):
        ready = select.select([], [self.input_fd], [], min(timeout, self.timeout))[1]
        if not ready:
            raise OSError("ACP write timed out")
        return os.write(self.input_fd, data)

    def read(self, limit, timeout):
        ready = select.select([self.output_fd], [], [], timeout)[0]
        if not ready:
            return None
        return os.read(self.output_fd, limit)


@dataclass(frozen=True)
class CleanupResult:
    parent_exited: bool
    # This implementation does not claim cgroup descendant verification.
    descendants_verified: bool = False


def stop_owned_process_group(process, timeout=3.0):
    """Stop only this unreaped child/session; never target an arbitrary PID.

    Parent exit alone does not prove every descendant stopped. A reviewed
    cgroup/supervisor is still required for a production cleanup guarantee.
    """
    if process.poll() is not None:
        return CleanupResult(True, False)
    pid = process.pid
    if type(pid) is not int or pid <= 1 or os.getpgid(pid) != pid or pid == os.getpgrp():
        raise AdapterFailure("owned ACP process group could not be verified")
    os.killpg(pid, signal.SIGTERM)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        # wait timeout leaves the child unreaped, protecting against PID reuse.
        if os.getpgid(pid) != pid:
            raise AdapterFailure("ACP process group changed during cleanup")
        os.killpg(pid, signal.SIGKILL)
        process.wait(timeout=timeout)
    return CleanupResult(True, False)


class AcpProcessAdapter:
    def __init__(self, process, io_backend, group_stopper, io_timeout, approved_workspace):
        if (not isinstance(approved_workspace, str) or not approved_workspace.startswith("/")
                or os.path.normpath(approved_workspace) != approved_workspace):
            raise Rejected("exact preflighted workspace is required")
        self._process = process
        self._io = io_backend
        self._stopper = group_stopper
        self._timeout = io_timeout
        self._workspace = approved_workspace
        self.generation = object()
        self._failed = False
        self._closed = False

    def __repr__(self):
        return "<AcpProcessAdapter process_details=hidden>"

    @classmethod
    def launch(cls, plan, preflight, *, process_factory=None, io_factory=None, group_stopper=None):
        """STARTS Devin if explicitly called. Offline tests inject all factories.

        The operator must already approve actual launch/account/workspace/use.
        preflight MUST verify current canonical paths, trust without changing
        it, installed CLI permissions, sandbox and credential isolation. It is
        not supplied here, and a checklist boolean is not an authorization.
        """
        plan.validate()
        if not callable(preflight):
            raise Rejected("a real fail-closed local preflight is required")
        # Freeze caller-owned dictionaries before any external callback.
        env = dict(plan.environment)
        try:
            if preflight(plan) is not None:
                raise Rejected("preflight must raise on failure and return no authorization token")
        except Exception:
            raise AdapterFailure("ACP local preflight did not pass") from None
        factory = process_factory or subprocess.Popen
        io_builder = io_factory or FdPipeIO
        stopper = group_stopper or stop_owned_process_group
        process = None
        try:
            process = factory([plan.executable, "acp"], cwd=plan.workspace, env=env,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                shell=False, close_fds=True, start_new_session=True, bufsize=0)
            io = io_builder(process, plan.io_timeout)
            return cls(process, io, stopper, plan.io_timeout, plan.workspace)
        except Exception:
            cleanup_failed = False
            if process is not None:
                try:
                    stopper(process)
                except Exception:
                    cleanup_failed = True
                finally:
                    for stream in (process.stdin, process.stdout):
                        try:
                            stream.close()
                        except Exception:
                            cleanup_failed = True
            if cleanup_failed:
                raise AdapterFailure("ACP setup failed and cleanup is uncertain") from None
            raise AdapterFailure("ACP setup failed; no automatic retry") from None

    def write(self, packet):
        if self._failed or self._closed:
            raise AdapterFailure("ACP transport is unavailable")
        try:
            if not isinstance(packet, dict):
                raise ValueError("invalid packet")
            method = packet.get("method")
            if method is not None and method not in {
                    "initialize", "session/new", "session/load", "session/prompt", "session/cancel"}:
                raise ValueError("unsupported client method")
            if method in {"session/new", "session/load"}:
                params = packet.get("params")
                if (not isinstance(params, dict) or params.get("cwd") != self._workspace
                        or params.get("mcpServers") != []):
                    raise ValueError("session workspace differs from preflighted workspace")
            raw = (json.dumps(packet, ensure_ascii=True, separators=(",", ":")) + "\n").encode("utf-8")
            if len(raw) > 65536:
                raise ValueError("packet too large")
            end = time.monotonic() + self._timeout
            offset = 0
            while offset < len(raw):
                if time.monotonic() >= end:
                    raise OSError("deadline")
                count = self._io.write(raw[offset:], max(0, end - time.monotonic()))
                if type(count) is not int or count <= 0 or count > len(raw) - offset:
                    raise OSError("short or invalid write")
                offset += count
        except Exception:
            self._failed = True
            raise AdapterFailure("ACP write outcome is uncertain; no automatic replay") from None

    def read(self, timeout=0.0):
        if self._failed or self._closed:
            raise AdapterFailure("ACP transport is unavailable")
        if type(timeout) not in (int, float) or not 0 <= timeout <= 1:
            raise Rejected("bounded poll timeout required")
        try:
            data = self._io.read(32768, timeout)
            if data is not None and (not isinstance(data, bytes) or len(data) > 32768):
                raise OSError("invalid read")
            if data == b"":
                self._failed = True
            return data
        except Exception:
            self._failed = True
            raise AdapterFailure("ACP read failed") from None

    def close(self):
        if self._closed:
            raise Rejected("ACP cleanup already attempted; reconcile before retry")
        self._closed = True
        self._failed = True
        result, failed = None, False
        try:
            result = self._stopper(self._process)
        except Exception:
            failed = True
        finally:
            for stream in (self._process.stdin, self._process.stdout):
                try:
                    stream.close()
                except Exception:
                    failed = True
        if failed:
            raise AdapterFailure("ACP cleanup outcome is uncertain") from None
        return result
