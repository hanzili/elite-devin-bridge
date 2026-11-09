"""Cancel/exit verification helpers for the runtime layer.

The engine never assumes a cancelled task's devin process exited: the
runtime observes the actual child, then reports a boolean to
engine.confirm_exit. Unobserved or timed-out exits are reported False —
never upgraded to "verified" by inference.
"""
import os
import signal
import time


def poll_process_exit(process, timeout=5.0, interval=0.05,
                      monotonic=time.monotonic, sleeper=time.sleep):
    """True only when the child is actually observed reaped within timeout."""
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if process.poll() is not None:
            return True
        sleeper(interval)
    return process.poll() is not None


def ensure_group_stopped(process, grace=3.0, kill_grace=2.0,
                         monotonic=time.monotonic, sleeper=time.sleep):
    """SIGTERM then SIGKILL this owned process group. Returns observed exit.

    Parent exit alone does not prove every descendant stopped; a cgroup or
    supervisor is still required for a production-grade guarantee (see
    DEPLOYMENT_GATES). This function reports only what was observed.
    """
    if process.poll() is not None:
        return True
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    if poll_process_exit(process, grace, 0.05, monotonic, sleeper):
        return True
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    return poll_process_exit(process, kill_grace, 0.05, monotonic, sleeper)


def confirm_cancel_exit(engine, key, process, exit_timeout=8.0, **kwargs):
    """Wait for the cancelled task's process to exit; report truth to engine.

    If the process does not exit in time it is force-stopped and the result
    re-observed. The engine posts stopped_verified / stopped_unverified from
    this boolean only.
    """
    if process is None:
        engine.confirm_exit(key, False)
        return False
    exited = poll_process_exit(process, exit_timeout, **kwargs)
    if not exited:
        exited = ensure_group_stopped(process, **kwargs)
    engine.confirm_exit(key, exited)
    return exited
