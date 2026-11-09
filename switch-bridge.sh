#!/bin/bash
# smoke-echo -> session-bridge switch. Runs as a transient systemd
# unit so it survives every session smoke-echo owns (including the
# one that launched it). Result lands in .switch-result; full trace
# in .switch-log. Rollback on any failed verification.
set -u
UNIT_DIR="$HOME/.config/systemd/user"
RESULT="$HOME/elite-devin-bridge/.switch-result"
LOG="$HOME/elite-devin-bridge/.switch-log"
exec >>"$LOG" 2>&1
set -x
date -Is
cp "$HOME/elite-devin-bridge/session-bridge.service" \
   "$UNIT_DIR/session-bridge.service"
systemctl --user daemon-reload
systemctl --user stop smoke-echo.service
systemctl --user start session-bridge.service
ok=""
for i in $(seq 1 20); do
    sleep 2
    if journalctl --user -u session-bridge.service -n 200 --no-pager \
            2>/dev/null | grep -q "session bridge up in"; then
        ok=1
        break
    fi
done
if [ -n "$ok" ]; then
    systemctl --user enable session-bridge.service
    systemctl --user disable smoke-echo.service
    echo "SWITCH_OK $(date -Is)" | tee "$RESULT"
    DEVIN_TASKS_DIR="$HOME/devin-tasks" \
        "$HOME/elite-devin-bridge/.venv/bin/python" \
        "$HOME/elite-devin-bridge/session_bridge.py" --approvals \
        >> "$RESULT" 2>&1
    journalctl --user -u session-bridge.service -n 15 --no-pager \
        >> "$RESULT" 2>&1
else
    systemctl --user stop session-bridge.service
    systemctl --user start smoke-echo.service
    echo "SWITCH_ROLLBACK $(date -Is)" | tee "$RESULT"
    journalctl --user -u session-bridge.service -n 30 --no-pager \
        >> "$RESULT" 2>&1
fi
