"""Memory-only wrapper. Importing or constructing it performs no real I/O."""
import json
from bridge_engine import BridgeEngine, OutputPolicy, NOTICES


class MemoryAcp:
    def __init__(self):
        self.lines: list[str] = []
        self.fail_after_next_write = False

    def write(self, packet: dict):
        line = json.dumps(packet, ensure_ascii=True, separators=(",", ":")) + "\n"
        self.lines.append(line)
        if self.fail_after_next_write:
            self.fail_after_next_write = False
            raise OSError("simulated ambiguous write")

    def packets(self):
        return [json.loads(line) for line in self.lines]


class MemorySlack:
    def __init__(self):
        self.messages: list[dict] = []
        self.acks: list[dict] = []
        self.fail_after_next_send = False

    def ack(self, envelope_id: str):
        self.acks.append({"envelope_id": envelope_id})

    def post(self, payload: dict):
        self.messages.append(dict(payload))
        if self.fail_after_next_send:
            self.fail_after_next_send = False
            raise OSError("simulated ambiguous Slack send")
        return {"ok": True, "channel": payload["channel"], "ts": f"1800000000.{len(self.messages):06}"}


class OfflineBridge(BridgeEngine):
    def __init__(self, ledger, policy, receiver_app_id, configured_paths, output_policy=None):
        super().__init__(ledger, policy, receiver_app_id, configured_paths,
                         MemorySlack(), MemoryAcp(), output_policy)

    receive_socket_fixture = BridgeEngine.receive_socket_frame
    feed_acp_fixture = BridgeEngine.feed_acp_bytes
    flush_one_slack_fixture = BridgeEngine.flush_one_slack
