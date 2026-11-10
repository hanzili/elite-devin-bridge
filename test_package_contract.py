"""Offline artifact checks. No manifest validation calls or external access."""
import ast
import json
from pathlib import Path
import unittest
from bridge_core import Policy, Rejected


ROOT = Path(__file__).resolve().parent


class PackageContractTests(unittest.TestCase):
    def test_private_manifest_minimal_bot_scopes(self):
        manifest = json.loads((ROOT / "manifest.private-channel.draft.json").read_text())
        self.assertEqual(set(manifest["oauth_config"]["scopes"]["bot"]),
                         {"app_mentions:read", "assistant:write",
                          "bookmarks:read", "bookmarks:write",
                          "calls:read", "calls:write",
                          "canvases:read", "canvases:write",
                          "channels:history", "channels:join", "channels:manage",
                          "chat:write", "commands", "emoji:read",
                          "files:read", "files:write", "groups:history",
                          "links:read", "pins:read", "pins:write",
                          "reactions:read", "reactions:write",
                          "remote_files:read", "search:read.files"})

    def test_private_manifest_only_message_groups(self):
        manifest = json.loads((ROOT / "manifest.private-channel.draft.json").read_text())
        self.assertEqual(manifest["settings"]["event_subscriptions"]["bot_events"], ["message.groups"])
        self.assertTrue(manifest["settings"]["socket_mode_enabled"])
        self.assertFalse(manifest["settings"]["org_deploy_enabled"])

    def test_target_is_requested_private_workspace_channel(self):
        plan = json.loads((ROOT / "target.private-channel.example.json").read_text())
        self.assertEqual((plan["workspace_name"], plan["team_id"], plan["channel_name"], plan["channel_visibility"]),
                         ("your-workspace", "T00000000", "devin-tasks", "private"))
        self.assertEqual((plan["dot_user_id"], plan["dot_bot_id"]), ("U00000000", "B00000000"))

    def test_target_is_incomplete_and_cannot_enable_runtime(self):
        plan = json.loads((ROOT / "target.private-channel.example.json").read_text())
        self.assertEqual(plan["kind"], "offline-plan-not-runtime-config")
        for field in ("channel_id", "owner_user_id", "dot_app_id", "worker_user_id", "worker_bot_id", "receiver_app_id"):
            self.assertIsNone(plan[field])
        for field in ("launch_enabled", "workspace_trust_changed", "source_share_approved"):
            self.assertIs(plan[field], False)
        self.assertEqual(plan["approved_workspace_paths"], {})
        with self.assertRaises(Rejected):
            Policy(plan["team_id"], plan["channel_id"], plan["owner_user_id"], plan["dot_bot_id"],
                   plan["dot_app_id"], plan["worker_bot_id"], plan["dot_user_id"],
                   plan["worker_user_id"], frozenset(plan["workspace_aliases"]))

    def test_source_dependencies_are_standard_library_only(self):
        allowed = {"dataclasses", "json", "re", "threading", "hashlib", "sqlite3",
                   "contextlib", "time", "os", "shlex", "bridge_core", "bridge_store",
                   "bridge_engine", "task_scope", "result_summary"}
        for name in ("bridge_core.py", "bridge_store.py", "bridge_engine.py", "offline_wiring.py"):
            tree = ast.parse((ROOT / name).read_text())
            imports = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports.update(alias.name.split(".")[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom):
                    imports.add(node.module.split(".")[0])
            self.assertFalse(imports - allowed, (name, imports - allowed))


if __name__ == "__main__":
    unittest.main()
