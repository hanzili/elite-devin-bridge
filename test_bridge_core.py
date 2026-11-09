"""Offline standard-library tests; no credentials, network, or subprocesses."""
import json
import unittest
from dataclasses import replace
from bridge_core import (Policy, Rejected, parse_command, acp_initialize,
                         acp_new_session, acp_prompt, acp_cancel, deny_permission)


POLICY = Policy("T_TEST", "C_TEST", "U_OWNER", "B_DOT", "A_DOT", "B_WORKER",
                "U_DOT", "U_WORKER", frozenset({"scratch"}))
RUN_PREFIX = "<@U_WORKER> bridge.v1 "


def event(**changes):
    body = {"type": "message", "channel": "C_TEST", "user": "U_OWNER",
            "ts": "1700000000.000001", "text": RUN_PREFIX + json.dumps({
                "request_id": "request_001", "action": "run", "workspace": "scratch",
                "text": "Explain this sandbox project."})}
    body.update(changes)
    return {"type": "event_callback", "team_id": "T_TEST", "event_id": "Ev1", "event": body}


class PolicyTests(unittest.TestCase):
    def test_owner_command(self):
        self.assertEqual(parse_command(event(), POLICY).workspace, "scratch")

    def test_dot_bot_command(self):
        value = event(user="U_DOT", bot_id="B_DOT", app_id="A_DOT", subtype="bot_message")
        self.assertEqual(parse_command(value, POLICY).request_id, "request_001")

    def test_bot_profile_identity(self):
        value = event(user="U_DOT", bot_id="B_DOT", bot_profile={"app_id": "A_DOT"})
        self.assertEqual(parse_command(value, POLICY).action, "run")

    def test_reject_spoofed_owner_on_bot(self):
        with self.assertRaises(Rejected):
            parse_command(event(bot_id="B_OTHER", app_id="A_DOT"), POLICY)

    def test_dot_requires_exact_user_even_with_matching_bot_and_app(self):
        for user in ("U_OWNER", "U_OTHER", None, ""):
            with self.subTest(user=user), self.assertRaises(Rejected):
                parse_command(event(user=user, bot_id="B_DOT", app_id="A_DOT"), POLICY)

    def test_full_sender_identity_matrix(self):
        for user in ("U_OWNER", "U_DOT", "U_OTHER", None):
            for bot in (None, "B_DOT", "B_WORKER", "B_OTHER"):
                for app in (None, "A_DOT", "A_OTHER"):
                    changes = {"user": user}
                    if bot is not None:
                        changes["bot_id"] = bot
                    if app is not None:
                        changes["app_id"] = app
                    allowed = (user == "U_OWNER" and bot is None and app is None) or \
                              (user == "U_DOT" and bot == "B_DOT" and app == "A_DOT")
                    with self.subTest(user=user, bot=bot, app=app):
                        if allowed:
                            self.assertEqual(parse_command(event(**changes), POLICY).action, "run")
                        else:
                            with self.assertRaises(Rejected):
                                parse_command(event(**changes), POLICY)

    def test_dot_user_alone_cannot_use_human_path(self):
        with self.assertRaises(Rejected):
            parse_command(event(user="U_DOT"), POLICY)

    def test_dot_conflicting_bot_profile_rejected(self):
        with self.assertRaises(Rejected):
            parse_command(event(user="U_DOT", bot_id="B_DOT", app_id="A_DOT",
                                bot_profile={"id": "B_OTHER", "app_id": "A_DOT"}), POLICY)

    def test_unresolved_policy_fails_closed(self):
        for field in ("team_id", "channel_id", "owner_user_id", "dot_user_id",
                      "dot_bot_id", "dot_app_id", "worker_bot_id", "worker_user_id"):
            with self.subTest(field=field), self.assertRaises(Rejected):
                replace(POLICY, **{field: None})

    def test_overlapping_identities_rejected(self):
        with self.assertRaises(Rejected):
            replace(POLICY, owner_user_id="U_DOT")
        with self.assertRaises(Rejected):
            replace(POLICY, worker_bot_id="B_DOT")

    def test_run_without_mention_is_rejected(self):
        value = event()
        value["event"]["text"] = value["event"]["text"].removeprefix("<@U_WORKER> ")
        with self.assertRaisesRegex(Rejected, "explicitly mention"):
            parse_command(value, POLICY)

    def test_wrong_fake_or_embedded_mentions_cannot_create_task(self):
        body = event()["event"]["text"][len(RUN_PREFIX):]
        for prefix in ("<@U_OTHER> bridge.v1 ", "@Elite Devin Bridge bridge.v1 ",
                       "> <@U_WORKER> bridge.v1 ", "<@U_WORKER>bridge.v1 ",
                       "<@U_WORKER|Elite Devin Bridge> bridge.v1 ", "<@U_WORKER> \u200bbridge.v1 "):
            with self.subTest(prefix=prefix), self.assertRaises(Rejected):
                parse_command(event(text=prefix + body), POLICY)
        payload = json.loads(body); payload["text"] = "<@U_WORKER> Please run"
        with self.assertRaises(Rejected):
            parse_command(event(text="bridge.v1 " + json.dumps(payload)), POLICY)

    def test_exact_mention_accepts_ascii_spacing(self):
        body = event()["event"]["text"][len(RUN_PREFIX):]
        self.assertEqual(parse_command(event(text="<@U_WORKER>\nbridge.v1 " + body), POLICY).action, "run")

    def test_app_mention_is_not_a_second_entry_point(self):
        with self.assertRaises(Rejected):
            parse_command(event(type="app_mention"), POLICY)

    def test_bound_control_can_optionally_mention_bridge(self):
        text = '<@U_WORKER> bridge.v1 {"request_id":"cancel_001","action":"cancel"}'
        self.assertEqual(parse_command(event(text=text, thread_ts="1699999999.000001"), POLICY).action, "cancel")

    def test_reject_worker_echo(self):
        with self.assertRaises(Rejected):
            parse_command(event(bot_id="B_WORKER", app_id="A_DOT"), POLICY)

    def test_reject_conflicting_bot_identity(self):
        with self.assertRaises(Rejected):
            parse_command(event(bot_id="B_DOT", app_id="A_DOT",
                                bot_profile={"app_id": "A_OTHER"}), POLICY)

    def test_reject_bot_metadata_on_human_path(self):
        with self.assertRaises(Rejected):
            parse_command(event(app_id="A_OTHER"), POLICY)

    def test_reject_slack_connect(self):
        value = event(); value["is_ext_shared_channel"] = True
        with self.assertRaises(Rejected):
            parse_command(value, POLICY)

    def test_reject_unauthorized_user(self):
        with self.assertRaises(Rejected):
            parse_command(event(user="U_OTHER"), POLICY)

    def test_reject_wrong_team(self):
        value = event(); value["team_id"] = "T_OTHER"
        with self.assertRaises(Rejected):
            parse_command(value, POLICY)

    def test_reject_wrong_channel(self):
        with self.assertRaises(Rejected):
            parse_command(event(channel="C_OTHER"), POLICY)

    def test_reject_message_edit(self):
        with self.assertRaises(Rejected):
            parse_command(event(subtype="message_changed"), POLICY)

    def test_reject_cwd_injection(self):
        value = event(); payload = json.loads(value["event"]["text"][len(RUN_PREFIX):])
        payload["cwd"] = "/home/user"
        value["event"]["text"] = RUN_PREFIX + json.dumps(payload)
        with self.assertRaises(Rejected):
            parse_command(value, POLICY)

    def test_reject_duplicate_json_keys(self):
        with self.assertRaises(Rejected):
            parse_command(event(text='bridge.v1 {"action":"run","action":"cancel"}'), POLICY)

    def test_reject_raw_invalid_unicode(self):
        with self.assertRaises(Rejected):
            parse_command(event(text="bridge.v1 " + chr(0xd800)), POLICY)

    def test_reject_invalid_event_identity(self):
        value = event(); value["event_id"] = chr(0xd800)
        with self.assertRaises(Rejected):
            parse_command(value, POLICY)

    def test_reject_escaped_invalid_unicode(self):
        value = event()
        value["event"]["text"] = RUN_PREFIX + '{"request_id":"request01","action":"run",' \
                                 '"workspace":"scratch","text":"\\ud800"}'
        with self.assertRaises(Rejected):
            parse_command(value, POLICY)

    def test_cancel_only_in_bound_thread(self):
        text = 'bridge.v1 {"request_id":"cancel_001","action":"cancel"}'
        with self.assertRaises(Rejected):
            parse_command(event(text=text), POLICY)
        command = parse_command(event(text=text, thread_ts="1699999999.000001"), POLICY)
        self.assertEqual(command.action, "cancel")

    def test_no_automatic_approval_action(self):
        with self.assertRaises(Rejected):
            parse_command(event(text='bridge.v1 {"request_id":"approve01","action":"approve"}'), POLICY)

    def test_no_unimplemented_client_capabilities(self):
        self.assertEqual(acp_initialize(1)["params"]["clientCapabilities"], {})

    def test_workspace_alias_resolution(self):
        self.assertEqual(acp_new_session(2, "scratch", {"scratch": "/sandbox/demo"})
                         ["params"]["cwd"], "/sandbox/demo")
        with self.assertRaises(Rejected):
            acp_new_session(2, "../../home/user", {"scratch": "/sandbox/demo"})

    def test_acp_requests(self):
        self.assertEqual(acp_prompt(3, "session-bound", "hello")["method"], "session/prompt")
        self.assertNotIn("id", acp_cancel("session-bound"))
        self.assertEqual(deny_permission(4)["result"]["outcome"]["outcome"], "cancelled")


if __name__ == "__main__":
    unittest.main()
