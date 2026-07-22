from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "codex_ax_eval.py"
SPEC = importlib.util.spec_from_file_location("codex_ax_eval", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
ax = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ax)


def event(timestamp: str, event_type: str, payload: dict) -> dict:
    return {"timestamp": timestamp, "type": event_type, "payload": payload}


def write_jsonl(path: Path, events: list[dict]) -> None:
    path.write_text("".join(json.dumps(item) + "\n" for item in events), encoding="utf-8")


def synthetic_rollout(with_followup: bool = True) -> list[dict]:
    events = [
        event(
            "2026-07-22T12:00:00Z",
            "session_meta",
            {
                "id": "agent-uuid",
                "agent_path": "/root/ax_a_01",
                "source": {"subagent": {"thread_spawn": {"agent_path": "/root/ax_a_01"}}},
            },
        ),
        # Forked parent history must not contribute to duration or tool counts.
        event("2026-07-22T12:00:00Z", "event_msg", {"type": "task_started"}),
        event(
            "2026-07-22T12:00:00Z",
            "response_item",
            {"type": "function_call", "name": "old_parent_call", "call_id": "old"},
        ),
        event("2026-07-22T12:00:00Z", "event_msg", {"type": "task_complete"}),
        event("2026-07-22T12:00:01Z", "event_msg", {"type": "task_started"}),
        event(
            "2026-07-22T12:00:02Z",
            "response_item",
            {
                "type": "agent_message",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Message Type: NEW_TASK\nTask name: /root/ax_a_01\nPayload:\nDo it",
                    }
                ],
            },
        ),
        event(
            "2026-07-22T12:00:03Z",
            "response_item",
            {
                "type": "custom_tool_call",
                "name": "exec",
                "call_id": "one",
                "status": "completed",
                "input": 'const r = await tools.exec_command({cmd:"cargo test",workdir:"/tmp"});',
            },
        ),
        event(
            "2026-07-22T12:00:04Z",
            "response_item",
            {
                "type": "custom_tool_call_output",
                "call_id": "one",
                "output": [
                    {"type": "input_text", "text": "Script completed\n"},
                    {"type": "input_text", "text": '{"exit_code":3,"output":"failed"}'},
                ],
            },
        ),
        event(
            "2026-07-22T12:00:05Z",
            "response_item",
            {
                "type": "function_call",
                "namespace": "collaboration",
                "name": "send_message",
                "call_id": "two",
                "arguments": '{"target":"/root"}',
            },
        ),
        event(
            "2026-07-22T12:00:06Z",
            "response_item",
            {"type": "function_call_output", "call_id": "two", "output": "{}"},
        ),
        event("2026-07-22T12:00:07Z", "event_msg", {"type": "task_complete"}),
    ]
    if with_followup:
        events.extend(
            [
                event("2026-07-22T12:00:08Z", "event_msg", {"type": "task_started"}),
                event(
                    "2026-07-22T12:00:09Z",
                    "response_item",
                    {
                        "type": "agent_message",
                        "content": [{"type": "input_text", "text": "Please fix the test"}],
                    },
                ),
                event("2026-07-22T12:00:11Z", "event_msg", {"type": "task_complete"}),
            ]
        )
    return events


class ExtractTests(unittest.TestCase):
    def test_extracts_active_codex_turns_and_scores(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.jsonl"
            write_jsonl(path, synthetic_rollout())
            metrics = ax.extract(path)

        self.assertEqual(metrics["agent_id"], "agent-uuid")
        self.assertEqual(metrics["agent_path"], "/root/ax_a_01")
        self.assertEqual(metrics["duration_sec"], 10.0)
        self.assertEqual(metrics["tool_calls"], 2)
        self.assertEqual(metrics["errors"], 1)
        self.assertEqual(metrics["interruptions"], 1)
        self.assertEqual(metrics["first_3_commands"], ["cargo test"])
        self.assertEqual(metrics["tool_breakdown"], {"collaboration.send_message": 1, "exec": 1})
        self.assertEqual(metrics["scores"], ax.score(metrics))

    def test_rejects_empty_rollout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "empty.jsonl"
            path.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "empty session log"):
                ax.extract(path)

    def test_reads_judgment_from_codex_final_message(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "judge.jsonl"
            write_jsonl(
                path,
                [
                    event(
                        "2026-07-22T12:01:00Z",
                        "response_item",
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": '{"success": false, "reason": "tests failed with exit 1"}',
                                }
                            ],
                        },
                    )
                ],
            )
            judgment = ax._judge_result(path)
        self.assertEqual(
            judgment,
            {"success": False, "reason": "tests failed with exit 1"},
        )


class PersistenceTests(unittest.TestCase):
    def test_prepares_isolated_non_overwriting_workspaces(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template = root / "template"
            template.mkdir()
            (template / "README.md").write_text("starter", encoding="utf-8")
            output = root / "runs"
            workspaces = ax.prepare_workspaces(template, output, 3)
            self.assertEqual([path.name for path in workspaces], ["agent-1", "agent-2", "agent-3"])
            self.assertEqual((workspaces[2] / "README.md").read_text(encoding="utf-8"), "starter")
            with self.assertRaises(FileExistsError):
                ax.prepare_workspaces(template, output, 3)

    def test_persists_valid_result_index_and_transcript(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            transcript = root / "agent.jsonl"
            judge = root / "judge.json"
            manifest = root / "manifest.json"
            write_jsonl(transcript, synthetic_rollout(with_followup=False))
            judge.write_text(
                json.dumps({"success": True, "reason": "cargo test ran in the transcript"}),
                encoding="utf-8",
            )
            manifest.write_text(
                json.dumps(
                    {
                        "tool": {
                            "slug": "lix-api-a",
                            "name": "Lix API A",
                            "version": "research-1",
                            "install": "included",
                        },
                        "task": "Implement a CSV plugin",
                        "round": "Candidate A",
                        "ts": "2026-07-22T12:00:00Z",
                        "config": {
                            "agent_model": "gpt-5.6-terra",
                            "agent_count": 1,
                            "temperature": 0,
                            "tools": ["exec"],
                            "mcp_servers": [],
                            "system_prompt": None,
                            "max_turns": 40,
                            "prompt_template": "{task} using {tool}",
                            "docs_included": False,
                            "overrides": {"agent_model": "Codex override"},
                        },
                        "agents": [
                            {"id": 1, "transcript": str(transcript), "judge": str(judge)}
                        ],
                    }
                ),
                encoding="utf-8",
            )

            round_dir = ax.persist_round(manifest, root / "results")
            result = json.loads((round_dir / "result.json").read_text(encoding="utf-8"))
            index = json.loads(
                (root / "results" / "lix-api-a" / "index.json").read_text(encoding="utf-8")
            )
            ax.validate_document(result, "result")
            ax.validate_document(index, "index")
            self.assertEqual(result["summary"]["success_rate"], 1)
            self.assertTrue((round_dir / "transcripts" / "agent-1.jsonl").is_file())
            self.assertEqual(index["rounds"][0]["name"], "candidate-a")
            with self.assertRaises(FileExistsError):
                ax.persist_round(manifest, root / "results")

    def test_schema_validator_rejects_extra_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "unexpected property"):
            ax.validate_document(
                {"schema": 1, "tool_slug": "valid", "rounds": [], "extra": True},
                "index",
            )


if __name__ == "__main__":
    unittest.main()
