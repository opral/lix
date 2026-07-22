#!/usr/bin/env python3
"""Deterministic ax-eval adapter for Codex rollout JSONL logs.

This is derived from ax-eval's ``scripts/extract_metrics.py``.  Codex records
tool calls as ``response_item`` events rather than Claude ``tool_use`` blocks,
so this adapter translates that event model while preserving the published
scoring formulas.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import statistics
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


HERE = Path(__file__).resolve().parent
RESULT_SCHEMA = HERE / "schemas" / "result.schema.json"
INDEX_SCHEMA = HERE / "schemas" / "index.schema.json"
DEFAULT_SESSIONS_ROOT = Path.home() / ".codex" / "sessions"
DEFAULT_RESULTS_ROOT = Path.home() / ".ax-eval"


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, 1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path}:{line_number}: invalid JSON: {error}") from error
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number}: event must be a JSON object")
            events.append(value)
    if not events:
        raise ValueError(f"{path}: empty session log")
    return events


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str):
            parts.append(text)
    return "\n".join(parts)


def _agent_metadata(events: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    for event in events:
        if event.get("type") != "session_meta":
            continue
        payload = event.get("payload") or {}
        source = payload.get("source") or {}
        is_subagent = isinstance(source, dict) and isinstance(source.get("subagent"), dict)
        if is_subagent or payload.get("agent_path"):
            agent_id = payload.get("id")
            agent_path = payload.get("agent_path")
            return (
                agent_id if isinstance(agent_id, str) else None,
                agent_path if isinstance(agent_path, str) else None,
            )
    payload = events[0].get("payload") or {}
    agent_id = payload.get("id")
    return (agent_id if isinstance(agent_id, str) else None, None)


def _agent_metadata_from_path(path: Path) -> tuple[str | None, str | None]:
    """Read only the rollout preamble; session metadata is always emitted first."""

    with path.open(encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, 1):
            if line_number > 32:
                break
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                return (None, None)
            if isinstance(event, dict):
                agent_id, agent_path = _agent_metadata([event])
                if agent_path is not None:
                    return (agent_id, agent_path)
    return (None, None)


def _is_task_envelope(event: dict[str, Any], agent_path: str | None) -> bool:
    if event.get("type") != "response_item":
        return False
    payload = event.get("payload") or {}
    if payload.get("type") != "agent_message":
        return False
    text = _content_text(payload.get("content"))
    if "Message Type: NEW_TASK" not in text:
        return False
    return agent_path is None or f"Task name: {agent_path}" in text


def _active_events(events: list[dict[str, Any]], agent_path: str | None) -> list[dict[str, Any]]:
    """Remove forked parent history from a Codex subagent rollout.

    A subagent rollout can contain compacted parent turns before its NEW_TASK
    envelope.  The actual subagent turn starts at the closest preceding
    ``task_started`` event.  ``fork_turns=none`` logs also contain this envelope.
    """

    envelope_index = next(
        (index for index, event in enumerate(events) if _is_task_envelope(event, agent_path)),
        None,
    )
    if envelope_index is None:
        return events
    starts = [
        index
        for index, event in enumerate(events[: envelope_index + 1])
        if event.get("type") == "event_msg"
        and (event.get("payload") or {}).get("type") == "task_started"
    ]
    return events[starts[-1] :] if starts else events[envelope_index:]


def _tool_name(payload: dict[str, Any]) -> str:
    name = payload.get("name")
    name = name if isinstance(name, str) and name else "unknown"
    namespace = payload.get("namespace")
    if isinstance(namespace, str) and namespace:
        return f"{namespace}.{name}"
    return name


_DOUBLE_QUOTED_CMD = re.compile(r"[\"']?cmd[\"']?\s*:\s*(\"(?:\\.|[^\"\\])*\")")


def _extract_shell_command(payload: dict[str, Any]) -> str | None:
    payload_type = payload.get("type")
    if payload_type == "custom_tool_call" and payload.get("name") == "exec":
        source = payload.get("input")
        if not isinstance(source, str):
            return None
        match = _DOUBLE_QUOTED_CMD.search(source)
        if match:
            try:
                value = json.loads(match.group(1))
            except json.JSONDecodeError:
                return None
            return value if isinstance(value, str) else None
    if payload_type == "function_call":
        arguments = payload.get("arguments")
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                return None
        if isinstance(arguments, dict):
            value = arguments.get("cmd", arguments.get("command"))
            return value if isinstance(value, str) else None
    return None


def _embedded_failure(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, dict):
        if value.get("isError") is True or value.get("is_error") is True:
            return True
        exit_code = value.get("exit_code")
        if isinstance(exit_code, int) and not isinstance(exit_code, bool) and exit_code != 0:
            return True
        return any(_embedded_failure(child) for child in value.values())
    if isinstance(value, list):
        return any(_embedded_failure(child) for child in value)
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    if stripped.startswith(("Script failed\n", "Tool execution failed", "Tool call failed")):
        return True
    if stripped.startswith(("{", "[")):
        try:
            return _embedded_failure(json.loads(stripped))
        except json.JSONDecodeError:
            return False
    return False


def _followup_text(events: list[dict[str, Any]], start: int, stop: int) -> str:
    for event in events[start:stop]:
        payload = event.get("payload") or {}
        if event.get("type") == "response_item":
            payload_type = payload.get("type")
            if payload_type == "agent_message":
                text = _content_text(payload.get("content"))
                if text:
                    return text
            if payload_type == "message" and payload.get("role") == "user":
                text = _content_text(payload.get("content"))
                if text:
                    return text
        if event.get("type") == "event_msg" and payload.get("type") == "user_message":
            text = payload.get("message")
            if isinstance(text, str) and text:
                return text
    return "turn follow-up"


def extract(path: Path) -> dict[str, Any]:
    events = _read_jsonl(path)
    agent_id, agent_path = _agent_metadata(events)
    events = _active_events(events, agent_path)

    timestamps = [event["timestamp"] for event in events if isinstance(event.get("timestamp"), str)]
    if not timestamps:
        raise ValueError(f"{path}: no timestamps in active session events")
    parsed_timestamps = [_parse_ts(timestamp) for timestamp in timestamps]
    duration_sec = round((max(parsed_timestamps) - min(parsed_timestamps)).total_seconds(), 1)

    calls: list[dict[str, Any]] = []
    outputs: dict[str, Any] = {}
    task_starts: list[int] = []
    for index, event in enumerate(events):
        event_type = event.get("type")
        payload = event.get("payload") or {}
        payload_type = payload.get("type")
        if event_type == "event_msg" and payload_type == "task_started":
            task_starts.append(index)
        if event_type != "response_item":
            continue
        if payload_type in ("custom_tool_call", "function_call"):
            calls.append(payload)
        elif payload_type in ("custom_tool_call_output", "function_call_output"):
            call_id = payload.get("call_id")
            if isinstance(call_id, str):
                outputs[call_id] = payload.get("output")

    errors = 0
    breakdown: Counter[str] = Counter()
    commands: list[str] = []
    for call in calls:
        breakdown[_tool_name(call)] += 1
        command = _extract_shell_command(call)
        if command is not None:
            commands.append(command)
        failed_status = call.get("status") in ("failed", "error", "cancelled")
        call_id = call.get("call_id")
        failed_output = isinstance(call_id, str) and _embedded_failure(outputs.get(call_id))
        if failed_status or failed_output:
            errors += 1

    interruption_details: list[str] = []
    for ordinal, start in enumerate(task_starts[1:], 1):
        stop = task_starts[ordinal + 1] if ordinal + 1 < len(task_starts) else len(events)
        text = _followup_text(events, start, stop).replace("\n", " ").strip()
        interruption_details.append(f"text-followup: {text[:80]}")

    metrics = {
        "agent_id": agent_id,
        "agent_path": agent_path,
        "source": str(path),
        "duration_sec": duration_sec,
        "tool_calls": len(calls),
        "errors": errors,
        "interruptions": len(interruption_details),
        "interruption_details": interruption_details,
        "first_3_commands": commands[:3],
        "tool_breakdown": dict(sorted(breakdown.items())),
    }
    metrics["scores"] = score(metrics)
    return metrics


def score(metrics: dict[str, Any]) -> dict[str, int]:
    clamp = lambda value: max(0.0, min(100.0, value))
    friction = clamp(100 - metrics["interruptions"] * 14)
    speed = clamp(100 - ((metrics["duration_sec"] - 30) / 570) * 100)
    efficiency = clamp(100 - ((metrics["tool_calls"] - 1) / 39) * 100)
    error_recovery = clamp(100 - (metrics["errors"] / 15) * 100)
    final = (
        0.30 * friction
        + 0.25 * speed
        + 0.20 * efficiency
        + 0.25 * error_recovery
    )
    return {
        "friction": round(friction),
        "speed": round(speed),
        "efficiency": round(efficiency),
        "errorRecovery": round(error_recovery),
        "final": round(final),
    }


def _judge_result(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    try:
        direct = json.loads(raw)
    except json.JSONDecodeError:
        direct = None
    candidates: list[str] = []
    if (
        isinstance(direct, dict)
        and isinstance(direct.get("success"), bool)
        and isinstance(direct.get("reason"), str)
    ):
        candidates.append(raw)
    else:
        try:
            events = _read_jsonl(path)
        except ValueError:
            events = []
        for event in events:
            payload = event.get("payload") or {}
            if event.get("type") == "response_item":
                if payload.get("type") == "message" and payload.get("role") == "assistant":
                    candidates.append(_content_text(payload.get("content")))
                elif payload.get("type") == "agent_message":
                    candidates.append(_content_text(payload.get("content")))
            elif event.get("type") == "event_msg" and payload.get("type") == "agent_message":
                message = payload.get("message")
                if isinstance(message, str):
                    candidates.append(message)
        candidates.append(raw)

    for candidate in reversed(candidates):
        for line in reversed(candidate.splitlines()):
            line = line.strip()
            if line in ("```", "```json") or not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (
                isinstance(value, dict)
                and isinstance(value.get("success"), bool)
                and isinstance(value.get("reason"), str)
            ):
                return {"success": value["success"], "reason": value["reason"]}
    raise ValueError(f"{path}: no valid judge {{success, reason}} JSON found")


def _matches_type(value: Any, expected: str) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "null":
        return value is None
    raise ValueError(f"unsupported schema type {expected!r}")


def _validate_schema(value: Any, schema: dict[str, Any], location: str = "$") -> list[str]:
    errors: list[str] = []
    expected = schema.get("type")
    if expected is not None:
        types = expected if isinstance(expected, list) else [expected]
        if not any(_matches_type(value, item) for item in types):
            return [f"{location}: expected {' or '.join(types)}, got {type(value).__name__}"]
    if "const" in schema and value != schema["const"]:
        errors.append(f"{location}: expected constant {schema['const']!r}")
    if isinstance(value, dict):
        required = schema.get("required", [])
        for key in required:
            if key not in value:
                errors.append(f"{location}: missing required property {key!r}")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            for key in value:
                if key not in properties:
                    errors.append(f"{location}: unexpected property {key!r}")
        for key, child in value.items():
            child_schema = properties.get(key)
            if isinstance(child_schema, dict):
                errors.extend(_validate_schema(child, child_schema, f"{location}.{key}"))
    elif isinstance(value, list):
        max_items = schema.get("maxItems")
        if isinstance(max_items, int) and len(value) > max_items:
            errors.append(f"{location}: has {len(value)} items, maximum is {max_items}")
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, child in enumerate(value):
                errors.extend(_validate_schema(child, item_schema, f"{location}[{index}]"))
    elif isinstance(value, str):
        pattern = schema.get("pattern")
        if isinstance(pattern, str) and re.search(pattern, value) is None:
            errors.append(f"{location}: does not match /{pattern}/")
        if schema.get("format") == "date-time":
            try:
                _parse_ts(value)
            except ValueError:
                errors.append(f"{location}: invalid date-time {value!r}")
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if minimum is not None and value < minimum:
            errors.append(f"{location}: {value} is below minimum {minimum}")
        if maximum is not None and value > maximum:
            errors.append(f"{location}: {value} exceeds maximum {maximum}")
    return errors


def validate_document(document: Any, kind: str) -> None:
    if kind not in ("result", "index"):
        raise ValueError(f"unknown schema kind {kind!r}")
    schema_path = RESULT_SCHEMA if kind == "result" else INDEX_SCHEMA
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = _validate_schema(document, schema)
    if errors:
        raise ValueError("schema validation failed:\n  - " + "\n  - ".join(errors))


def _percentile(values: Iterable[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot compute a percentile of an empty cohort")
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _summary(agents: list[dict[str, Any]]) -> dict[str, Any]:
    finals = [agent["scores"]["final"] for agent in agents]
    first_commands = [
        agent["first_3_commands"][0] if agent["first_3_commands"] else "<none>"
        for agent in agents
    ]
    counts = Counter(first_commands)
    common = max(counts, key=lambda command: (counts[command], -first_commands.index(command)))
    return {
        "success_rate": sum(agent["success"] for agent in agents) / len(agents),
        "median_final": statistics.median(finals),
        "p25_final": _percentile(finals, 0.25),
        "p75_final": _percentile(finals, 0.75),
        "common_first_command": f"{common} ({counts[common]}/{len(agents)})",
    }


def _atomic_json(path: Path, document: Any) -> None:
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")


def persist_round(manifest_path: Path, output_root: Path) -> Path:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    tool = manifest["tool"]
    task = manifest["task"]
    round_name = _slugify(manifest["round"])
    if not round_name:
        raise ValueError("round slug is empty")
    timestamp = manifest.get("ts") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    _parse_ts(timestamp)
    config = manifest["config"]
    entries = manifest["agents"]
    if not isinstance(entries, list) or not entries:
        raise ValueError("manifest agents must be a non-empty array")
    agent_ids = [entry.get("id") for entry in entries if isinstance(entry, dict)]
    if len(agent_ids) != len(entries) or any(
        not isinstance(agent_id, int) or isinstance(agent_id, bool) or agent_id < 1
        for agent_id in agent_ids
    ):
        raise ValueError("every manifest agent must have a positive integer id")
    if len(set(agent_ids)) != len(agent_ids):
        raise ValueError("manifest agent ids must be unique")

    agents: list[dict[str, Any]] = []
    transcript_sources: list[Path] = []
    for entry in sorted(entries, key=lambda item: item["id"]):
        transcript = Path(entry["transcript"]).expanduser().resolve()
        judge = Path(entry["judge"]).expanduser().resolve()
        metrics = extract(transcript)
        judgment = _judge_result(judge)
        agents.append(
            {
                "id": entry["id"],
                "success": judgment["success"],
                "success_reason": judgment["reason"],
                "duration_sec": metrics["duration_sec"],
                "tool_calls": metrics["tool_calls"],
                "interruptions": metrics["interruptions"],
                "interruption_details": metrics["interruption_details"],
                "errors": metrics["errors"],
                "scores": metrics["scores"],
                "first_3_commands": metrics["first_3_commands"],
            }
        )
        transcript_sources.append(transcript)

    result = {
        "schema": 1,
        "tool": tool,
        "task": task,
        "round": round_name,
        "ts": timestamp,
        "config": config,
        "agents": agents,
        "summary": _summary(agents),
    }
    if config.get("agent_count") != len(agents):
        raise ValueError(
            f"config.agent_count is {config.get('agent_count')}, but manifest has {len(agents)} agents"
        )
    validate_document(result, "result")

    tool_dir = output_root / tool["slug"]
    directory_name = f"{timestamp}_{round_name}"
    round_dir = tool_dir / directory_name
    if round_dir.exists():
        raise FileExistsError(f"refusing to overwrite existing round {round_dir}")

    index_path = tool_dir / "index.json"
    if index_path.exists():
        index = json.loads(index_path.read_text(encoding="utf-8"))
        validate_document(index, "index")
        if index["tool_slug"] != tool["slug"]:
            raise ValueError("existing index tool_slug does not match manifest")
    else:
        index = {"schema": 1, "tool_slug": tool["slug"], "rounds": []}

    transcripts_dir = round_dir / "transcripts"
    transcripts_dir.mkdir(parents=True)
    for agent, source in zip(agents, transcript_sources):
        shutil.copyfile(source, transcripts_dir / f"agent-{agent['id']}.jsonl")
    _atomic_json(round_dir / "result.json", result)

    index["rounds"].insert(
        0,
        {
            "ts": timestamp,
            "name": round_name,
            "dir": directory_name,
            "median_final": result["summary"]["median_final"],
            "success_rate": result["summary"]["success_rate"],
        },
    )
    validate_document(index, "index")
    _atomic_json(index_path, index)
    return round_dir


def prepare_workspaces(template: Path, output: Path, count: int) -> list[Path]:
    """Copy one small starter template into isolated, non-overwriting workspaces."""

    if count < 1:
        raise ValueError("workspace count must be at least one")
    template = template.resolve()
    output = output.resolve()
    if not template.is_dir():
        raise ValueError(f"template is not a directory: {template}")
    if output.exists():
        raise FileExistsError(f"refusing to overwrite workspace root {output}")
    output.mkdir(parents=True)
    workspaces: list[Path] = []
    for agent_id in range(1, count + 1):
        destination = output / f"agent-{agent_id}"
        shutil.copytree(template, destination, symlinks=True)
        workspaces.append(destination)
    return workspaces


def _resolve_round(root: Path, tool_slug: str, value: str) -> dict[str, Any]:
    index_path = root / tool_slug / "index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    validate_document(index, "index")
    match = next(
        (entry for entry in index["rounds"] if entry["dir"] == value or entry["name"] == value),
        None,
    )
    if match is None:
        raise ValueError(f"round {value!r} not found for {tool_slug}")
    result = json.loads((root / tool_slug / match["dir"] / "result.json").read_text(encoding="utf-8"))
    validate_document(result, "result")
    return result


def _median_score(result: dict[str, Any], field: str) -> float:
    return statistics.median(agent["scores"][field] for agent in result["agents"])


def _format_delta(value: float, suffix: str = "") -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:g}{suffix}"


def compare_rounds(root: Path, tool_slug: str, before_name: str, after_name: str) -> str:
    before = _resolve_round(root, tool_slug, before_name)
    after = _resolve_round(root, tool_slug, after_name)
    before_success = before["summary"]["success_rate"] * 100
    after_success = after["summary"]["success_rate"] * 100
    rows: list[tuple[str, Any, Any, str]] = [
        (
            "success rate",
            f"{before_success:g}%",
            f"{after_success:g}%",
            _format_delta(after_success - before_success, "pp"),
        )
    ]
    fields = [
        ("median final", "final"),
        ("median friction", "friction"),
        ("median speed", "speed"),
        ("median efficiency", "efficiency"),
        ("median errorRec", "errorRecovery"),
    ]
    for label, field in fields:
        left = _median_score(before, field)
        right = _median_score(after, field)
        rows.append((label, f"{left:g}", f"{right:g}", _format_delta(right - left)))
    rows.append(
        (
            "common first cmd",
            before["summary"]["common_first_command"],
            after["summary"]["common_first_command"],
            "shifted",
        )
    )
    lines = [
        f"## {tool_slug}: {before['round']} → {after['round']}",
        "",
        f"| metric | {before['round']} | {after['round']} | delta |",
        "|---|---:|---:|---:|",
    ]
    lines.extend(f"| {label} | {left} | {right} | {delta} |" for label, left, right, delta in rows)
    lines.extend(
        [
            "",
            f"note: N={len(before['agents'])} and N={len(after['agents'])}. "
            "Treat deltas <10pp / <10pts as noise; re-run with N=25+ before shipping.",
        ]
    )
    return "\n".join(lines)


def judge_prompt(task: str, transcript: Path) -> str:
    return f"""You are an ax-eval judge. Read this agent's session log and decide if they completed the task.

Task: {task}

Session log: {transcript.resolve()}

The log is one JSON event per line — user messages, assistant messages with tool_use blocks, tool_result blocks. Read it end-to-end. Look at the actual tool calls and their results, not just the agent's self-narration in the final message (agents lie about success).

Return only this JSON on the final line, no other text:
{{"success": true|false, "reason": "<one short sentence citing concrete evidence from the log>"}}"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    extract_parser = commands.add_parser("extract", help="extract and score Codex rollout metrics")
    extract_parser.add_argument("transcripts", nargs="+", type=Path)

    find_parser = commands.add_parser("find", help="find a rollout by exact subagent path")
    find_parser.add_argument("--agent-path", required=True)
    find_parser.add_argument("--sessions-root", type=Path, default=DEFAULT_SESSIONS_ROOT)

    persist_parser = commands.add_parser("persist", help="persist one schema-validated ax-eval round")
    persist_parser.add_argument("manifest", type=Path)
    persist_parser.add_argument("--output-root", type=Path, default=DEFAULT_RESULTS_ROOT)

    workspace_parser = commands.add_parser(
        "prepare-workspaces", help="copy an evaluation starter into isolated agent workspaces"
    )
    workspace_parser.add_argument("--template", required=True, type=Path)
    workspace_parser.add_argument("--output", required=True, type=Path)
    workspace_parser.add_argument("--count", required=True, type=int)

    validate_parser = commands.add_parser("validate", help="validate result.json or index.json")
    validate_parser.add_argument("document", type=Path)
    validate_parser.add_argument("--kind", choices=("auto", "result", "index"), default="auto")

    judge_parser = commands.add_parser("judge-prompt", help="print the canonical judge prompt")
    judge_parser.add_argument("--task", required=True)
    judge_parser.add_argument("--transcript", required=True, type=Path)

    list_parser = commands.add_parser("list", help="list persisted tools or rounds")
    list_parser.add_argument("tool_slug", nargs="?")
    list_parser.add_argument("--output-root", type=Path, default=DEFAULT_RESULTS_ROOT)

    compare_parser = commands.add_parser("compare", help="compare two persisted rounds")
    compare_parser.add_argument("tool_slug")
    compare_parser.add_argument("round_a")
    compare_parser.add_argument("round_b")
    compare_parser.add_argument("--output-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "extract":
            for transcript in args.transcripts:
                print(json.dumps(extract(transcript)))
        elif args.command == "find":
            found: list[Path] = []
            for path in sorted(args.sessions_root.rglob("rollout-*.jsonl")):
                try:
                    _, agent_path = _agent_metadata_from_path(path)
                except OSError:
                    continue
                if agent_path == args.agent_path:
                    found.append(path)
            if not found:
                raise ValueError(f"no rollout found for {args.agent_path}")
            for path in found:
                print(path)
        elif args.command == "persist":
            print(persist_round(args.manifest, args.output_root))
        elif args.command == "prepare-workspaces":
            workspaces = prepare_workspaces(args.template, args.output, args.count)
            print(json.dumps([str(path) for path in workspaces]))
        elif args.command == "validate":
            document = json.loads(args.document.read_text(encoding="utf-8"))
            kind = args.kind
            if kind == "auto":
                kind = "result" if "agents" in document else "index"
            validate_document(document, kind)
            print(f"valid {kind}: {args.document}")
        elif args.command == "judge-prompt":
            print(judge_prompt(args.task, args.transcript))
        elif args.command == "list":
            if args.tool_slug:
                index_path = args.output_root / args.tool_slug / "index.json"
                index = json.loads(index_path.read_text(encoding="utf-8"))
                validate_document(index, "index")
                for entry in index["rounds"]:
                    print(
                        f"{entry['ts']}\t{entry['name']}\t"
                        f"success={entry['success_rate']:g}\tmedian={entry['median_final']:g}"
                    )
            else:
                for index_path in sorted(args.output_root.glob("*/index.json")):
                    index = json.loads(index_path.read_text(encoding="utf-8"))
                    validate_document(index, "index")
                    latest = index["rounds"][0] if index["rounds"] else None
                    latest_score = latest["median_final"] if latest else "n/a"
                    print(f"{index['tool_slug']}\trounds={len(index['rounds'])}\tlatest={latest_score}")
        elif args.command == "compare":
            print(compare_rounds(args.output_root, args.tool_slug, args.round_a, args.round_b))
        return 0
    except (OSError, KeyError, TypeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
