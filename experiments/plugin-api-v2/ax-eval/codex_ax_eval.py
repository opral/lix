#!/usr/bin/env python3
"""Run and persist reproducible ax-eval cohorts with ``codex exec --json``.

The tested prompt and judge intent come from ax-eval v2.  Codex JSONL uses a
different event model and does not include timestamps, so this harness keeps
stdout byte-for-byte and records process timing in an adjacent metadata file.
All scores are then computed deterministically from those retained artifacts.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import fcntl
import hashlib
import json
import math
import os
import re
import signal
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence, TypeVar


HERE = Path(__file__).resolve().parent
RESULT_SCHEMA = HERE / "schemas" / "result.schema.json"
INDEX_SCHEMA = HERE / "schemas" / "index.schema.json"
DEFAULT_SESSIONS_ROOT = Path.home() / ".codex" / "sessions"
DEFAULT_RESULTS_ROOT = Path.home() / ".ax-eval"
PROMPT_TEMPLATE = "{task} using {tool}"
PINNED_MODEL = "claude-opus-4-7"
PINNED_TOOLS = ["Bash", "Read", "Write", "Edit", "Glob", "Grep"]
PINNED_MAX_TURNS = 40
CODEX_TOOLS = ["shell", "apply_patch"]
TOOL_ITEM_TYPES = frozenset(
    {
        "command_execution",
        "file_change",
        "mcp_tool_call",
        "web_search",
        "image_view",
        "image_generation",
        "dynamic_tool_call",
        "collab_agent_tool_call",
        "subagent_activity",
        "sleep",
    }
)
FAILED_STATUSES = frozenset({"failed", "error", "cancelled", "canceled", "interrupted"})

_ACTIVE_PROCESSES: set[subprocess.Popen[bytes]] = set()
_ACTIVE_PROCESSES_LOCK = threading.Lock()
_CANCEL_EVENT = threading.Event()
T = TypeVar("T")


class ProcessRecord:
    def __init__(
        self,
        *,
        ordinal: int,
        kind: str,
        transcript: Path,
        stderr: Path,
        final_message: Path,
        metadata: Path,
        duration_sec: float,
        exit_code: int,
        timed_out: bool,
    ) -> None:
        self.ordinal = ordinal
        self.kind = kind
        self.transcript = transcript
        self.stderr = stderr
        self.final_message = final_message
        self.metadata = metadata
        self.duration_sec = duration_sec
        self.exit_code = exit_code
        self.timed_out = timed_out


class OwnedWorktrees:
    def __init__(self, *, repo: Path, root: Path, marker_token: str, paths: list[Path]) -> None:
        self.repo = repo
        self.root = root
        self.marker_token = marker_token
        self.paths = paths

    def cleanup(self) -> list[str]:
        """Remove only worktrees proven to be children of this owned temp root."""

        warnings: list[str] = []
        marker = self.root / ".ax-eval-owned.json"
        try:
            marker_data = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            return [f"refusing cleanup: ownership marker is unreadable: {error}"]
        if marker_data.get("token") != self.marker_token:
            return ["refusing cleanup: ownership marker token changed"]

        root = self.root.resolve()
        for path in reversed(self.paths):
            resolved = path.resolve()
            if resolved.parent != root:
                warnings.append(f"refusing cleanup outside owned root: {resolved}")
                continue
            completed = subprocess.run(
                [
                    "git",
                    "-C",
                    str(self.repo),
                    "-c",
                    "core.hooksPath=/dev/null",
                    "worktree",
                    "remove",
                    "--force",
                    str(resolved),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                text=True,
            )
            if completed.returncode != 0:
                detail = completed.stderr.strip() or completed.stdout.strip()
                warnings.append(f"could not remove owned worktree {resolved}: {detail}")

        if not warnings:
            shutil.rmtree(root)
        return warnings


def _parse_ts(value: str) -> datetime:
    if re.fullmatch(
        r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})",
        value,
    ) is None:
        raise ValueError(f"not an RFC3339 date-time: {value!r}")
    normalized = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"date-time has no UTC offset: {value!r}")
    return parsed


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


def _event_duration(events: Sequence[dict[str, Any]]) -> float | None:
    timestamps = [event["timestamp"] for event in events if isinstance(event.get("timestamp"), str)]
    if not timestamps:
        return None
    parsed_timestamps = [_parse_ts(timestamp) for timestamp in timestamps]
    return round((max(parsed_timestamps) - min(parsed_timestamps)).total_seconds(), 1)


def _sidecar_duration(path: Path) -> float | None:
    metadata_path = path.with_suffix(".meta.json")
    if not metadata_path.is_file():
        return None
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    value = metadata.get("duration_sec")
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
        return round(float(value), 1)
    raise ValueError(f"{metadata_path}: duration_sec must be a non-negative number")


def _extract_rollout_metrics(path: Path, events: list[dict[str, Any]]) -> dict[str, Any]:
    agent_id, agent_path = _agent_metadata(events)
    events = _active_events(events, agent_path)

    duration_sec = _event_duration(events)
    if duration_sec is None:
        raise ValueError(f"{path}: no timestamps in active session events")

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


def _codex_item_failed(item: dict[str, Any]) -> bool:
    status = item.get("status")
    if isinstance(status, str) and status.lower() in FAILED_STATUSES:
        return True
    exit_code = item.get("exit_code")
    if isinstance(exit_code, int) and not isinstance(exit_code, bool) and exit_code != 0:
        return True
    for key in ("error", "result", "output"):
        if key in item and _embedded_failure(item[key]):
            return True
    return False


def _command_text(item: dict[str, Any]) -> str | None:
    command = item.get("command")
    if isinstance(command, str):
        return command
    if isinstance(command, list) and all(isinstance(part, str) for part in command):
        return " ".join(command)
    return None


def _extract_exec_metrics(
    path: Path,
    events: list[dict[str, Any]],
    duration_sec: float | None,
) -> dict[str, Any]:
    """Extract one tool call per unique Codex ``item.*`` tool item."""

    if duration_sec is None:
        duration_sec = _sidecar_duration(path)
    if duration_sec is None:
        duration_sec = _event_duration(events)
    if duration_sec is None:
        raise ValueError(
            f"{path}: codex exec JSONL has no timestamps; expected {path.with_suffix('.meta.json')}"
        )

    items: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for index, event in enumerate(events):
        event_type = event.get("type")
        item = event.get("item")
        if not isinstance(event_type, str) or not event_type.startswith("item."):
            continue
        if not isinstance(item, dict):
            continue
        item_id = item.get("id")
        key = item_id if isinstance(item_id, str) and item_id else f"event-{index}"
        if key not in items:
            items[key] = {}
            order.append(key)
        items[key].update(item)

    calls = [items[key] for key in order if items[key].get("type") in TOOL_ITEM_TYPES]
    breakdown: Counter[str] = Counter()
    commands: list[str] = []
    errors = 0
    for call in calls:
        item_type = str(call["type"])
        breakdown[item_type] += 1
        if item_type == "command_execution":
            command = _command_text(call)
            if command is not None:
                commands.append(command)
        if _codex_item_failed(call):
            errors += 1

    thread_id = next(
        (
            event.get("thread_id")
            for event in events
            if event.get("type") == "thread.started" and isinstance(event.get("thread_id"), str)
        ),
        None,
    )
    metrics = {
        "agent_id": thread_id,
        "agent_path": None,
        "source": str(path),
        "duration_sec": round(float(duration_sec), 1),
        "tool_calls": len(calls),
        "errors": errors,
        # Each agent is exactly one non-interactive invocation.  The harness
        # never resumes it or supplies a follow-up user turn.
        "interruptions": 0,
        "interruption_details": [],
        "first_3_commands": commands[:3],
        "tool_breakdown": dict(sorted(breakdown.items())),
    }
    metrics["scores"] = score(metrics)
    return metrics


def extract(path: Path, duration_sec: float | None = None) -> dict[str, Any]:
    events = _read_jsonl(path)
    is_exec_stream = any(
        isinstance(event.get("type"), str)
        and (
            "." in event["type"]
            or event["type"] == "error"
        )
        for event in events
    )
    if is_exec_stream:
        return _extract_exec_metrics(path, events, duration_sec)
    return _extract_rollout_metrics(path, events)


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
    def is_judgment(value: Any) -> bool:
        return (
            isinstance(value, dict)
            and set(value) == {"success", "reason"}
            and isinstance(value.get("success"), bool)
            and isinstance(value.get("reason"), str)
            and bool(value["reason"].strip())
        )

    raw = path.read_text(encoding="utf-8")
    try:
        direct = json.loads(raw)
    except json.JSONDecodeError:
        direct = None
    candidates: list[str] = []
    if is_judgment(direct):
        candidates.append(raw)
    else:
        try:
            events = _read_jsonl(path)
        except ValueError:
            events = []
        for event in events:
            payload = event.get("payload") or {}
            item = event.get("item") or {}
            if (
                event.get("type") == "item.completed"
                and isinstance(item, dict)
                and item.get("type") == "agent_message"
                and isinstance(item.get("text"), str)
            ):
                candidates.append(item["text"])
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
            if is_judgment(value):
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
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(document, indent=2) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


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

    transcripts_dir = round_dir / "transcripts"
    transcripts_dir.mkdir(parents=True)
    for agent, source in zip(agents, transcript_sources):
        shutil.copyfile(source, transcripts_dir / f"agent-{agent['id']}.jsonl")
    _atomic_json(round_dir / "result.json", result)

    _update_index(
        tool_dir,
        {
            "ts": timestamp,
            "name": round_name,
            "dir": directory_name,
            "median_final": result["summary"]["median_final"],
            "success_rate": result["summary"]["success_rate"],
        },
    )
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


def _git(repo: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ValueError(f"git {' '.join(arguments)} failed: {detail}")
    return completed.stdout.strip()


def resolve_repo_revision(repo: Path, revision: str) -> tuple[Path, str]:
    repo = repo.expanduser().resolve()
    if not repo.is_dir():
        raise ValueError(f"repository is not a directory: {repo}")
    top_level_text = _git(repo, "rev-parse", "--show-toplevel")
    top_level = Path(top_level_text).resolve()
    commit = _git(
        top_level,
        "rev-parse",
        "--verify",
        "--end-of-options",
        f"{revision}^{{commit}}",
    ).splitlines()[0]
    if re.fullmatch(r"[0-9a-fA-F]{40,64}", commit) is None:
        raise ValueError(f"git returned an unexpected commit id: {commit!r}")
    return top_level, commit.lower()


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def create_worktrees(
    repo: Path,
    commit: str,
    count: int,
    tool_slug: str,
    forbidden_roots: Sequence[Path] = (),
) -> OwnedWorktrees:
    if count < 1:
        raise ValueError("agent count must be at least one")
    marker_token = uuid.uuid4().hex
    root = Path(tempfile.mkdtemp(prefix=f"ax-eval-{tool_slug}-")).resolve()
    for forbidden in forbidden_roots:
        if _paths_overlap(root, forbidden.resolve()):
            shutil.rmtree(root)
            raise ValueError(f"owned worktree root overlaps protected path: {root} vs {forbidden}")
    owned = OwnedWorktrees(repo=repo, root=root, marker_token=marker_token, paths=[])
    _atomic_json(
        root / ".ax-eval-owned.json",
        {"token": marker_token, "repo": str(repo), "commit": commit},
    )
    try:
        for ordinal in range(1, count + 1):
            path = root / f"agent-{ordinal}"
            # Track the intended path before Git starts so an interrupted or
            # partially successful add is still included in owned cleanup.
            owned.paths.append(path)
            completed = subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "-c",
                    "core.hooksPath=/dev/null",
                    "worktree",
                    "add",
                    "--detach",
                    str(path),
                    commit,
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                text=True,
            )
            if completed.returncode != 0:
                detail = completed.stderr.strip() or completed.stdout.strip()
                raise ValueError(f"could not create worktree {path}: {detail}")
    except BaseException:
        for warning in owned.cleanup():
            print(f"warning: {warning}", file=sys.stderr)
        raise
    return owned


def _kill_process_group(process_group: int, grace_seconds: float = 1.0) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process_group, signal.SIGTERM)
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process_group, signal.SIGKILL)


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        _kill_process_group(process.pid)
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=5)
    _kill_process_group(process.pid)


def _terminate_active_processes() -> None:
    with _ACTIVE_PROCESSES_LOCK:
        active = list(_ACTIVE_PROCESSES)
    for process in active:
        _terminate_process(process)


def _codex_command(
    *,
    codex_bin: str,
    cwd: Path,
    model: str,
    sandbox: str,
    final_message: Path,
    cargo_target_dir: Path,
    output_schema: Path | None,
    skip_git_repo_check: bool,
    temp_dir: Path | None,
) -> list[str]:
    command = [
        codex_bin,
        "exec",
        "--json",
        "--ephemeral",
        "--color",
        "never",
        "--model",
        model,
        "--sandbox",
        sandbox,
        "--cd",
        str(cwd),
        "--ignore-user-config",
        "--ignore-rules",
        "--strict-config",
        "--disable",
        "apps",
        "--disable",
        "hooks",
        "--disable",
        "goals",
        "--disable",
        "multi_agent",
        "--disable",
        "memories",
        "--disable",
        "remote_plugin",
        "--disable",
        "plugins",
        "--disable",
        "plugin_sharing",
        "--disable",
        "image_generation",
        "--disable",
        "browser_use",
        "--disable",
        "browser_use_external",
        "--disable",
        "browser_use_full_cdp_access",
        "--disable",
        "computer_use",
        "--disable",
        "tool_suggest",
        "--disable",
        "workspace_dependencies",
        "--disable",
        "auth_elicitation",
        "--disable",
        "skill_mcp_dependency_install",
        "--config",
        'web_search="disabled"',
        "--config",
        "mcp_servers={}",
        "--config",
        'approval_policy="never"',
        "--config",
        "sandbox_workspace_write.network_access=false",
        "--config",
        "sandbox_workspace_write.exclude_tmpdir_env_var=true",
        "--config",
        "sandbox_workspace_write.exclude_slash_tmp=true",
        "--config",
        'shell_environment_policy.inherit="core"',
        "--config",
        (
            "shell_environment_policy.set.CARGO_TARGET_DIR="
            + json.dumps(str(cargo_target_dir))
        ),
        "--output-last-message",
        str(final_message),
    ]
    if skip_git_repo_check:
        command.append("--skip-git-repo-check")
    if sandbox == "workspace-write":
        command.extend(["--add-dir", str(cargo_target_dir)])
        if temp_dir is not None:
            command.extend(
                [
                    "--add-dir",
                    str(temp_dir),
                    "--config",
                    "shell_environment_policy.set.TMPDIR=" + json.dumps(str(temp_dir)),
                ]
            )
    if output_schema is not None:
        command.extend(["--output-schema", str(output_schema)])
    # A dash makes stdin the entire prompt.  No wrapper text or newline is
    # introduced by this harness.
    command.append("-")
    return command


def _run_codex(
    *,
    ordinal: int,
    kind: str,
    cwd: Path,
    prompt: str,
    model: str,
    codex_bin: str,
    cargo_target_dir: Path,
    transcript: Path,
    stderr_path: Path,
    final_message: Path,
    metadata_path: Path,
    timeout_seconds: float,
    sandbox: str,
    output_schema: Path | None = None,
    skip_git_repo_check: bool = False,
    temp_dir: Path | None = None,
) -> ProcessRecord:
    transcript.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    final_message.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=False)
    command = _codex_command(
        codex_bin=codex_bin,
        cwd=cwd,
        model=model,
        sandbox=sandbox,
        final_message=final_message,
        cargo_target_dir=cargo_target_dir,
        output_schema=output_schema,
        skip_git_repo_check=skip_git_repo_check,
        temp_dir=temp_dir,
    )
    started_at = _iso_now()
    base_metadata = {
        "schema": 1,
        "ordinal": ordinal,
        "kind": kind,
        "status": "starting",
        "cwd": str(cwd),
        "command": command,
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "started_at": started_at,
        "cargo_target_dir": str(cargo_target_dir),
    }
    _atomic_json(metadata_path, base_metadata)
    started_ns = time.monotonic_ns()

    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(cargo_target_dir)
    if temp_dir is not None:
        environment["TMPDIR"] = str(temp_dir)
    process: subprocess.Popen[bytes] | None = None
    timed_out = False
    spawn_error: str | None = None
    run_error: BaseException | None = None
    exit_code = 127
    try:
        with transcript.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
            if _CANCEL_EVENT.is_set():
                raise InterruptedError("evaluation batch was cancelled before Codex started")
            try:
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    env=environment,
                    start_new_session=True,
                )
            except OSError as error:
                spawn_error = str(error)
                stderr_handle.write(f"could not start Codex: {error}\n".encode("utf-8"))
            if process is not None:
                with _ACTIVE_PROCESSES_LOCK:
                    _ACTIVE_PROCESSES.add(process)
                try:
                    if _CANCEL_EVENT.is_set():
                        raise InterruptedError("evaluation batch was cancelled as Codex started")
                    try:
                        process.communicate(input=prompt.encode("utf-8"), timeout=timeout_seconds)
                    except subprocess.TimeoutExpired:
                        timed_out = True
                        _terminate_process(process)
                        process.communicate()
                    exit_code = process.returncode if process.returncode is not None else 1
                    _kill_process_group(process.pid)
                except BaseException as error:
                    run_error = error
                    _terminate_process(process)
                    with contextlib.suppress(Exception):
                        process.communicate()
                    raise
                finally:
                    with _ACTIVE_PROCESSES_LOCK:
                        _ACTIVE_PROCESSES.discard(process)
            stdout_handle.flush()
            os.fsync(stdout_handle.fileno())
            stderr_handle.flush()
            os.fsync(stderr_handle.fileno())
    except BaseException as error:
        run_error = error
        raise
    finally:
        finished_at = _iso_now()
        duration_sec = round((time.monotonic_ns() - started_ns) / 1_000_000_000, 1)
        completed_metadata = {
            **base_metadata,
            "status": (
                "timed_out"
                if timed_out
                else ("spawn_failed" if spawn_error else ("failed" if run_error else "completed"))
            ),
            "finished_at": finished_at,
            "duration_sec": duration_sec,
            "exit_code": exit_code,
            "timed_out": timed_out,
        }
        if spawn_error is not None:
            completed_metadata["spawn_error"] = spawn_error
        _atomic_json(metadata_path, completed_metadata)

    return ProcessRecord(
        ordinal=ordinal,
        kind=kind,
        transcript=transcript,
        stderr=stderr_path,
        final_message=final_message,
        metadata=metadata_path,
        duration_sec=duration_sec,
        exit_code=exit_code,
        timed_out=timed_out,
    )


def _run_batched(
    inputs: Sequence[T],
    parallelism: int,
    runner: Callable[[T], ProcessRecord],
    label: str,
) -> list[ProcessRecord]:
    if parallelism < 1:
        raise ValueError("parallelism must be at least one")
    results: dict[int, ProcessRecord] = {}
    completed_count = 0
    for offset in range(0, len(inputs), parallelism):
        batch = inputs[offset : offset + parallelism]
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(batch))
        futures = {executor.submit(runner, item): item for item in batch}
        try:
            for future in concurrent.futures.as_completed(futures):
                record = future.result()
                results[record.ordinal] = record
                completed_count += 1
                state = "timeout" if record.timed_out else f"exit {record.exit_code}"
                print(
                    f"{label} {record.ordinal} done ({state}), "
                    f"{completed_count}/{len(inputs)} complete...",
                    flush=True,
                )
        except BaseException:
            _CANCEL_EVENT.set()
            _terminate_active_processes()
            for future in futures:
                future.cancel()
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
    return [results[index] for index in sorted(results)]


def canonical_prompt(task: str, tool: str) -> str:
    for label, value in (("task", task), ("tool", tool)):
        if not value.strip():
            raise ValueError(f"{label} must not be empty")
        if "\n" in value or "\r" in value:
            raise ValueError(f"{label} must be one line to preserve the canonical prompt")
    return f"{task} using {tool}"


def _result_config(
    model: str,
    agent_count: int,
    timeout_seconds: float,
    cargo_target_dir: Path,
) -> dict[str, Any]:
    return {
        "agent_model": model,
        "agent_count": agent_count,
        "temperature": 0,
        "tools": CODEX_TOOLS,
        "mcp_servers": [],
        "system_prompt": "Codex built-in system/developer instructions (implicit)",
        "max_turns": PINNED_MAX_TURNS,
        "prompt_template": PROMPT_TEMPLATE,
        "docs_included": False,
        "overrides": {
            "model": f"{PINNED_MODEL} is unavailable in the required Codex CLI; used {model}",
            "temperature": "codex exec exposes no temperature control; 0 is the pinned target but is not enforceable",
            "tools": (
                f"Claude allow-list {PINNED_TOOLS!r} is unavailable; used Codex built-in "
                "shell/apply_patch with web, apps, MCP, and subagents disabled"
            ),
            "system_prompt": "none is unavailable; codex exec uses its built-in system/developer instructions",
            "mode": "workspace-write plus the shared Cargo target replaced bypassPermissions",
            "max_turns": (
                f"codex exec exposes no max-turn limit; {PINNED_MAX_TURNS} is recorded but "
                f"the enforced bound is a {timeout_seconds:g}s process timeout"
            ),
            "log_format": "raw codex exec --json JSONL replaced Claude Code session JSONL",
            "duration_source": (
                "codex exec JSONL has no event timestamps; duration uses retained monotonic "
                "process timing metadata"
            ),
            "cargo_target_dir": f"all tested and judge processes inherited {cargo_target_dir}",
        },
    }


def _update_index(tool_dir: Path, entry: dict[str, Any]) -> None:
    tool_dir.mkdir(parents=True, exist_ok=True)
    index_path = tool_dir / "index.json"
    lock_path = tool_dir / ".index.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        if index_path.exists():
            index = json.loads(index_path.read_text(encoding="utf-8"))
            validate_document(index, "index")
        else:
            index = {"schema": 1, "tool_slug": tool_dir.name, "rounds": []}
        if index["tool_slug"] != tool_dir.name:
            raise ValueError("existing index tool_slug does not match output directory")
        if any(existing["dir"] == entry["dir"] for existing in index["rounds"]):
            raise ValueError(f"index already contains round directory {entry['dir']}")
        index["rounds"].insert(0, entry)
        index["rounds"].sort(
            key=lambda existing: (_parse_ts(existing["ts"]), existing["dir"]),
            reverse=True,
        )
        validate_document(index, "index")
        # The lock makes the schema-mandated index.json.tmp name safe even
        # when multiple harness processes finish at nearly the same time.
        _atomic_json(index_path, index)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def _resolve_cargo_target(repo: Path, requested: Path | None) -> Path:
    repo_target = (repo / "target").resolve()
    if requested is None:
        target = repo_target / "ax-eval"
    else:
        requested = requested.expanduser()
        target = requested.resolve() if requested.is_absolute() else (repo / requested).resolve()
    try:
        suffix = target.relative_to(repo_target)
    except ValueError as error:
        raise ValueError(
            f"--cargo-target-dir must be under the main repository target directory {repo_target}"
        ) from error
    git_relative = Path("target") / suffix
    tracked = _git(repo, "ls-files", "--", git_relative.as_posix())
    if tracked:
        raise ValueError(f"shared Cargo target contains tracked paths: {tracked.splitlines()[0]}")
    ignored = subprocess.run(
        ["git", "-C", str(repo), "check-ignore", "-q", "--no-index", "--", git_relative.as_posix()],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    if ignored.returncode != 0:
        detail = ignored.stderr.strip()
        suffix_text = f": {detail}" if detail else ""
        raise ValueError(f"shared Cargo target must be Git-ignored ({git_relative}){suffix_text}")
    target.mkdir(parents=True, exist_ok=True)
    return target


def _write_completed_result(
    *,
    round_dir: Path,
    output_root: Path,
    directory_name: str,
    timestamp: str,
    round_slug: str,
    tool_name: str,
    tool_slug: str,
    commit: str,
    task: str,
    model: str,
    timeout_seconds: float,
    cargo_target_dir: Path,
    agents: Sequence[ProcessRecord],
    judges: Sequence[ProcessRecord],
) -> dict[str, Any]:
    judge_by_id = {record.ordinal: record for record in judges}
    agent_results: list[dict[str, Any]] = []
    for record in sorted(agents, key=lambda item: item.ordinal):
        judge = judge_by_id[record.ordinal]
        metrics = extract(record.transcript, duration_sec=record.duration_sec)
        judgment_source = judge.final_message if judge.final_message.is_file() else judge.transcript
        judgment = _judge_result(judgment_source)
        agent_results.append(
            {
                "id": record.ordinal,
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
        outcome = "success" if judgment["success"] else "failure"
        print(
            f"agent {record.ordinal} judged {outcome}, "
            f"{len(agent_results)}/{len(agents)} complete...",
            flush=True,
        )

    result = {
        "schema": 1,
        "tool": {
            "slug": tool_slug,
            "name": tool_name,
            "version": commit,
            "install": f"detached git worktree at {commit}",
        },
        "task": task,
        "round": round_slug,
        "ts": timestamp,
        "config": _result_config(model, len(agent_results), timeout_seconds, cargo_target_dir),
        "agents": agent_results,
        "summary": _summary(agent_results),
    }
    validate_document(result, "result")
    _atomic_json(round_dir / "result.json", result)
    _update_index(
        output_root / tool_slug,
        {
            "ts": timestamp,
            "name": round_slug,
            "dir": directory_name,
            "median_final": result["summary"]["median_final"],
            "success_rate": result["summary"]["success_rate"],
        },
    )
    return result


def run_evaluation(args: argparse.Namespace) -> Path:
    _CANCEL_EVENT.clear()
    if args.agent_count < 1:
        raise ValueError("--agent-count must be at least one")
    if args.parallelism < 1:
        raise ValueError("--parallelism must be at least one")
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be greater than zero")
    if not args.model.strip() or "\n" in args.model or "\r" in args.model:
        raise ValueError("--model must be a non-empty single line")
    codex_path = shutil.which(args.codex_bin)
    if codex_path is None:
        raise ValueError(f"Codex executable not found: {args.codex_bin}")
    args.codex_bin = codex_path
    prompt = canonical_prompt(args.task, args.tool)
    tool_slug = _slugify(args.tool)
    round_slug = _slugify(args.round)
    if not tool_slug:
        raise ValueError("--tool produces an empty slug")
    if not round_slug:
        raise ValueError("--round produces an empty slug")

    repo, commit = resolve_repo_revision(args.repo, args.revision)
    cargo_target_dir = _resolve_cargo_target(repo, args.cargo_target_dir)
    output_root = args.output_root.expanduser().resolve()
    timestamp = _iso_now()
    directory_name = f"{timestamp}_{round_slug}"
    round_dir = output_root / tool_slug / directory_name
    if _paths_overlap(round_dir, cargo_target_dir):
        raise ValueError(
            "evaluation output and the agent-writable Cargo target must not overlap: "
            f"{round_dir} vs {cargo_target_dir}"
        )
    round_dir.mkdir(parents=True, exist_ok=False)
    judge_schema = round_dir / "judges" / "output.schema.json"
    _atomic_json(
        judge_schema,
        {
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "reason": {"type": "string", "minLength": 1},
            },
            "required": ["success", "reason"],
            "additionalProperties": False,
        },
    )
    invocation_path = round_dir / "invocation.json"
    invocation: dict[str, Any] = {
        "schema": 1,
        "status": "preparing",
        "repo": str(repo),
        "revision": args.revision,
        "commit": commit,
        "task": args.task,
        "tool": args.tool,
        "round": round_slug,
        "model": args.model,
        "codex_bin": args.codex_bin,
        "agent_count": args.agent_count,
        "parallelism": min(args.parallelism, args.agent_count),
        "timeout_seconds": args.timeout_seconds,
        "cargo_target_dir": str(cargo_target_dir),
        "started_at": timestamp,
    }
    _atomic_json(invocation_path, invocation)

    owned: OwnedWorktrees | None = None
    try:
        owned = create_worktrees(
            repo,
            commit,
            args.agent_count,
            tool_slug,
            forbidden_roots=(cargo_target_dir, round_dir),
        )
        invocation.update({"status": "running_agents", "worktree_root": str(owned.root)})
        _atomic_json(invocation_path, invocation)
        agent_inputs = list(enumerate(owned.paths, 1))

        def run_agent(item: tuple[int, Path]) -> ProcessRecord:
            ordinal, worktree = item
            base = round_dir / "transcripts" / f"agent-{ordinal}"
            return _run_codex(
                ordinal=ordinal,
                kind="tested-agent",
                cwd=worktree,
                prompt=prompt,
                model=args.model,
                codex_bin=args.codex_bin,
                cargo_target_dir=cargo_target_dir,
                transcript=base.with_suffix(".jsonl"),
                stderr_path=base.with_suffix(".stderr.log"),
                final_message=base.with_suffix(".final.txt"),
                metadata_path=base.with_suffix(".meta.json"),
                timeout_seconds=args.timeout_seconds,
                sandbox="workspace-write",
                temp_dir=owned.root / f"tmp-agent-{ordinal}",
            )

        agents = _run_batched(
            agent_inputs,
            min(args.parallelism, args.agent_count),
            run_agent,
            "agent",
        )
        invocation["status"] = "running_judges"
        _atomic_json(invocation_path, invocation)

        def run_judge(record: ProcessRecord) -> ProcessRecord:
            base = round_dir / "judges" / f"agent-{record.ordinal}"
            return _run_codex(
                ordinal=record.ordinal,
                kind="judge",
                cwd=round_dir,
                prompt=judge_prompt(args.task, record.transcript),
                model=args.model,
                codex_bin=args.codex_bin,
                cargo_target_dir=cargo_target_dir,
                transcript=base.with_suffix(".jsonl"),
                stderr_path=base.with_suffix(".stderr.log"),
                final_message=base.with_suffix(".final.json"),
                metadata_path=base.with_suffix(".meta.json"),
                timeout_seconds=args.timeout_seconds,
                sandbox="read-only",
                output_schema=judge_schema,
                skip_git_repo_check=True,
            )

        judges = _run_batched(
            agents,
            min(args.parallelism, args.agent_count),
            run_judge,
            "judge",
        )
        invocation["status"] = "persisting"
        _atomic_json(invocation_path, invocation)
        _write_completed_result(
            round_dir=round_dir,
            output_root=output_root,
            directory_name=directory_name,
            timestamp=timestamp,
            round_slug=round_slug,
            tool_name=args.tool,
            tool_slug=tool_slug,
            commit=commit,
            task=args.task,
            model=args.model,
            timeout_seconds=args.timeout_seconds,
            cargo_target_dir=cargo_target_dir,
            agents=agents,
            judges=judges,
        )
        invocation.update({"status": "complete", "finished_at": _iso_now()})
        _atomic_json(invocation_path, invocation)
        return round_dir
    except BaseException as error:
        failure = {
            "schema": 1,
            "status": "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
            "error_type": type(error).__name__,
            "error": str(error),
            "recorded_at": _iso_now(),
        }
        with contextlib.suppress(OSError):
            _atomic_json(round_dir / "failure.json", failure)
        invocation.update({"status": failure["status"], "finished_at": failure["recorded_at"]})
        with contextlib.suppress(OSError):
            _atomic_json(invocation_path, invocation)
        raise
    finally:
        _terminate_active_processes()
        cleanup_warnings: list[str] = []
        if owned is not None:
            try:
                cleanup_warnings.extend(owned.cleanup())
            except Exception as cleanup_error:
                cleanup_warnings.append(f"owned worktree cleanup raised: {cleanup_error}")
        for warning in cleanup_warnings:
            print(f"warning: {warning}", file=sys.stderr)
        invocation["cleanup"] = {
            "status": "warning" if cleanup_warnings else "complete",
            "warnings": cleanup_warnings,
        }
        with contextlib.suppress(OSError):
            _atomic_json(invocation_path, invocation)


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
    parser.add_argument("--repo", type=Path, help="repository whose commit will be evaluated")
    parser.add_argument("--revision", help="commit-ish used for every detached worktree")
    parser.add_argument("--agent-count", type=int, default=10, help="tested agents (default: 10)")
    parser.add_argument("--model", help="Codex model used for tested agents and judges")
    parser.add_argument("--task", help="single-line task in the canonical tested-agent prompt")
    parser.add_argument("--tool", help="single-line tool text in the canonical prompt")
    parser.add_argument("--round", help="round name (slugified for persistence)")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_RESULTS_ROOT,
        help="result root (default: ~/.ax-eval)",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=2,
        help="maximum tested agents or judges per parallel batch (default: 2)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=3600,
        help="hard timeout for each Codex process (default: 3600)",
    )
    parser.add_argument(
        "--cargo-target-dir",
        type=Path,
        help="shared Cargo target under REPO/target (default: REPO/target/ax-eval)",
    )
    parser.add_argument("--codex-bin", default="codex", help=argparse.SUPPRESS)

    commands = parser.add_subparsers(dest="command", title="artifact utilities")

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
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command is None:
            required = ("repo", "revision", "model", "task", "tool", "round")
            missing = [f"--{name.replace('_', '-')}" for name in required if getattr(args, name) is None]
            if missing:
                parser.error("the following arguments are required for an evaluation: " + ", ".join(missing))
            print(run_evaluation(args))
        elif args.command == "extract":
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
    except KeyboardInterrupt:
        _terminate_active_processes()
        print("error: interrupted; retained all completed and partial artifacts", file=sys.stderr)
        return 130
    except (OSError, KeyError, TypeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
