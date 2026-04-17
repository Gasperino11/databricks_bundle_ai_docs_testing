#!/usr/bin/env python3
"""Discover DABs and dispatch Copilot coding agents via the GitHub Copilot SDK.

Usage:
    python scripts/dispatch_agent.py writer --scan-dir data_eng
    python scripts/dispatch_agent.py writer --dab-path data_eng/my_dab
    python scripts/dispatch_agent.py reviewer --scan-dir data_eng
    python scripts/dispatch_agent.py reviewer --dab-path data_eng/my_dab

Environment variables:
    GITHUB_TOKEN        GitHub token (used by the Copilot SDK for authentication)
    GITHUB_REPOSITORY   owner/repo (set automatically in GitHub Actions)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import textwrap
from pathlib import Path

from copilot import CopilotClient
from copilot.session import PermissionHandler


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SESSION_TIMEOUT = 600  # seconds to wait for a Copilot session to complete

# Preferred model patterns, checked in order.  The first available model whose
# ``id`` contains one of these substrings (case-insensitive) wins.
_PREFERRED_MODEL_PATTERNS: list[str] = [
    "claude",       # Prefer Claude models first
    "gpt",          # Then GPT models
]
_FALLBACK_MODEL = "gpt-4o-mini"  # Last-resort if list_models() fails

# ---------------------------------------------------------------------------
# DAB discovery
# ---------------------------------------------------------------------------


def discover_dabs(base_dir: str) -> list[str]:
    """Return relative paths of subdirectories that contain a ``databricks.yml``."""
    base = Path(base_dir)
    if not base.exists():
        return []
    return sorted(
        str(d.relative_to(Path.cwd())) if d.is_absolute() else str(d)
        for d in base.iterdir()
        if d.is_dir() and (d / "databricks.yml").exists()
    )


# ---------------------------------------------------------------------------
# Agent instruction loader
# ---------------------------------------------------------------------------


def _read_agent_instructions(agent_filename: str) -> str:
    """Read a markdown agent prompt from ``.github/agents/<agent_filename>``."""
    repo_root = Path(__file__).resolve().parent.parent
    path = repo_root / ".github" / "agents" / agent_filename
    if not path.exists():
        sys.exit(f"Agent instructions not found: {path}")
    return path.read_text()


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------


def _writer_prompt(dab_paths: list[str]) -> str:
    dab_list = "\n".join(f"- `{p}/`" for p in dab_paths)
    return textwrap.dedent(f"""\
        Write documentation for the following Databricks Asset Bundles:

        {dab_list}

        For each DAB listed above:
        1. Read `databricks.yml`, all files in `src/`, and all files in `resources/`
        2. Read the existing `README.md` if present (it may be a template or already-written docs)
        3. Replace or create the README.md with fully completed documentation following the template structure from `templates/README_TEMPLATE.md`
        4. Remove all `<!-- INSTRUCTIONS: ... -->` comment blocks from the output

        The template is at `templates/README_TEMPLATE.md` for reference.

        After writing documentation for all DABs, commit your changes.
    """)


def _reviewer_prompt(dab_paths: list[str]) -> str:
    dab_list = "\n".join(f"- `{p}/`" for p in dab_paths)
    return textwrap.dedent(f"""\
        Review the documentation for the following Databricks Asset Bundles:

        {dab_list}

        For each DAB listed above:
        1. Read the `README.md`
        2. Read all code and config files (`databricks.yml`, `src/*.py`, `resources/*.yml`)
        3. Cross-reference the documentation against the code
        4. Write a `REVIEW.md` in the DAB root with any issues found (include file paths and line numbers)

        After reviewing all DABs, commit your changes.
    """)


# ---------------------------------------------------------------------------
# Copilot SDK session runner
# ---------------------------------------------------------------------------


async def _select_model(client: CopilotClient) -> str:
    """Pick the best available model, preferring Claude, with a fallback."""
    try:
        models = await client.list_models()
        if not models:
            print(f"⚠️  No models returned by list_models(); falling back to {_FALLBACK_MODEL}")
            return _FALLBACK_MODEL

        available_ids = [m.id for m in models]
        print(f"📋 Available models: {', '.join(available_ids)}")

        # Walk through preference list and pick the first match
        for pattern in _PREFERRED_MODEL_PATTERNS:
            for model in models:
                if pattern.lower() in model.id.lower():
                    print(f"✅ Selected model: {model.id} (matched preference '{pattern}')")
                    return model.id

        # No preference matched – use the first available model
        chosen = models[0].id
        print(f"✅ Selected model: {chosen} (first available)")
        return chosen
    except Exception as exc:
        print(f"⚠️  list_models() failed ({exc}); falling back to {_FALLBACK_MODEL}")
        return _FALLBACK_MODEL


async def _run_copilot_session(system_message: str, prompt: str) -> None:
    """Create a Copilot SDK session, send a prompt, and wait for completion."""
    if not os.environ.get("GITHUB_TOKEN"):
        sys.exit("GITHUB_TOKEN environment variable is not set")

    repo_root = Path(__file__).resolve().parent.parent
    os.chdir(repo_root)

    client = CopilotClient()
    await client.start()

    session = None
    try:
        model = await _select_model(client)

        session = await client.create_session(
            on_permission_request=PermissionHandler.approve_all,
            model=model,
            system_message={"mode": "replace", "content": system_message},
        )

        def handle_session_event(event):
            etype = event.type.value if hasattr(event.type, "value") else str(event.type)
            if etype == "tool.execution_start":
                print(f"  ⚙️  {event.data.tool_name}")
            elif etype == "assistant.message":
                print(f"\n🤖 {event.data.content}\n")

        session.on(handle_session_event)

        print("📊 Sending task to Copilot…")
        await session.send_and_wait(
            prompt,
            timeout=_SESSION_TIMEOUT,
        )
        print("✅ Copilot session completed.")
    except TimeoutError:
        print(
            f"⏱️ Copilot session timed out after {_SESSION_TIMEOUT}s. "
            "Consider increasing _SESSION_TIMEOUT or breaking the task into "
            "smaller pieces."
        )
        sys.exit(1)
    finally:
        if session is not None:
            await session.disconnect()
        await client.stop()


# ---------------------------------------------------------------------------
# Sub-commands
# ---------------------------------------------------------------------------


def _resolve_dab_paths(args: argparse.Namespace) -> list[str]:
    """Resolve DAB paths from either --dab-path or --scan-dir."""
    if args.dab_path:
        dab_path = args.dab_path
        if not Path(dab_path, "databricks.yml").exists():
            print(f"No databricks.yml found at {dab_path}")
            sys.exit(1)
        return [dab_path]

    scan_dir = args.scan_dir
    dab_paths = discover_dabs(scan_dir)
    if not dab_paths:
        print(f"No DABs found in {scan_dir}")
        sys.exit(0)
    return dab_paths


def cmd_writer(args: argparse.Namespace) -> None:
    """Dispatch the Writer agent."""
    dab_paths = _resolve_dab_paths(args)
    print(f"Found {len(dab_paths)} DAB(s) to document: {', '.join(dab_paths)}")
    system_msg = _read_agent_instructions("writer-agent.md")
    prompt = _writer_prompt(dab_paths)
    asyncio.run(_run_copilot_session(system_msg, prompt))


def cmd_reviewer(args: argparse.Namespace) -> None:
    """Dispatch the Reviewer agent."""
    dab_paths = _resolve_dab_paths(args)
    print(f"Found {len(dab_paths)} DAB(s) to review: {', '.join(dab_paths)}")
    system_msg = _read_agent_instructions("reviewer-agent.md")
    prompt = _reviewer_prompt(dab_paths)
    asyncio.run(_run_copilot_session(system_msg, prompt))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dispatch Copilot coding agents for DAB documentation"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # Shared arguments for both commands
    for name, help_text in [
        ("writer", "Dispatch the Writer agent"),
        ("reviewer", "Dispatch the Reviewer agent"),
    ]:
        p = sub.add_parser(name, help=help_text)
        group = p.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--dab-path",
            help="Path to a single DAB folder containing a databricks.yml",
        )
        group.add_argument(
            "--scan-dir",
            help="Directory to scan for DAB subfolders (e.g., data_eng)",
        )

    args = parser.parse_args()
    {"writer": cmd_writer, "reviewer": cmd_reviewer}[args.command](args)


if __name__ == "__main__":
    main()
