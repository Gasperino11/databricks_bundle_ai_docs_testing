#!/usr/bin/env python3
"""Discover DABs and dispatch Copilot coding agents via the GitHub Copilot SDK.

Usage:
    python scripts/dispatch_agent.py writer [--ai-docs-dir ai_docs]
    python scripts/dispatch_agent.py reviewer [--ai-docs-dir ai_docs] [--output-dir data_eng] [--pr-number 42]
    python scripts/dispatch_agent.py review-single --dab-path data_eng/my_dab
    python scripts/dispatch_agent.py review-batch [--ai-docs-dir ai_docs] [--output-dir data_eng]

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
_MODEL = "gpt-4o"

# ---------------------------------------------------------------------------
# DAB discovery
# ---------------------------------------------------------------------------


def discover_dabs(base_dir: str) -> list[str]:
    """Return names of subdirectories that contain a ``databricks.yml``."""
    base = Path(base_dir)
    if not base.exists():
        return []
    return sorted(
        d.name
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


def _writer_prompt(dab_dirs: list[str], ai_docs_dir: str) -> str:
    dab_list = "\n".join(f"- `{ai_docs_dir}/{d}/`" for d in dab_dirs)
    return textwrap.dedent(f"""\
        Write documentation for the following Databricks Asset Bundles:

        {dab_list}

        For each DAB listed above:
        1. Read `databricks.yml`, all files in `src/`, and all files in `resources/`
        2. Read the `README.md` template (which follows the structure from `templates/README_TEMPLATE.md`)
        3. Replace the README.md with fully completed documentation following the template instructions
        4. Remove all `<!-- INSTRUCTIONS: ... -->` comment blocks from the output

        The template is at `templates/README_TEMPLATE.md` for reference.

        After writing documentation for all DABs, open a pull request with your changes.
    """)


def _reviewer_prompt(
    dab_dirs: list[str],
    ai_docs_dir: str,
    output_dir: str,
    *,
    pr_number: int | None = None,
) -> str:
    dab_list = "\n".join(f"- `{ai_docs_dir}/{d}/`" for d in dab_dirs)
    merged_note = ""
    if pr_number:
        merged_note = f"\nThe Writer agent's PR #{pr_number} has been merged. "
    return textwrap.dedent(f"""\
        {merged_note}Review the documentation for the following Databricks Asset Bundles:

        {dab_list}

        Use **Mode 1: Batch Review** as described in your instructions.

        For each DAB listed above:
        1. Read the `README.md`
        2. Read all code and config files (`databricks.yml`, `src/*.py`, `resources/*.yml`)
        3. Cross-reference the documentation against the code
        4. Write a `REVIEW.md` in the DAB root with any issues found (include file paths and line numbers)
        5. Move the DAB subfolder from `{ai_docs_dir}/` to `{output_dir}/`

        After reviewing all DABs, open a pull request with your changes.
    """)


def _single_review_prompt(dab_path: str) -> str:
    return textwrap.dedent(f"""\
        Review the documentation for this Databricks Asset Bundle:

        - `{dab_path}/`

        Use **Mode 2: Single DAB Review** as described in your instructions.

        1. Read the `README.md` in `{dab_path}/`
        2. Read all code and config files in the DAB (`databricks.yml`, `src/*.py`, `resources/*.yml`)
        3. Cross-reference the documentation against the code
        4. Write a `REVIEW.md` in `{dab_path}/` with any issues found (include file paths and line numbers)
        5. Do NOT move this DAB — it is already in its final location

        After reviewing, open a pull request with your changes.
    """)


# ---------------------------------------------------------------------------
# Copilot SDK session runner
# ---------------------------------------------------------------------------


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
        session = await client.create_session(
            on_permission_request=PermissionHandler.approve_all,
            model=_MODEL,
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


def cmd_writer(args: argparse.Namespace) -> None:
    """Dispatch the Writer agent."""
    dab_dirs = discover_dabs(args.ai_docs_dir)
    if not dab_dirs:
        print(f"No DABs found in {args.ai_docs_dir}")
        return

    print(f"Found {len(dab_dirs)} DAB(s) to document: {', '.join(dab_dirs)}")
    system_msg = _read_agent_instructions("writer-agent.md")
    prompt = _writer_prompt(dab_dirs, args.ai_docs_dir)
    asyncio.run(_run_copilot_session(system_msg, prompt))


def cmd_reviewer(args: argparse.Namespace) -> None:
    """Dispatch the Reviewer agent (auto-chained from writer merge)."""
    dab_dirs = discover_dabs(args.ai_docs_dir)
    if not dab_dirs:
        print(f"No DABs found in {args.ai_docs_dir} to review")
        return

    print(f"Found {len(dab_dirs)} DAB(s) to review: {', '.join(dab_dirs)}")
    system_msg = _read_agent_instructions("reviewer-agent.md")
    prompt = _reviewer_prompt(
        dab_dirs,
        args.ai_docs_dir,
        args.output_dir,
        pr_number=args.pr_number,
    )
    asyncio.run(_run_copilot_session(system_msg, prompt))


def cmd_review_single(args: argparse.Namespace) -> None:
    """Dispatch a reviewer for a single DAB."""
    dab_path = args.dab_path
    if not Path(dab_path, "databricks.yml").exists():
        print(f"No databricks.yml found at {dab_path}")
        sys.exit(1)

    dab_name = Path(dab_path).name
    print(f"Reviewing single DAB: {dab_name}")
    system_msg = _read_agent_instructions("reviewer-agent.md")
    prompt = _single_review_prompt(dab_path)
    asyncio.run(_run_copilot_session(system_msg, prompt))


def cmd_review_batch(args: argparse.Namespace) -> None:
    """Dispatch a batch reviewer (manual trigger)."""
    dab_dirs = discover_dabs(args.ai_docs_dir)
    if not dab_dirs:
        print(f"No DABs found in {args.ai_docs_dir}")
        return

    print(f"Found {len(dab_dirs)} DAB(s) to review: {', '.join(dab_dirs)}")
    system_msg = _read_agent_instructions("reviewer-agent.md")
    prompt = _reviewer_prompt(dab_dirs, args.ai_docs_dir, args.output_dir)
    asyncio.run(_run_copilot_session(system_msg, prompt))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dispatch Copilot coding agents for DAB documentation"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # writer
    p_writer = sub.add_parser("writer", help="Dispatch the Writer agent")
    p_writer.add_argument("--ai-docs-dir", default="ai_docs")

    # reviewer (auto-chained)
    p_reviewer = sub.add_parser(
        "reviewer", help="Dispatch the Reviewer agent (auto-chained)"
    )
    p_reviewer.add_argument("--ai-docs-dir", default="ai_docs")
    p_reviewer.add_argument("--output-dir", default="data_eng")
    p_reviewer.add_argument("--pr-number", type=int, default=None)

    # review-single
    p_single = sub.add_parser("review-single", help="Review a single DAB")
    p_single.add_argument("--dab-path", required=True)

    # review-batch
    p_batch = sub.add_parser("review-batch", help="Batch review all DABs")
    p_batch.add_argument("--ai-docs-dir", default="ai_docs")
    p_batch.add_argument("--output-dir", default="data_eng")

    args = parser.parse_args()
    {
        "writer": cmd_writer,
        "reviewer": cmd_reviewer,
        "review-single": cmd_review_single,
        "review-batch": cmd_review_batch,
    }[args.command](args)


if __name__ == "__main__":
    main()
