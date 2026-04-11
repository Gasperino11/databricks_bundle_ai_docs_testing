#!/usr/bin/env python3
"""Discover DABs and dispatch Copilot coding agents via GitHub Issues.

Usage:
    python scripts/dispatch_agent.py writer [--ai-docs-dir ai_docs]
    python scripts/dispatch_agent.py reviewer [--ai-docs-dir ai_docs] [--output-dir data_eng] [--pr-number 42]
    python scripts/dispatch_agent.py review-single --dab-path data_eng/my_dab
    python scripts/dispatch_agent.py review-batch [--ai-docs-dir ai_docs] [--output-dir data_eng]

Environment variables:
    GITHUB_TOKEN        GitHub token with issues/contents/PRs write access
    GITHUB_REPOSITORY   owner/repo (set automatically in GitHub Actions)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap
from pathlib import Path

import requests


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
# GitHub API helpers
# ---------------------------------------------------------------------------

def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def create_copilot_issue(
    token: str,
    owner: str,
    repo: str,
    title: str,
    body: str,
    labels: list[str],
) -> dict:
    """Create a GitHub issue assigned to the Copilot coding agent.

    Tries the ``copilot_sdk`` Python package first (if installed), then
    falls back to a direct GitHub REST API call via ``requests``.
    """
    # ------------------------------------------------------------------
    # Attempt: use the Copilot SDK if it is installed
    # ------------------------------------------------------------------
    try:
        from copilot_sdk import create_agent_task  # type: ignore[import-untyped]

        result = create_agent_task(
            token=token,
            owner=owner,
            repo=repo,
            title=title,
            body=body,
            labels=labels,
        )
        print(f"[copilot-sdk] Created agent task: {result}")
        return result
    except ImportError:
        pass  # SDK not available – fall through to REST API
    except Exception as exc:
        print(f"[copilot-sdk] SDK call failed ({exc}); falling back to REST API")

    # ------------------------------------------------------------------
    # Fallback: GitHub REST API
    # ------------------------------------------------------------------
    url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    payload = {
        "title": title,
        "body": body,
        "labels": labels,
        "assignees": ["copilot"],
    }
    resp = requests.post(url, headers=_github_headers(token), json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    print(f"Created issue #{data['number']}: {data['html_url']}")
    return data


# ---------------------------------------------------------------------------
# Issue-body builders
# ---------------------------------------------------------------------------

def _writer_body(dab_dirs: list[str], ai_docs_dir: str) -> str:
    dab_list = "\n".join(f"- `{ai_docs_dir}/{d}/`" for d in dab_dirs)
    return textwrap.dedent(f"""\
        ## Task: Write DAB Documentation

        @copilot Please write documentation for the following Databricks Asset Bundles:

        {dab_list}

        ### Instructions

        Follow the instructions in `.github/agents/writer-agent.md` to generate documentation.

        For each DAB listed above:
        1. Read `databricks.yml`, all files in `src/`, and all files in `resources/`
        2. Read the `README.md` template (which follows the structure from `templates/README_TEMPLATE.md`)
        3. Replace the README.md with fully completed documentation following the template instructions
        4. Remove all `<!-- INSTRUCTIONS: ... -->` comment blocks from the output

        The template is at `templates/README_TEMPLATE.md` for reference.
    """)


def _reviewer_body(
    dab_dirs: list[str],
    ai_docs_dir: str,
    output_dir: str,
    *,
    pr_number: int | None = None,
) -> str:
    dab_list = "\n".join(f"- `{ai_docs_dir}/{d}/`" for d in dab_dirs)
    merged_note = ""
    if pr_number:
        merged_note = (
            f"\nThe Writer agent's PR #{pr_number} has been merged. "
        )
    return textwrap.dedent(f"""\
        ## Task: Review DAB Documentation (Batch — Auto-triggered)

        @copilot {merged_note}Please review the documentation for the following Databricks Asset Bundles:

        {dab_list}

        ### Instructions

        Follow the instructions in `.github/agents/reviewer-agent.md` using **Mode 1: Batch Review**.

        For each DAB listed above:
        1. Read the `README.md`
        2. Read all code and config files (`databricks.yml`, `src/*.py`, `resources/*.yml`)
        3. Cross-reference the documentation against the code
        4. Write a `REVIEW.md` in the DAB root with any issues found (include file paths and line numbers)
        5. Move the DAB subfolder from `{ai_docs_dir}/` to `{output_dir}/`

        The review format and checks are documented in `.github/agents/reviewer-agent.md`.
    """)


def _single_review_body(dab_path: str) -> str:
    dab_name = Path(dab_path).name
    return textwrap.dedent(f"""\
        ## Task: Review DAB Documentation (Single DAB)

        @copilot Please review the documentation for this Databricks Asset Bundle:

        - `{dab_path}/`

        ### Instructions

        Follow the instructions in `.github/agents/reviewer-agent.md` using **Mode 2: Single DAB Review**.

        1. Read the `README.md` in `{dab_path}/`
        2. Read all code and config files in the DAB (`databricks.yml`, `src/*.py`, `resources/*.yml`)
        3. Cross-reference the documentation against the code
        4. Write a `REVIEW.md` in `{dab_path}/` with any issues found (include file paths and line numbers)
        5. Do NOT move this DAB — it is already in its final location

        The review format and checks are documented in `.github/agents/reviewer-agent.md`.
    """)


# ---------------------------------------------------------------------------
# Sub-commands
# ---------------------------------------------------------------------------

def cmd_writer(args: argparse.Namespace) -> None:
    """Dispatch the Writer agent."""
    token, owner, repo = _env()
    dab_dirs = discover_dabs(args.ai_docs_dir)
    if not dab_dirs:
        print(f"No DABs found in {args.ai_docs_dir}")
        return

    print(f"Found {len(dab_dirs)} DAB(s) to document: {', '.join(dab_dirs)}")
    title = f"\U0001f4dd Write DAB Documentation: {', '.join(dab_dirs)}"
    body = _writer_body(dab_dirs, args.ai_docs_dir)
    create_copilot_issue(token, owner, repo, title, body, ["copilot", "dab-docs-writer"])


def cmd_reviewer(args: argparse.Namespace) -> None:
    """Dispatch the Reviewer agent (auto-chained from writer merge)."""
    token, owner, repo = _env()
    dab_dirs = discover_dabs(args.ai_docs_dir)
    if not dab_dirs:
        print(f"No DABs found in {args.ai_docs_dir} to review")
        return

    print(f"Found {len(dab_dirs)} DAB(s) to review: {', '.join(dab_dirs)}")
    title = f"\U0001f50d Review DAB Documentation: {', '.join(dab_dirs)} (auto-chained)"
    body = _reviewer_body(
        dab_dirs,
        args.ai_docs_dir,
        args.output_dir,
        pr_number=args.pr_number,
    )
    create_copilot_issue(token, owner, repo, title, body, ["copilot", "dab-docs-reviewer"])


def cmd_review_single(args: argparse.Namespace) -> None:
    """Dispatch a reviewer for a single DAB."""
    token, owner, repo = _env()
    dab_path = args.dab_path
    if not Path(dab_path, "databricks.yml").exists():
        print(f"No databricks.yml found at {dab_path}")
        sys.exit(1)

    dab_name = Path(dab_path).name
    title = f"\U0001f50d Review DAB Documentation: {dab_name}"
    body = _single_review_body(dab_path)
    create_copilot_issue(token, owner, repo, title, body, ["copilot", "dab-docs-reviewer"])


def cmd_review_batch(args: argparse.Namespace) -> None:
    """Dispatch a batch reviewer (manual trigger)."""
    token, owner, repo = _env()
    dab_dirs = discover_dabs(args.ai_docs_dir)
    if not dab_dirs:
        print(f"No DABs found in {args.ai_docs_dir}")
        return

    print(f"Found {len(dab_dirs)} DAB(s) to review: {', '.join(dab_dirs)}")
    title = f"\U0001f50d Review DAB Documentation: {', '.join(dab_dirs)}"
    body = _reviewer_body(dab_dirs, args.ai_docs_dir, args.output_dir)
    create_copilot_issue(token, owner, repo, title, body, ["copilot", "dab-docs-reviewer"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env() -> tuple[str, str, str]:
    """Return (token, owner, repo) from environment."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        sys.exit("GITHUB_TOKEN environment variable is required")
    full_repo = os.environ.get("GITHUB_REPOSITORY", "")
    if "/" not in full_repo:
        sys.exit("GITHUB_REPOSITORY must be in 'owner/repo' format")
    owner, repo = full_repo.split("/", 1)
    return token, owner, repo


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
    p_reviewer = sub.add_parser("reviewer", help="Dispatch the Reviewer agent (auto-chained)")
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
