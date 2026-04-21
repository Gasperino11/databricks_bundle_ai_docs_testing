#!/usr/bin/env python3
"""
Generate and commit DAB documentation directly, without creating GitHub Issues.

Usage:
    python scripts/dispatch_copilot.py write-pr
    python scripts/dispatch_copilot.py write-adhoc
    python scripts/dispatch_copilot.py review-adhoc
    python scripts/dispatch_copilot.py request-review

All configuration is read from environment variables — nothing is interpolated
from untrusted input directly into script code.

Required env vars (all subcommands):
    GITHUB_TOKEN   - PAT or GITHUB_TOKEN with contents:write
    GITHUB_REPOSITORY - e.g. "owner/repo"

Subcommand-specific env vars:
    write-pr:
        PR_NUMBER   - pull request number
        PR_BRANCH   - head branch name
        DAB_LIST    - newline-separated list of DAB relative paths

    write-adhoc / review-adhoc:
        FOLDER_PATH - relative path to the DAB folder

    request-review:
        PR_NUMBER   - pull request number
"""

import glob
import json
import os
import sys
import urllib.request
import urllib.error

_MODELS_API_URL = "https://models.inference.ai.azure.com/chat/completions"


def _api(method: str, path: str, payload: dict | None = None) -> dict:
    token = os.environ["GITHUB_TOKEN"]
    url = f"https://api.github.com{path}"
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        print(f"GitHub API error {exc.code}: {body}", file=sys.stderr)
        sys.exit(1)


def _repo() -> str:
    return os.environ["GITHUB_REPOSITORY"]


def _read_file_safe(path: str) -> str:
    """Read a file, returning an empty string if it doesn't exist."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return ""


def _collect_dab_files(dab_path: str) -> dict:
    """Collect all relevant source files from a DAB directory."""
    files = {}
    for rel in ("databricks.yml", "README.md"):
        content = _read_file_safe(os.path.join(dab_path, rel))
        if content:
            files[rel] = content
    for pattern in (
        os.path.join(dab_path, "src", "**", "*.py"),
        os.path.join(dab_path, "resources", "**", "*.yml"),
        os.path.join(dab_path, "resources", "**", "*.py"),
        os.path.join(dab_path, "resources", "**", "*.sql"),
    ):
        for full in glob.glob(pattern, recursive=True):
            rel = os.path.relpath(full, dab_path)
            content = _read_file_safe(full)
            if content:
                files[rel] = content
    return files


def _call_models_api(token: str, messages: list) -> str:
    """Call the GitHub Models inference API to generate content."""
    url = _MODELS_API_URL
    payload = {
        "model": "gpt-4o-mini",
        "messages": messages,
        "max_tokens": 4096,
        "temperature": 0.2,
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            return result["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        print(f"Models API error {exc.code}: {body}", file=sys.stderr)
        sys.exit(1)


def _generate_dab_readme(token: str, dab_path: str) -> str:
    """Generate README.md content for a DAB using the Models API."""
    skill_path = ".github/agents/skills/write-dab-documentation.md"
    template_path = "templates/README_TEMPLATE.md"
    skill_content = _read_file_safe(skill_path)
    template_content = _read_file_safe(template_path)
    if not skill_content:
        print(f"Warning: skill file not found: {skill_path}", file=sys.stderr)
    if not template_content:
        print(f"Warning: template file not found: {template_path}", file=sys.stderr)
    files = _collect_dab_files(dab_path)

    file_sections = "\n\n".join(
        f"### File: `{rel}`\n```\n{content}\n```"
        for rel, content in files.items()
    )

    messages = [
        {
            "role": "system",
            "content": (
                "You are a technical documentation writer for Databricks Asset Bundles.\n\n"
                + skill_content
            ),
        },
        {
            "role": "user",
            "content": (
                f"Generate complete README.md documentation for the Databricks Asset Bundle "
                f"at path `{dab_path}`.\n\n"
                f"README template to follow:\n{template_content}\n\n"
                f"DAB files:\n{file_sections}\n\n"
                "Output ONLY the finished markdown content with no leftover template "
                "instruction comments and no additional prose before or after the markdown."
            ),
        },
    ]
    return _call_models_api(token, messages)


def cmd_write_pr() -> None:
    pr_number = os.environ["PR_NUMBER"]
    branch = os.environ["PR_BRANCH"]
    dab_list_raw = os.environ.get("DAB_LIST", "")
    dab_paths = [p.strip() for p in dab_list_raw.splitlines() if p.strip()]
    token = os.environ["GITHUB_TOKEN"]

    for dab_path in dab_paths:
        print(f"Generating documentation for {dab_path} …")
        readme_content = _generate_dab_readme(token, dab_path)
        readme_path = os.path.join(dab_path, "README.md")
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(readme_content)
        print(f"  ✓ Wrote {readme_path}")

    if dab_paths:
        print(
            f"Documentation generated for PR #{pr_number} on branch '{branch}'. "
            "Caller should commit and push changes."
        )


def cmd_write_adhoc() -> None:
    folder_path = os.environ["FOLDER_PATH"]
    token = os.environ["GITHUB_TOKEN"]

    print(f"Generating documentation for {folder_path} …")
    readme_content = _generate_dab_readme(token, folder_path)
    readme_path = os.path.join(folder_path, "README.md")
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(readme_content)
    print(f"  ✓ Wrote {readme_path}")
    print("Caller should commit and push changes.")


def cmd_review_adhoc() -> None:
    folder_path = os.environ["FOLDER_PATH"]
    token = os.environ["GITHUB_TOKEN"]

    skill_path = ".github/agents/skills/review-dab-documentation.md"
    template_path = "templates/README_TEMPLATE.md"
    skill_content = _read_file_safe(skill_path)
    template_content = _read_file_safe(template_path)
    if not skill_content:
        print(f"Warning: skill file not found: {skill_path}", file=sys.stderr)
    if not template_content:
        print(f"Warning: template file not found: {template_path}", file=sys.stderr)
    files = _collect_dab_files(folder_path)

    file_sections = "\n\n".join(
        f"### File: `{rel}`\n```\n{content}\n```"
        for rel, content in files.items()
    )

    messages = [
        {
            "role": "system",
            "content": (
                "You are a technical documentation reviewer for Databricks Asset Bundles.\n\n"
                + skill_content
            ),
        },
        {
            "role": "user",
            "content": (
                f"Review the README.md documentation for the Databricks Asset Bundle "
                f"at path `{folder_path}`.\n\n"
                f"README template reference:\n{template_content}\n\n"
                f"DAB files:\n{file_sections}\n\n"
                "Output ONLY the finished REVIEW.md markdown content with no additional prose."
            ),
        },
    ]

    review_content = _call_models_api(token, messages)
    review_path = os.path.join(folder_path, "REVIEW.md")
    with open(review_path, "w", encoding="utf-8") as f:
        f.write(review_content)
    print(f"  ✓ Wrote {review_path}")
    print("Caller should commit and push changes.")


def cmd_request_review() -> None:
    # No-op: PR review requests to 'copilot' are not supported via the REST API.
    # Documentation review is handled by the review-ad-hoc workflow instead.
    pr_number = os.environ.get("PR_NUMBER", "unknown")
    print(f"Skipping PR review request for PR #{pr_number} (not supported via REST API).")


COMMANDS = {
    "write-pr": cmd_write_pr,
    "write-adhoc": cmd_write_adhoc,
    "review-adhoc": cmd_review_adhoc,
    "request-review": cmd_request_review,
}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        print(f"Usage: {sys.argv[0]} <{'|'.join(COMMANDS)}>\n\n{__doc__}", file=sys.stderr)
        sys.exit(1)
    COMMANDS[sys.argv[1]]()
