#!/usr/bin/env python3
"""
Dispatch a Copilot agent task by creating a GitHub issue assigned to 'copilot'.

Usage:
    python scripts/dispatch_copilot.py write-pr
    python scripts/dispatch_copilot.py write-adhoc
    python scripts/dispatch_copilot.py review-adhoc
    python scripts/dispatch_copilot.py request-review

All configuration is read from environment variables — nothing is interpolated
from untrusted input directly into script code.

Required env vars (all subcommands):
    GITHUB_TOKEN   - PAT with issues:write and pull-requests:write
    GITHUB_REPOSITORY - e.g. "owner/repo"

Subcommand-specific env vars:
    write-pr:
        PR_NUMBER   - pull request number
        PR_BRANCH   - head branch name
        DAB_LIST    - newline-separated list of DAB relative paths

    write-adhoc / review-adhoc:
        FOLDER_PATH - relative path to the DAB folder
        RUN_ID      - GitHub Actions run ID (for linking back)
        SERVER_URL  - e.g. "https://github.com"

    request-review:
        PR_NUMBER   - pull request number
"""

import json
import os
import sys
import urllib.request
import urllib.error


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


def cmd_write_pr() -> None:
    pr_number = os.environ["PR_NUMBER"]
    branch = os.environ["PR_BRANCH"]
    dab_list_raw = os.environ.get("DAB_LIST", "")
    dab_paths = [p.strip() for p in dab_list_raw.splitlines() if p.strip()]

    dab_bullets = "\n".join(f"- `{p}`" for p in dab_paths)
    body = f"""\
## Task: Write DAB Documentation

This task was triggered by PR #{pr_number} on branch `{branch}`.

Please use the **write-dab-documentation** skill (`.github/agents/skills/write-dab-documentation.md`) to document each Databricks Asset Bundle listed below.
Each listed directory contains a `databricks.yml` and a `README.md` that still has template placeholder content.
The original README template can always be found at `templates/README_TEMPLATE.md`.

### DABs to Document
{dab_bullets}

### Instructions
1. For each DAB path above, follow the `write-dab-documentation` skill.
2. Replace the template README content with fully generated documentation.
3. Commit the updated README(s) directly to branch `{branch}`.
"""
    owner, repo = _repo().split("/", 1)
    result = _api("POST", f"/repos/{owner}/{repo}/issues", {
        "title": f"Write DAB documentation for PR #{pr_number}",
        "body": body,
        "assignees": ["copilot"],
    })
    print(f"Created issue #{result['number']}: {result['html_url']}")


def cmd_write_adhoc() -> None:
    folder_path = os.environ["FOLDER_PATH"]
    run_id = os.environ.get("RUN_ID", "")
    server_url = os.environ.get("SERVER_URL", "https://github.com")
    owner, repo = _repo().split("/", 1)
    run_url = f"{server_url}/{owner}/{repo}/actions/runs/{run_id}"

    body = f"""\
## Task: Write DAB Documentation

This task was triggered manually via workflow run [#{run_id}]({run_url}).

Please use the **write-dab-documentation** skill (`.github/agents/skills/write-dab-documentation.md`) to document the Databricks Asset Bundle below.
The original README template can always be found at `templates/README_TEMPLATE.md`.

### DAB to Document
- `{folder_path}`

### Instructions
1. Follow the `write-dab-documentation` skill for the DAB path above.
2. Replace the template README content with fully generated documentation.
3. Commit the updated README to the default branch.
"""
    result = _api("POST", f"/repos/{owner}/{repo}/issues", {
        "title": f"Write DAB documentation: {folder_path}",
        "body": body,
        "assignees": ["copilot"],
    })
    print(f"Created issue #{result['number']}: {result['html_url']}")


def cmd_review_adhoc() -> None:
    folder_path = os.environ["FOLDER_PATH"]
    run_id = os.environ.get("RUN_ID", "")
    server_url = os.environ.get("SERVER_URL", "https://github.com")
    owner, repo = _repo().split("/", 1)
    run_url = f"{server_url}/{owner}/{repo}/actions/runs/{run_id}"

    body = f"""\
## Task: Review DAB Documentation

This task was triggered manually via workflow run [#{run_id}]({run_url}).

Please use the **review-dab-documentation** skill (`.github/agents/skills/review-dab-documentation.md`) to review the Databricks Asset Bundle below.
The original README template can always be found at `templates/README_TEMPLATE.md`.

### DAB to Review
- `{folder_path}`

### Instructions
1. Follow the `review-dab-documentation` skill for the DAB path above.
2. Write a `REVIEW.md` file in the DAB root summarizing all findings.
3. Commit the `REVIEW.md` to the default branch.
"""
    result = _api("POST", f"/repos/{owner}/{repo}/issues", {
        "title": f"Review DAB documentation: {folder_path}",
        "body": body,
        "assignees": ["copilot"],
    })
    print(f"Created issue #{result['number']}: {result['html_url']}")


def cmd_request_review() -> None:
    pr_number = int(os.environ["PR_NUMBER"])
    owner, repo = _repo().split("/", 1)
    result = _api("POST", f"/repos/{owner}/{repo}/pulls/{pr_number}/requested_reviewers", {
        "reviewers": ["copilot"],
    })
    print(f"Requested review on PR #{pr_number}: {result.get('html_url', '')}")


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
