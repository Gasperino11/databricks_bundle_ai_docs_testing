"""
DAB Documentation Reviewer Agent
=================================
Reviews documentation for Databricks Asset Bundles. Operates in two modes:

1. **Batch mode (default):** Scans /ai_docs/ for DABs, reviews each one,
   produces a REVIEW.md, and moves the DAB subfolder to /data_eng/.

2. **Single DAB mode:** Points at a specific DAB (e.g., already in /data_eng/)
   and produces or updates a REVIEW.md with any outdated information.

Usage:
    # Batch review all DABs in ai_docs/
    python agents/reviewer_agent.py --ai-docs-dir ai_docs --output-dir data_eng

    # Review a single DAB already in data_eng/
    python agents/reviewer_agent.py --dab-path data_eng/s3_ingestion_pipeline
"""

import argparse
import os
import re
import shutil


def parse_args():
    parser = argparse.ArgumentParser(description="DAB Documentation Reviewer Agent")
    parser.add_argument(
        "--ai-docs-dir",
        default="ai_docs",
        help="Path to directory containing DABs to review and move (default: ai_docs)",
    )
    parser.add_argument(
        "--output-dir",
        default="data_eng",
        help="Path to directory where reviewed DABs are moved (default: data_eng)",
    )
    parser.add_argument(
        "--dab-path",
        default=None,
        help="Path to a single DAB to review (skips move). "
        "Use this to re-review a DAB already in data_eng/.",
    )
    return parser.parse_args()


def read_file(filepath):
    """Read and return the contents of a file, or None if it doesn't exist."""
    if not os.path.isfile(filepath):
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()


def collect_files_by_extension(directory, extensions):
    """Collect all files matching extensions in a directory tree."""
    files = {}
    if not os.path.isdir(directory):
        return files
    for root, _dirs, filenames in os.walk(directory):
        for fname in sorted(filenames):
            if any(fname.endswith(ext) for ext in extensions):
                fpath = os.path.join(root, fname)
                rel_path = os.path.relpath(fpath, directory)
                files[rel_path] = read_file(fpath)
    return files


def discover_dabs(ai_docs_dir):
    """Discover DAB subdirectories in the ai_docs folder."""
    dabs = []
    if not os.path.isdir(ai_docs_dir):
        return dabs
    for entry in sorted(os.listdir(ai_docs_dir)):
        dab_path = os.path.join(ai_docs_dir, entry)
        if os.path.isdir(dab_path) and os.path.isfile(
            os.path.join(dab_path, "databricks.yml")
        ):
            dabs.append(dab_path)
    return dabs


def read_dab_context(dab_path):
    """Read all relevant files from a DAB and return a context dict."""
    context = {
        "dab_name": os.path.basename(dab_path),
        "dab_path": dab_path,
        "databricks_yml": read_file(os.path.join(dab_path, "databricks.yml")),
        "readme": read_file(os.path.join(dab_path, "README.md")),
        "src_files": collect_files_by_extension(
            os.path.join(dab_path, "src"), [".py", ".sql", ".scala"]
        ),
        "resource_files": collect_files_by_extension(
            os.path.join(dab_path, "resources"), [".yml", ".yaml"]
        ),
    }
    return context


def extract_s3_paths_from_code(src_files):
    """Extract S3 paths from source code."""
    paths = set()
    for content in src_files.values():
        if content:
            for match in re.finditer(r's3://[^\s"\')+]+', content):
                paths.add(match.group())
    return paths


def extract_table_references_from_code(src_files):
    """Extract table references written to from source code, resolving f-string variables."""
    tables = {}  # table_ref -> list of (filename, line_number)
    for fname, content in src_files.items():
        if not content:
            continue
        lines = content.split("\n")

        # First pass: find f-string table name assignments
        table_vars = {}
        for m in re.finditer(r'(\w+)\s*=\s*f"([^"]*\{[^"]*\}[^"]*)"', content):
            var_name = m.group(1)
            fstring_val = m.group(2)
            table_vars[var_name] = fstring_val

        # Second pass: find saveAsTable calls and resolve
        for i, line in enumerate(lines, 1):
            m = re.search(r'saveAsTable\(\s*([^)]+)\s*\)', line)
            if m:
                ref = m.group(1).strip().strip('"').strip("'")
                resolved = table_vars.get(ref, ref)
                # Extract the actual table name (last part after dots)
                readable = re.sub(r'\{[^}]+\}', '', resolved)
                table_name = readable.split(".")[-1] if "." in readable else readable
                if table_name:
                    if table_name not in tables:
                        tables[table_name] = []
                    tables[table_name].append({"file": fname, "line": i})
    return tables


def check_readme_section_exists(readme, section_name):
    """Check if a section header exists in the README and has content."""
    if not readme:
        return False, "README.md is missing"
    pattern = rf"^##\s+{re.escape(section_name)}\s*$"
    match = re.search(pattern, readme, re.MULTILINE)
    if not match:
        return False, f"Section '## {section_name}' is missing from README"

    # Check if section has content (not just the header and markdown comments)
    section_start = match.end()
    next_section = re.search(r"^##\s+", readme[section_start:], re.MULTILINE)
    section_content = readme[section_start : section_start + next_section.start() if next_section else len(readme)]
    # Strip markdown comments
    stripped = re.sub(r"<!--.*?-->", "", section_content, flags=re.DOTALL).strip()
    if not stripped:
        return False, f"Section '## {section_name}' exists but has no content (only template comments)"
    return True, None


def check_s3_paths_documented(readme, src_files):
    """Verify that S3 paths in code are mentioned in the README."""
    issues = []
    code_paths = extract_s3_paths_from_code(src_files)
    for path in code_paths:
        if readme and path in readme:
            continue
        issues.append({
            "type": "missing_data_source",
            "description": f"S3 path `{path}` found in code but not documented in README",
            "code_refs": [],
        })
        # Find which files reference this path
        for fname, content in src_files.items():
            if content and path in content:
                for i, line in enumerate(content.split("\n"), 1):
                    if path in line:
                        issues[-1]["code_refs"].append({"file": fname, "line": i})
    return issues


def check_tables_documented(readme, src_files):
    """Verify that output tables in code are documented in the README."""
    issues = []
    code_tables = extract_table_references_from_code(src_files)
    for table_name, code_refs in code_tables.items():
        if readme and table_name in readme:
            continue
        issues.append({
            "type": "missing_data_output",
            "description": f"Output table `{table_name}` found in code but not documented in README",
            "code_refs": code_refs,
        })
    return issues


def check_jobs_documented(readme, databricks_yml):
    """Verify that jobs defined in databricks.yml are mentioned in README."""
    issues = []
    if not databricks_yml:
        return issues
    for match in re.finditer(r"^\s{4}(\w+):\s*$", databricks_yml.split("jobs:")[-1] if "jobs:" in databricks_yml else "", re.MULTILINE):
        job_name = match.group(1)
        if job_name.startswith("name") or job_name.startswith("description"):
            continue
        if readme and job_name in readme:
            continue
        issues.append({
            "type": "missing_managed_asset",
            "description": f"Job `{job_name}` defined in databricks.yml but not documented in README",
            "code_refs": [{"file": "databricks.yml", "line": "N/A"}],
        })
    return issues


def check_schedule_documented(readme, databricks_yml):
    """Verify that schedule information is documented."""
    issues = []
    if not databricks_yml:
        return issues
    cron_matches = re.findall(r'quartz_cron_expression:\s*"([^"]+)"', databricks_yml)
    for cron in cron_matches:
        if readme and cron in readme:
            continue
        issues.append({
            "type": "missing_schedule",
            "description": f"Schedule `{cron}` found in databricks.yml but not in README",
            "code_refs": [{"file": "databricks.yml", "line": "N/A"}],
        })
    return issues


def check_readme_has_template_comments(readme):
    """Check if README still has unfilled template instruction comments."""
    issues = []
    if not readme:
        return issues
    comment_count = len(re.findall(r"<!--\s*INSTRUCTIONS:", readme))
    if comment_count > 0:
        issues.append({
            "type": "template_comments_remaining",
            "description": f"README still contains {comment_count} unfilled template instruction comment(s). These should be removed after filling in documentation.",
            "code_refs": [],
        })
    return issues


def review_dab(context):
    """
    Review a DAB's documentation against its code and config.
    Returns a list of issues found.
    """
    issues = []
    readme = context["readme"]
    databricks_yml = context["databricks_yml"]
    src_files = context["src_files"]

    # Check required sections
    required_sections = [
        "Description & Purpose",
        "Folder Structure",
        "Job & Pipeline Diagram",
        "How to Deploy",
        "Schedule",
        "Data Sources",
        "Data Outputs",
        "Managed Assets",
        "Authors",
        "References",
    ]
    for section in required_sections:
        exists, msg = check_readme_section_exists(readme, section)
        if not exists:
            issues.append({
                "type": "missing_section",
                "description": msg,
                "code_refs": [],
            })

    # Cross-reference checks
    issues.extend(check_s3_paths_documented(readme, src_files))
    issues.extend(check_tables_documented(readme, src_files))
    issues.extend(check_jobs_documented(readme, databricks_yml))
    issues.extend(check_schedule_documented(readme, databricks_yml))
    issues.extend(check_readme_has_template_comments(readme))

    return issues


def generate_review_md(context, issues):
    """Generate the REVIEW.md content based on issues found."""
    lines = []
    lines.append(f"# Documentation Review: {context['dab_name']}")
    lines.append("")
    lines.append(f"**Reviewed:** {context['dab_path']}")
    lines.append("")

    if not issues:
        lines.append("## ✅ Review Result: PASSED")
        lines.append("")
        lines.append("No issues found. Documentation is consistent with the code and configuration.")
        lines.append("")
        return "\n".join(lines)

    lines.append(f"## ⚠️ Review Result: {len(issues)} Issue(s) Found")
    lines.append("")

    # Group issues by type
    issue_types = {}
    for issue in issues:
        itype = issue["type"]
        if itype not in issue_types:
            issue_types[itype] = []
        issue_types[itype].append(issue)

    type_labels = {
        "missing_section": "Missing Documentation Sections",
        "missing_data_source": "Undocumented Data Sources",
        "missing_data_output": "Undocumented Data Outputs",
        "missing_managed_asset": "Undocumented Managed Assets",
        "missing_schedule": "Undocumented Schedules",
        "template_comments_remaining": "Template Comments Still Present",
    }

    for itype, type_issues in issue_types.items():
        label = type_labels.get(itype, itype.replace("_", " ").title())
        lines.append(f"### {label}")
        lines.append("")

        for issue in type_issues:
            lines.append(f"- **{issue['description']}**")
            if issue["code_refs"]:
                for ref in issue["code_refs"]:
                    lines.append(f"  - File: `{ref['file']}`, Line: {ref['line']}")
        lines.append("")

    # Summary table
    lines.append("## Summary")
    lines.append("")
    lines.append("| Issue Type | Count |")
    lines.append("|------------|-------|")
    for itype, type_issues in issue_types.items():
        label = type_labels.get(itype, itype.replace("_", " ").title())
        lines.append(f"| {label} | {len(type_issues)} |")
    lines.append(f"| **Total** | **{len(issues)}** |")
    lines.append("")

    return "\n".join(lines)


def write_review(dab_path, review_content):
    """Write REVIEW.md to the DAB directory."""
    review_path = os.path.join(dab_path, "REVIEW.md")
    with open(review_path, "w", encoding="utf-8") as f:
        f.write(review_content)
    print(f"[Reviewer] Wrote review to {review_path}")


def move_dab(dab_path, output_dir):
    """Move a DAB from ai_docs to the output directory."""
    dab_name = os.path.basename(dab_path)
    dest_path = os.path.join(output_dir, dab_name)

    os.makedirs(output_dir, exist_ok=True)

    if os.path.exists(dest_path):
        print(f"[Reviewer] WARNING: {dest_path} already exists. Overwriting.")
        shutil.rmtree(dest_path)

    shutil.move(dab_path, dest_path)
    print(f"[Reviewer] Moved {dab_path} -> {dest_path}")
    return dest_path


def process_single_dab(dab_path):
    """Review a single DAB and produce REVIEW.md (no move)."""
    print(f"\n[Reviewer] Reviewing DAB: {dab_path}")

    if not os.path.isdir(dab_path):
        print(f"[Reviewer] ERROR: Directory not found: {dab_path}")
        return False

    context = read_dab_context(dab_path)
    if not context["databricks_yml"]:
        print(f"[Reviewer] ERROR: No databricks.yml found in {dab_path}")
        return False

    issues = review_dab(context)
    review_content = generate_review_md(context, issues)
    write_review(dab_path, review_content)

    if issues:
        print(f"[Reviewer] Found {len(issues)} issue(s) in {dab_path}")
    else:
        print(f"[Reviewer] No issues found in {dab_path}")
    return True


def process_batch(ai_docs_dir, output_dir):
    """Review all DABs in ai_docs and move them to output_dir."""
    dabs = discover_dabs(ai_docs_dir)
    if not dabs:
        print(f"[Reviewer] No DABs found in {ai_docs_dir}")
        return

    print(f"[Reviewer] Found {len(dabs)} DAB(s) to review:")
    for dab in dabs:
        print(f"  - {dab}")

    for dab_path in dabs:
        print(f"\n[Reviewer] Reviewing DAB: {dab_path}")

        context = read_dab_context(dab_path)
        if not context["databricks_yml"]:
            print(f"[Reviewer] ERROR: No databricks.yml in {dab_path}, skipping.")
            continue

        issues = review_dab(context)
        review_content = generate_review_md(context, issues)
        write_review(dab_path, review_content)

        # Move to output directory
        dest = move_dab(dab_path, output_dir)
        print(f"[Reviewer] DAB reviewed and moved to {dest}")

    print(f"\n[Reviewer] Batch review complete.")


def main():
    args = parse_args()

    if args.dab_path:
        # Single DAB mode
        process_single_dab(args.dab_path)
    else:
        # Batch mode
        process_batch(args.ai_docs_dir, args.output_dir)


if __name__ == "__main__":
    main()
