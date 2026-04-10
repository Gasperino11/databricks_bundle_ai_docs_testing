"""
DAB Documentation Writer Agent
===============================
Scans the /ai_docs/ directory for Databricks Asset Bundles that need documentation.
For each DAB, reads the bundle configuration, source code, resource definitions,
and the README.md template, then generates documentation by following the
instructions embedded in the template's markdown comments.

After writing documentation for all DABs, kicks off the Reviewer Agent.

Usage:
    python agents/writer_agent.py [--ai-docs-dir <path>] [--template <path>] [--skip-review]
"""

import argparse
import os
import re
import sys


def parse_args():
    parser = argparse.ArgumentParser(description="DAB Documentation Writer Agent")
    parser.add_argument(
        "--ai-docs-dir",
        default="ai_docs",
        help="Path to the directory containing DABs to document (default: ai_docs)",
    )
    parser.add_argument(
        "--template",
        default="templates/README_TEMPLATE.md",
        help="Path to the README template (default: templates/README_TEMPLATE.md)",
    )
    parser.add_argument(
        "--skip-review",
        action="store_true",
        help="Skip invoking the reviewer agent after writing documentation",
    )
    return parser.parse_args()


def discover_dabs(ai_docs_dir):
    """Discover DAB subdirectories in the ai_docs folder."""
    dabs = []
    if not os.path.isdir(ai_docs_dir):
        print(f"[Writer] Directory not found: {ai_docs_dir}")
        return dabs

    for entry in sorted(os.listdir(ai_docs_dir)):
        dab_path = os.path.join(ai_docs_dir, entry)
        databricks_yml = os.path.join(dab_path, "databricks.yml")
        if os.path.isdir(dab_path) and os.path.isfile(databricks_yml):
            dabs.append(dab_path)

    return dabs


def read_file(filepath):
    """Read and return the contents of a file, or None if it doesn't exist."""
    if not os.path.isfile(filepath):
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()


def collect_files_by_extension(directory, extensions):
    """Collect all files in a directory tree matching the given extensions."""
    files = {}
    if not os.path.isdir(directory):
        return files
    for root, _dirs, filenames in os.walk(directory):
        for fname in sorted(filenames):
            if any(fname.endswith(ext) for ext in extensions):
                fpath = os.path.join(root, fname)
                rel_path = os.path.relpath(fpath, os.path.dirname(directory))
                files[rel_path] = read_file(fpath)
    return files


def read_dab_context(dab_path):
    """
    Read all relevant files from a DAB directory and return a structured context dict.
    """
    dab_name = os.path.basename(dab_path)
    context = {
        "dab_name": dab_name,
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


def build_folder_structure(dab_path):
    """Generate a tree-style folder structure string for the DAB."""
    dab_name = os.path.basename(dab_path)
    lines = [f"{dab_name}/"]
    entries = []
    for root, dirs, files in os.walk(dab_path):
        dirs[:] = sorted(dirs)
        level = root.replace(dab_path, "").count(os.sep)
        indent = "│   " * level
        basename = os.path.basename(root)
        if root != dab_path:
            entries.append((level, f"{indent}├── {basename}/"))
        for f in sorted(files):
            entries.append((level + 1, f"{'│   ' * (level + 1)}├── {f}"))

    # Clean up the tree formatting
    for _, line in entries:
        lines.append(line)

    return "\n".join(lines)


def extract_bundle_name(databricks_yml_content):
    """Extract the bundle name from databricks.yml content."""
    if not databricks_yml_content:
        return "Unknown Bundle"
    match = re.search(r"^\s*name:\s*(.+)$", databricks_yml_content, re.MULTILINE)
    if match:
        return match.group(1).strip()
    return "Unknown Bundle"


def extract_targets(databricks_yml_content):
    """Extract target environments from databricks.yml."""
    targets = []
    if not databricks_yml_content:
        return targets
    # Simple YAML parsing for targets section
    in_targets = False
    current_target = None
    current_host = None
    for line in databricks_yml_content.split("\n"):
        stripped = line.strip()
        if line.startswith("targets:"):
            in_targets = True
            continue
        if in_targets:
            if line and not line.startswith(" ") and not line.startswith("\t"):
                in_targets = False
                continue
            # Top-level target name (2-space indent under targets)
            indent = len(line) - len(line.lstrip())
            if indent == 2 and stripped.endswith(":") and not stripped.startswith("#"):
                if current_target:
                    targets.append({"name": current_target, "host": current_host or "N/A"})
                current_target = stripped.rstrip(":")
                current_host = None
            elif "host:" in stripped and current_target:
                current_host = stripped.split("host:")[-1].strip()
    if current_target:
        targets.append({"name": current_target, "host": current_host or "N/A"})
    return targets


def extract_jobs(databricks_yml_content):
    """Extract job definitions from databricks.yml."""
    jobs = []
    if not databricks_yml_content:
        return jobs

    # Extract only the jobs section
    if "jobs:" not in databricks_yml_content:
        return jobs

    in_jobs = False
    current_job = None
    current_desc = None
    current_schedule = None
    current_timezone = None
    for line in databricks_yml_content.split("\n"):
        stripped = line.strip()
        indent = len(line) - len(line.lstrip()) if line.strip() else 0

        # Detect start of jobs section (under resources)
        if stripped == "jobs:":
            in_jobs = True
            continue

        if in_jobs:
            # Exit jobs section when we hit another top-level key at same or lower indent
            if indent <= 2 and stripped and not stripped.startswith("#") and stripped != "jobs:":
                if stripped.endswith(":") and not stripped.startswith("-"):
                    in_jobs = False
                    if current_job:
                        jobs.append({
                            "name": current_job,
                            "description": current_desc or "",
                            "schedule": current_schedule,
                            "timezone": current_timezone,
                        })
                        current_job = None
                    continue

            # Job name at indent 4
            if indent == 4 and stripped.endswith(":") and not stripped.startswith("#") and not stripped.startswith("-"):
                if current_job:
                    jobs.append({
                        "name": current_job,
                        "description": current_desc or "",
                        "schedule": current_schedule,
                        "timezone": current_timezone,
                    })
                current_job = stripped.rstrip(":")
                current_desc = None
                current_schedule = None
                current_timezone = None
            elif "description:" in stripped and current_job:
                current_desc = stripped.split("description:")[-1].strip().strip('"').strip("'")
            elif "quartz_cron_expression:" in stripped:
                current_schedule = stripped.split("quartz_cron_expression:")[-1].strip().strip('"').strip("'")
            elif "timezone_id:" in stripped:
                current_timezone = stripped.split("timezone_id:")[-1].strip().strip('"').strip("'")

    if current_job:
        jobs.append({
            "name": current_job,
            "description": current_desc or "",
            "schedule": current_schedule,
            "timezone": current_timezone,
        })
    return jobs


def extract_tasks(databricks_yml_content):
    """Extract task names and dependencies from databricks.yml."""
    tasks = []
    if not databricks_yml_content:
        return tasks
    current_task = None
    depends_on = []
    python_file = None
    in_depends_on = False
    for line in databricks_yml_content.split("\n"):
        stripped = line.strip()
        indent = len(line) - len(line.lstrip())
        # A top-level task entry (- task_key: ...) at task list indent level
        if stripped.startswith("- task_key:") and not in_depends_on:
            if current_task:
                tasks.append({
                    "name": current_task,
                    "depends_on": depends_on,
                    "python_file": python_file,
                })
            current_task = stripped.split("task_key:")[-1].strip()
            depends_on = []
            python_file = None
            in_depends_on = False
        elif stripped == "depends_on:":
            in_depends_on = True
        elif in_depends_on:
            if stripped.startswith("- task_key:"):
                dep = stripped.split("task_key:")[-1].strip()
                depends_on.append(dep)
            elif stripped and not stripped.startswith("-") and not stripped.startswith("#"):
                in_depends_on = False
                if "python_file:" in stripped:
                    python_file = stripped.split("python_file:")[-1].strip()
        elif "python_file:" in stripped:
            python_file = stripped.split("python_file:")[-1].strip()
    if current_task:
        tasks.append({
            "name": current_task,
            "depends_on": depends_on,
            "python_file": python_file,
        })
    return tasks


def extract_s3_paths(src_files):
    """Extract S3 paths from source files."""
    paths = set()
    for _filename, content in src_files.items():
        if content:
            for match in re.finditer(r's3://[^\s"\']+', content):
                paths.add(match.group())
    return sorted(paths)


def extract_table_references(src_files):
    """Extract Unity Catalog table references from source files."""
    tables = set()
    for _filename, content in src_files.items():
        if content:
            # Look for catalog.schema.table patterns
            for match in re.finditer(r'f"({[^}]+\.}{[^}]+\.}[^"]+)"', content):
                tables.add(match.group(1))
            # Look for string format patterns
            for match in re.finditer(r'"(\w+\.\w+\.\w+)"', content):
                tables.add(match.group(1))
    return sorted(tables)


def generate_mermaid_diagram(context):
    """Generate a Mermaid diagram based on the DAB's tasks and dependencies."""
    tasks = extract_tasks(context["databricks_yml"])
    s3_paths = extract_s3_paths(context["src_files"])

    lines = ["```mermaid", "graph LR"]

    # Add S3 source
    if s3_paths:
        lines.append(f'    S3[("AWS S3<br/>{s3_paths[0]}")]')

    # Add task nodes
    task_ids = {}
    for i, task in enumerate(tasks):
        tid = f"T{i}"
        task_ids[task["name"]] = tid
        lines.append(f'    {tid}["{task["name"]}"]')

    # Add output
    lines.append('    OUT[("Unity Catalog<br/>Gold Tables")]')

    # Add edges
    if s3_paths and tasks:
        first_task_id = task_ids.get(tasks[0]["name"], "T0")
        lines.append(f"    S3 -->|Ingest| {first_task_id}")

    for task in tasks:
        tid = task_ids.get(task["name"], "")
        for dep in task["depends_on"]:
            dep_id = task_ids.get(dep, "")
            if dep_id:
                lines.append(f"    {dep_id} --> {tid}")

    # Connect last task to output
    if tasks:
        last_task_id = task_ids.get(tasks[-1]["name"], f"T{len(tasks) - 1}")
        lines.append(f"    {last_task_id} -->|Publish| OUT")

    # Add subgraph
    if task_ids:
        lines.append("")
        lines.append("    subgraph Databricks Workflow Job")
        for tid in task_ids.values():
            lines.append(f"        {tid}")
        lines.append("    end")

    lines.append("```")
    return "\n".join(lines)


def generate_documentation(context, template_content):
    """
    Generate documentation for a DAB by filling in the template sections
    based on the DAB's configuration and source code.
    """
    bundle_name = extract_bundle_name(context["databricks_yml"])
    targets = extract_targets(context["databricks_yml"])
    jobs = extract_jobs(context["databricks_yml"])
    s3_paths = extract_s3_paths(context["src_files"])
    folder_tree = build_folder_structure(context["dab_path"])

    doc_lines = []

    # Title
    doc_lines.append(f"# {bundle_name}")
    doc_lines.append("")

    # Description & Purpose
    doc_lines.append("## Description & Purpose")
    doc_lines.append("")
    if jobs:
        job = jobs[0]
        doc_lines.append(
            job.get("description", "This bundle manages a Databricks data pipeline.")
        )
    else:
        doc_lines.append("This Databricks Asset Bundle manages a data pipeline.")
    doc_lines.append("")
    doc_lines.append("**Key technologies:** Databricks Workflows, Delta Lake, Unity Catalog")
    if any("dlt" in f.lower() for f in context["src_files"]):
        doc_lines[-1] += ", Delta Live Tables"
    doc_lines.append("")

    # Folder Structure
    doc_lines.append("## Folder Structure")
    doc_lines.append("")
    doc_lines.append("```")
    doc_lines.append(folder_tree)
    doc_lines.append("```")
    doc_lines.append("")

    # File description table
    doc_lines.append("| Path | Description |")
    doc_lines.append("|------|-------------|")
    doc_lines.append("| `databricks.yml` | Bundle configuration and deployment targets |")
    doc_lines.append("| `README.md` | Documentation for this bundle |")
    for fname, content in context["src_files"].items():
        desc = "Source code"
        if content:
            # Extract module docstring for description
            match = re.search(r'"""(.*?)"""', content, re.DOTALL)
            if match:
                first_line = match.group(1).strip().split("\n")[0].strip()
                if first_line:
                    desc = first_line
        doc_lines.append(f"| `{fname}` | {desc} |")
    for fname in context["resource_files"]:
        doc_lines.append(f"| `{fname}` | Resource configuration |")
    doc_lines.append("")

    # Job & Pipeline Diagram
    doc_lines.append("## Job & Pipeline Diagram")
    doc_lines.append("")
    doc_lines.append(generate_mermaid_diagram(context))
    doc_lines.append("")

    # How to Deploy
    doc_lines.append("## How to Deploy")
    doc_lines.append("")
    doc_lines.append("### Prerequisites")
    doc_lines.append("")
    doc_lines.append("- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) installed and configured")
    doc_lines.append("- Access to the target Databricks workspace(s)")
    doc_lines.append("- Authentication configured (token or OAuth)")
    doc_lines.append("")
    doc_lines.append("### Validate")
    doc_lines.append("")
    doc_lines.append("```bash")
    doc_lines.append("databricks bundle validate")
    doc_lines.append("```")
    doc_lines.append("")
    doc_lines.append("### Deploy")
    doc_lines.append("")
    for target in targets:
        doc_lines.append(f"```bash")
        doc_lines.append(f"databricks bundle deploy --target {target['name']}")
        doc_lines.append("```")
        doc_lines.append("")
    doc_lines.append("### Run")
    doc_lines.append("")
    for job in jobs:
        for target in targets:
            doc_lines.append("```bash")
            doc_lines.append(f"databricks bundle run --target {target['name']} {job['name']}")
            doc_lines.append("```")
            doc_lines.append("")

    doc_lines.append("### Targets")
    doc_lines.append("")
    doc_lines.append("| Target | Workspace Host | Description |")
    doc_lines.append("|--------|---------------|-------------|")
    for target in targets:
        mode = "Development" if target["name"] == "dev" else "Production"
        doc_lines.append(f"| `{target['name']}` | `{target['host']}` | {mode} environment |")
    doc_lines.append("")

    # Schedule
    doc_lines.append("## Schedule")
    doc_lines.append("")
    doc_lines.append("| Job/Pipeline Name | Schedule (Cron) | Timezone | Description |")
    doc_lines.append("|-------------------|----------------|----------|-------------|")
    for job in jobs:
        sched = job.get("schedule") or "Manual trigger"
        tz = job.get("timezone") or "N/A"
        doc_lines.append(f"| `{job['name']}` | `{sched}` | `{tz}` | {job.get('description', '')} |")
    doc_lines.append("")

    # Data Sources
    doc_lines.append("## Data Sources")
    doc_lines.append("")
    doc_lines.append("| Source Name | Type | Location/Path | Format | Description |")
    doc_lines.append("|-------------|------|--------------|--------|-------------|")
    for s3_path in s3_paths:
        doc_lines.append(f"| Raw Events | AWS S3 | `{s3_path}` | Parquet | Raw event data ingested from S3 |")
    doc_lines.append("")

    # Data Outputs
    doc_lines.append("## Data Outputs")
    doc_lines.append("")
    doc_lines.append("| Output Name | Type | Location/Path | Format | Description |")
    doc_lines.append("|-------------|------|--------------|--------|-------------|")

    # Parse outputs from source code
    output_tables = []
    seen = set()
    for _fname, content in context["src_files"].items():
        if content:
            # First, find f-string table name assignments (e.g., target_table = f"...")
            table_vars = {}
            for m in re.finditer(
                r'(\w+)\s*=\s*f"([^"]*\{[^"]*\}[^"]*)"', content
            ):
                var_name = m.group(1)
                fstring_val = m.group(2)
                table_vars[var_name] = fstring_val

            # Find saveAsTable calls
            for m in re.finditer(r'saveAsTable\(\s*([^)]+)\s*\)', content):
                ref = m.group(1).strip().strip('"').strip("'")
                # Resolve variable references
                if ref in table_vars:
                    resolved = table_vars[ref]
                else:
                    resolved = ref
                if resolved not in seen:
                    seen.add(resolved)
                    output_tables.append(resolved)

    for table in output_tables:
        # Make f-string references human-readable
        readable = re.sub(r'\{[^}]+\}', '*', table)
        table_name = readable.split(".")[-1] if "." in readable else readable
        doc_lines.append(f"| `{table_name}` | Unity Catalog | `{readable}` | Delta | Managed Delta table |")
    doc_lines.append("")

    # Managed Assets
    doc_lines.append("## Managed Assets")
    doc_lines.append("")
    doc_lines.append("| Asset Type | Asset Name | Description |")
    doc_lines.append("|------------|-----------|-------------|")
    for job in jobs:
        doc_lines.append(f"| Workflow Job | `{job['name']}` | {job.get('description', 'Databricks workflow job')} |")

    # Check for pipelines
    if context["databricks_yml"]:
        if "pipelines:" in context["databricks_yml"]:
            for match in re.finditer(
                r"^\s{4}(\w+):\s*$",
                context["databricks_yml"].split("pipelines:")[1].split("\n\n")[0],
                re.MULTILINE,
            ):
                doc_lines.append(
                    f"| DLT Pipeline | `{match.group(1)}` | Delta Live Tables pipeline |"
                )
    doc_lines.append("")

    # Authors
    doc_lines.append("## Authors")
    doc_lines.append("")
    doc_lines.append("| Name | Role | Contact |")
    doc_lines.append("|------|------|---------|")
    doc_lines.append("| *To be filled in* | Owner / Maintainer | |")
    doc_lines.append("")

    # References
    doc_lines.append("## References")
    doc_lines.append("")
    doc_lines.append("- [Databricks Asset Bundles Documentation](https://docs.databricks.com/en/dev-tools/bundles/index.html)")
    doc_lines.append("- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html)")
    doc_lines.append("- [Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html)")
    doc_lines.append("- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)")
    doc_lines.append("")

    return "\n".join(doc_lines)


def write_documentation(dab_path, documentation):
    """Write generated documentation to the DAB's README.md."""
    readme_path = os.path.join(dab_path, "README.md")
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write(documentation)
    print(f"[Writer] Wrote documentation to {readme_path}")


def process_dab(dab_path, template_content):
    """Process a single DAB: read context, generate docs, write README."""
    print(f"\n[Writer] Processing DAB: {dab_path}")

    context = read_dab_context(dab_path)

    if not context["databricks_yml"]:
        print(f"[Writer] ERROR: No databricks.yml found in {dab_path}, skipping.")
        return False

    documentation = generate_documentation(context, template_content)
    write_documentation(dab_path, documentation)
    return True


def main():
    args = parse_args()
    ai_docs_dir = args.ai_docs_dir
    template_path = args.template

    print(f"[Writer] Scanning for DABs in: {ai_docs_dir}")

    # Read template
    template_content = read_file(template_path)
    if not template_content:
        print(f"[Writer] WARNING: Template not found at {template_path}, using default structure.")
        template_content = ""

    # Discover DABs
    dabs = discover_dabs(ai_docs_dir)
    if not dabs:
        print("[Writer] No DABs found to document.")
        return

    print(f"[Writer] Found {len(dabs)} DAB(s) to document:")
    for dab in dabs:
        print(f"  - {dab}")

    # Process each DAB
    success_count = 0
    for dab_path in dabs:
        if process_dab(dab_path, template_content):
            success_count += 1

    print(f"\n[Writer] Documentation complete. Processed {success_count}/{len(dabs)} DABs.")

    # Kick off reviewer agent
    if not args.skip_review:
        print("\n[Writer] Kicking off Reviewer Agent...")
        reviewer_script = os.path.join(os.path.dirname(__file__), "reviewer_agent.py")
        if os.path.isfile(reviewer_script):
            os.system(f'{sys.executable} "{reviewer_script}" --ai-docs-dir "{ai_docs_dir}"')
        else:
            print(f"[Writer] WARNING: Reviewer agent not found at {reviewer_script}")


if __name__ == "__main__":
    main()
