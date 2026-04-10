# databricks_bundle_ai_docs_testing

Automated documentation writing and review for Databricks Asset Bundles (DABs) using GitHub Copilot agents.

## Overview

This repository uses GitHub Copilot coding agents — triggered via GitHub Actions workflows and guided by detailed prompt files — to automate the process of writing and reviewing documentation for Databricks Asset/Automation Bundles:

1. **Writer Agent** (`agents/writer-agent.md`) — A Copilot agent prompt that instructs the AI to scan DABs in `ai_docs/`, read their configuration, source code, and resources, then generate comprehensive `README.md` documentation following a standardized template.
2. **Reviewer Agent** (`agents/reviewer-agent.md`) — A Copilot agent prompt that instructs the AI to cross-reference documentation against code, produce a `REVIEW.md` highlighting inconsistencies (with file and line references), and move reviewed DABs to `data_eng/`.

## Repository Structure

```
├── .github/
│   └── workflows/
│       ├── write-dab-docs.yml    # Workflow to trigger the writer agent
│       └── review-dab-docs.yml   # Workflow to trigger the reviewer agent
├── agents/
│   ├── writer-agent.md           # Prompt/instructions for the writer agent
│   └── reviewer-agent.md         # Prompt/instructions for the reviewer agent
├── templates/
│   └── README_TEMPLATE.md        # README template with section instructions
├── ai_docs/                      # DABs awaiting documentation
│   └── <dab_name>/               # Individual DAB folders
├── data_eng/                     # Reviewed & documented DABs
└── README.md                     # This file
```

## Workflow

### 1. Place DABs in `ai_docs/`

Copy or create your Databricks Asset Bundle in a subfolder under `ai_docs/`. Each DAB should have:
- `databricks.yml` — Bundle configuration
- `src/` — Python source code
- `resources/` — YAML resource definitions
- `README.md` — Copy from `templates/README_TEMPLATE.md`

### 2. Trigger the Writer Agent

Use one of the following methods:

**Via GitHub Actions (recommended):**
1. Go to **Actions** → **Write DAB Documentation**
2. Click **Run workflow**
3. Optionally set the `ai_docs_dir` input (defaults to `ai_docs`)
4. The workflow creates a Copilot-assigned issue with instructions from `agents/writer-agent.md`
5. Copilot reads the DAB files and generates the README documentation in a PR

**Via GitHub Issues:**
1. Create a new issue with the label `write-dab-docs`
2. In the issue body, mention `@copilot` and reference the DABs to document
3. Copilot will follow the instructions in `agents/writer-agent.md`

The writer agent will:
- Discover all DABs in `ai_docs/` that contain a `databricks.yml`
- Read `databricks.yml`, source files in `src/`, and resource configs in `resources/`
- Generate complete documentation in each DAB's `README.md` following the template
- Trigger the reviewer agent when done (unless `skip_review` is set)

### 3. Trigger the Reviewer Agent

The reviewer runs automatically after the writer, but can also be triggered independently:

**Via GitHub Actions:**
1. Go to **Actions** → **Review DAB Documentation**
2. Click **Run workflow**
3. To review all DABs in `ai_docs/`, leave `dab_path` empty (batch mode)
4. To review a specific DAB already in `data_eng/`, set `dab_path` (e.g., `data_eng/s3_ingestion_pipeline`)

**Via GitHub Issues:**
1. Create a new issue with the label `review-dab-docs`
2. In the issue body, mention `@copilot` and reference the DABs to review

The reviewer agent will:
- Read the `README.md` and cross-reference it against the code and configs
- Produce a `REVIEW.md` with any issues found (including file and line references)
- In batch mode: move the DAB from `ai_docs/` to `data_eng/`
- In single DAB mode: review in-place without moving

## Agent Prompts

The agent behavior is defined by detailed markdown prompt files in the `agents/` directory:

| File | Description |
|------|-------------|
| `agents/writer-agent.md` | Full instructions for the documentation writer — how to read each file type, what to extract, and how to fill in each README section |
| `agents/reviewer-agent.md` | Full instructions for the documentation reviewer — what checks to perform, how to report issues, and the REVIEW.md output format |

## README Template

The template at `templates/README_TEMPLATE.md` includes the following sections with embedded `<!-- INSTRUCTIONS: ... -->` comment blocks:

| Section | Description |
|---------|-------------|
| **DAB Name** | Title from `databricks.yml` |
| **Description & Purpose** | Business context and technologies used |
| **Folder Structure** | Tree view of files with descriptions |
| **Job & Pipeline Diagram** | Mermaid diagram of the workflow |
| **How to Deploy** | Step-by-step deployment instructions |
| **Schedule** | Cron schedules for all jobs |
| **Data Sources** | Input data sources with formats and paths |
| **Data Outputs** | Output tables and datasets |
| **Managed Assets** | All Databricks assets managed by the bundle |
| **Authors** | Team and maintainer information |
| **References** | Links to relevant documentation |

## Example DAB

An example DAB is included at `ai_docs/s3_ingestion_pipeline/` that demonstrates:
- A workflow job with bronze → silver → gold medallion architecture
- AWS S3 ingestion of Parquet event data
- Delta Live Tables pipeline
- Unity Catalog integration
- Multi-target deployment (dev/prod)
