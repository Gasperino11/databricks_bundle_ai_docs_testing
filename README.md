# databricks_bundle_ai_docs_testing

Automated documentation writing and review for Databricks Asset Bundles (DABs) using GitHub Copilot agents.

## Overview

This repository uses GitHub Copilot coding agents — triggered via GitHub Actions workflows and guided by detailed prompt files — to automate the process of writing and reviewing documentation for Databricks Asset Bundles:

1. **Writer Agent** (`.github/agents/writer-agent.md`) — A Copilot agent prompt that instructs the AI to scan DABs in `data_eng/`, read their configuration, source code, and resources, then generate comprehensive `README.md` documentation following a standardized template.
2. **Reviewer Agent** (`.github/agents/reviewer-agent.md`) — A Copilot agent prompt that instructs the AI to cross-reference documentation against code and produce a `REVIEW.md` highlighting inconsistencies (with file and line references).

## Repository Structure

```
├── .github/
│   ├── agents/
│   │   ├── writer-agent.md           # Prompt/instructions for the writer agent
│   │   └── reviewer-agent.md         # Prompt/instructions for the reviewer agent
│   └── workflows/
│       ├── write-dab-docs.yml        # Workflow to run writer then reviewer agent
│       └── review-dab-docs.yml       # Manual workflow to run reviewer agent standalone
├── templates/
│   └── README_TEMPLATE.md            # README template with section instructions
├── data_eng/                         # DAB folders live here
│   └── <dab_name>/                   # Individual DAB folders
├── scripts/
│   └── dispatch_agent.py             # Python script to dispatch Copilot agents
└── README.md                         # This file
```

## Workflow

### Automatic (PR-driven)

When a new pull request is opened that adds or modifies files under `data_eng/`:

1. The **Write and Review DAB Documentation** workflow triggers automatically.
2. **Job 1 (Writer)**: The writer agent generates documentation (`README.md`) for any new DABs in the PR.
3. **Job 2 (Reviewer)**: After the writer completes, the reviewer agent reviews the generated documentation and produces a `REVIEW.md` report.

### Manual Trigger

Both workflows can also be triggered manually via **Actions → Run workflow**:

#### Writer + Reviewer (Manual)

1. Go to **Actions** → **Write and Review DAB Documentation**
2. Click **Run workflow**
3. Set `dab_path` to the path of a DAB folder containing a `databricks.yml` (e.g., `data_eng/s3_ingestion_pipeline`)
4. The writer agent generates documentation, then the reviewer agent reviews it

If `dab_path` is left empty, both agents scan all DABs in `data_eng/`.

#### Reviewer Only (Manual)

1. Go to **Actions** → **Review DAB Documentation**
2. Click **Run workflow**
3. Set `dab_path` to the path of a specific DAB to review, or leave empty to review all DABs in `data_eng/`

The reviewer agent will:
- Read the `README.md` and cross-reference it against the code and configs
- Produce a `REVIEW.md` with any issues found (including file and line references)

## Agent Prompts

The agent behavior is defined by detailed markdown prompt files in `.github/agents/`:

| File | Description |
|------|-------------|
| `.github/agents/writer-agent.md` | Full instructions for the documentation writer — how to read each file type, what to extract, and how to fill in each README section |
| `.github/agents/reviewer-agent.md` | Full instructions for the documentation reviewer — what checks to perform, how to report issues, and the REVIEW.md output format |

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

An example DAB is included at `data_eng/s3_ingestion_pipeline/` that demonstrates:
- A workflow job with bronze → silver → gold medallion architecture
- AWS S3 ingestion of Parquet event data
- Spark Declarative Pipelines (formerly Delta Live Tables)
- Unity Catalog integration
- Multi-target deployment (dev/prod)
