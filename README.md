# databricks_bundle_ai_docs_testing

Automated documentation writing and review for Databricks Asset Bundles (DABs).

## Overview

This repository provides two AI-powered agents that automate the process of writing and reviewing documentation for Databricks Asset/Automation Bundles:

1. **Writer Agent** — Scans the `ai_docs/` directory, reads each DAB's configuration, source code, and resources, then generates a comprehensive `README.md` following a standardized template.
2. **Reviewer Agent** — Reviews each DAB's documentation against its code, produces a `REVIEW.md` highlighting any inconsistencies, and moves reviewed DABs to `data_eng/`.

## Repository Structure

```
├── agents/
│   ├── writer_agent.py      # Documentation writer agent
│   └── reviewer_agent.py    # Documentation reviewer agent
├── templates/
│   └── README_TEMPLATE.md   # README template with instructions
├── ai_docs/                  # DABs awaiting documentation
│   └── <dab_name>/          # Individual DAB folders
├── data_eng/                 # Reviewed & documented DABs
└── README.md                 # This file
```

## Workflow

### 1. Place DABs in `ai_docs/`

Copy or create your Databricks Asset Bundle in a subfolder under `ai_docs/`. Each DAB should have:
- `databricks.yml` — Bundle configuration
- `src/` — Python source code
- `resources/` — YAML resource definitions
- `README.md` — Copy from `templates/README_TEMPLATE.md`

### 2. Run the Writer Agent

```bash
python agents/writer_agent.py
```

The writer agent will:
- Discover all DABs in `ai_docs/`
- Read `databricks.yml`, source files, and resource configs
- Generate documentation in each DAB's `README.md`
- Automatically invoke the Reviewer Agent

**Options:**
```bash
python agents/writer_agent.py --ai-docs-dir ai_docs --template templates/README_TEMPLATE.md
python agents/writer_agent.py --skip-review  # Skip the reviewer step
```

### 3. Run the Reviewer Agent

The reviewer agent runs automatically after the writer, but can also be run independently:

```bash
# Review all DABs in ai_docs/ and move to data_eng/
python agents/reviewer_agent.py

# Review a single DAB already in data_eng/
python agents/reviewer_agent.py --dab-path data_eng/s3_ingestion_pipeline
```

The reviewer agent will:
- Read the README.md and cross-reference it against the code and configs
- Produce a `REVIEW.md` with any issues found (including file and line references)
- Move the DAB from `ai_docs/` to `data_eng/`

## README Template

The template at `templates/README_TEMPLATE.md` includes the following sections with embedded instructions:

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
