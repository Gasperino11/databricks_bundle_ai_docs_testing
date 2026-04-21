# Skill: review-dab-documentation

Use this skill to verify that a DAB's `README.md` accurately reflects its code and configuration, and to produce a `REVIEW.md` report summarizing any issues found.

## Template Reference

The canonical README template is located at `templates/README_TEMPLATE.md` in this repository. Use it as the authoritative reference for what sections and content a complete DAB README must contain. Any `README.md` that still contains `<!-- INSTRUCTIONS: ... -->` comment blocks has not been fully written and should be flagged immediately.

## Steps

### 1. Read the `README.md`

Read the DAB's `README.md` and parse its structure. Verify it contains all required sections:
- Description & Purpose
- Folder Structure
- Job & Pipeline Diagram
- How to Deploy
- Schedule
- Data Sources
- Data Outputs
- Managed Assets
- Authors
- References

### 2. Read the Code and Configs

Read all files in the DAB:
- `databricks.yml` — bundle configuration
- `src/*.py` — source code files
- `resources/*.yml` — resource configuration files

### 3. Cross-Reference Documentation Against Code

Check for the following issues:

#### Missing or Empty Sections
- Flag any required section that is missing from the README
- Flag any section that exists but has no content (only the header)
- Flag any section that still contains `<!-- INSTRUCTIONS: ... -->` template comments

#### Data Source Inconsistencies
- Find all S3 paths in the source code (e.g., `s3://...`) and verify they are documented in the "Data Sources" section
- Report the specific file and line number where each undocumented S3 path appears

#### Data Output Inconsistencies
- Find all `saveAsTable()` calls in source code and verify the output tables are documented in "Data Outputs"
- Find all `@dlt.table` declarations (Spark Declarative Pipelines, formerly Delta Live Tables) and verify they are documented
- Report the specific file and line number for each undocumented output

#### Job and Asset Inconsistencies
- Verify all jobs defined in `databricks.yml` under `resources.jobs` are listed in "Managed Assets"
- Verify all pipelines defined under `resources.pipelines` are listed
- Report any managed asset in the README that doesn't exist in the config (stale docs)

#### Schedule Inconsistencies
- Verify `quartz_cron_expression` values from job definitions are documented in the "Schedule" section
- Flag any schedule in the README that doesn't match the config

#### Deployment Target Inconsistencies
- Verify all targets from `databricks.yml` are listed in the "How to Deploy" section
- Flag any workspace host URL mismatches

### 4. Generate REVIEW.md

Write a `REVIEW.md` file in the DAB root with the following structure:

```markdown
# Documentation Review: <dab_name>

**Reviewed:** <dab_path>

## ✅ Review Result: PASSED
<!-- Use this if no issues found -->
No issues found. Documentation is consistent with the code and configuration.

## ⚠️ Review Result: <N> Issue(s) Found
<!-- Use this if issues are found. Group by type: -->

### Missing Documentation Sections
- **<section_name> is missing from README**

### Undocumented Data Sources
- **S3 path `s3://...` found in code but not documented in README**
  - File: `<filename>`, Line: <line_number>

### Undocumented Data Outputs
- **Output table `<table_name>` found in code but not documented in README**
  - File: `<filename>`, Line: <line_number>

### Undocumented Managed Assets
- **Job `<job_name>` defined in databricks.yml but not documented in README**
  - File: `databricks.yml`

### Undocumented Schedules
- **Schedule `<cron>` found in databricks.yml but not in README**
  - File: `databricks.yml`

### Template Comments Still Present
- **README still contains <N> unfilled template instruction comment(s)**

## Summary

| Issue Type | Count |
|------------|-------|
| Missing Sections | <n> |
| Undocumented Data Sources | <n> |
| Undocumented Data Outputs | <n> |
| Undocumented Managed Assets | <n> |
| Undocumented Schedules | <n> |
| Template Comments | <n> |
| **Total** | **<total>** |
```

## Important Notes

- Always include file paths and line numbers in issue references so developers can quickly find problems
- When resolving f-string table references (e.g., `f"{catalog}.{schema}.{table}"`), check for the actual table name portion (the last segment after the dots)
- Spark Declarative Pipeline table names from `@dlt.table(name="...")` decorators should also be checked against the docs
- Be thorough but avoid false positives — if a table name appears anywhere in the README (even in a different format), consider it documented
