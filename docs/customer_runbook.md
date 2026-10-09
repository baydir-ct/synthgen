# Profiling your tables for a Genie demo: instructions

You receive one file: **`01_profile_tables.ipynb`**. It reads the tables you choose and writes
**statistics only** (column types, share of empty values, typical ranges, most common categories, links
between tables). It never exports rows or personal values, and it only reads your tables.

## What you need
- A Databricks workspace with Unity Catalog and a cluster on Runtime 15.4 LTS or newer (or serverless).
- Read access to the tables, and a folder you can write to (a Unity Catalog volume or your workspace folder).
- The cluster must be able to install `pydantic` from PyPI (first cell of the notebook).

## Steps
1. **Import** the notebook: Workspace → your folder → ⋮ → **Import** → choose `01_profile_tables.ipynb`.
2. **Attach** it to a cluster.
3. **Edit cell 2 (Configuration)**: `TABLES` (e.g. `"main.sales.*"`) and `OUTPUT_FOLDER`.
   Optionally list extra confidential columns in `EXTRA_SENSITIVE_COLUMNS`.
4. **Run all.** A few minutes per table.
5. **Review** the `.json` files in `<OUTPUT_FOLDER>/review/`. The notebook explains what to check
   (mainly: category names and table/column descriptions). To hide a column, add it to
   `EXTRA_SENSITIVE_COLUMNS` and run again; don't edit the JSON.
6. **Run the last cell**, download `synthgen_profiles.zip` from the output folder, and send only that file.

If anything fails, send the error message shown in the notebook (it never contains table data).
