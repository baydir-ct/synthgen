# Profiling your tables for a Genie demo: instructions

We want to build a Genie demo that looks like your data without using your data. This notebook reads
your tables and writes **statistics only**: column types, how often values are empty, typical ranges,
most common categories, and how tables link to each other. It never exports rows, names, emails,
phone numbers, addresses or other personal values. You review the result before sending anything.

It takes about 30 minutes, most of it waiting for the notebook to run.

## What you received

| File | What it is |
| --- | --- |
| `synthgen-0.1.0-py3-none-any.whl` | The code, as a Python package |
| `01_profile_pass1.py` | The notebook you run |
| `INSTRUCTIONS.md` | This document |

## What you need before starting

- A Databricks workspace with **Unity Catalog**, and a cluster on **Databricks Runtime 15.4 LTS or newer**
  (a serverless notebook works too).
- **Read access** (`SELECT`, plus `USE CATALOG` / `USE SCHEMA`) to the tables in scope.
- A folder you can **write** to for the results. Either:
  - a Unity Catalog volume, e.g. `/Volumes/<catalog>/<schema>/<volume>/synthgen` (needs `WRITE VOLUME`), or
  - your own workspace folder, e.g. `/Workspace/Users/<your.email>/synthgen_output`.
- The cluster must be able to install Python packages from PyPI (the package needs `pydantic` and
  `pyyaml`; most runtimes already have `pyyaml`). If PyPI is blocked in your workspace, tell us.

The notebook only reads your tables. It does not create, change or delete any table, view or file
outside the results folder.

## Step 1: Upload the two files

1. In the left sidebar, open **Workspace → Shared** and create a folder named **`synthgen`**.
2. In that folder, choose **⋮ → Import**, and import **both** files:
   `synthgen-0.1.0-py3-none-any.whl` and `01_profile_pass1.py`.
   The `.py` file becomes a notebook.

You should end up with `/Workspace/Shared/synthgen/synthgen-0.1.0-py3-none-any.whl` and the notebook
next to it. If you cannot write to `Shared`, use your own user folder instead and change the path on
the `%pip install` line at the top of the notebook to match.

## Step 2: Open the notebook and fill in the boxes

Open `01_profile_pass1`, attach it to your cluster, and run the first two cells once. The boxes
appear at the top. Fill in:

1. **Tables**: the tables to profile, separated by commas. Use `*` for a whole schema.
   Examples: `main.sales.*` or `main.sales.orders, main.sales.customers, main.crm.contacts`
2. **Output folder**: the folder you can write to (see above).
3. **Extra sensitive columns** *(optional)*: any column whose values must not leave, as
   `catalog.schema.table.column`, separated by commas. The notebook already detects personal data
   such as names, emails, phones, addresses, IDs, IBANs, birth dates and IPs, both from column names
   and from the values themselves. It also honours Unity Catalog PII tags. Use this box for anything
   else that is confidential, for example internal project or client names.
4. **Config YAML**: leave empty.

## Step 3: Run it

Click **Run all**. The notebook prints one line per table: rows, columns, and which columns it
treated as sensitive. Small and medium tables take a few minutes. Tables with hundreds of millions of
rows take longer; tell us if that becomes a problem and we will send a sampling setting.

## Step 4: Review the results

Open the `review/` folder inside your output folder. It has one `.json` file per table plus
`relationships.json`. They are plain text; open them in the workspace file viewer or download them.

What you will see, and what to check:

- **Sensitive columns** (`"sensitive": true`) only show length and a format such as `"AAAA.AAAA@AAAA.AAA"`
  (letters become `A`, digits become `9`). No values.
- **Categorical columns** list their most common values with shares, e.g. `"value": "France", "share": 0.74`.
  Values used by fewer than 10 rows are never listed. **This is the main thing to check:** are you
  comfortable sending these category names (countries, product categories, statuses, channels...)?
- **Comments**: table and column descriptions from Unity Catalog are copied as they are. Check
  that none of them contain example personal data or confidential text.
- **Numbers and dates** show percentiles (for example the 1st, 50th and 99th) and shares by month,
  weekday and hour. Extreme values are cut off.

If something should not leave, **don't edit the JSON by hand**. Instead, add the column to box 3
(*Extra sensitive columns*) and run the notebook again. The column will then show no values at all.
To leave out a whole table, remove it from box 1 and rerun.

## Step 5: Package and send

When you are happy with the files, run the **last cell** of the notebook. It creates
`synthgen_profiles.zip` in your output folder, containing exactly the reviewed `review/` files.

Download it:
- **Volume**: Catalog Explorer → your volume → the file → **Download**.
- **Workspace folder**: right-click the file → **Download**.

Send us **only `synthgen_profiles.zip`**. You can delete the output folder afterwards.

## Questions

If anything fails, send us the error message shown in the notebook (it never contains table data).
