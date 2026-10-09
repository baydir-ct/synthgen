# Synthetic Data for Genie Demo — Implementation Plan

Oct 9, 2026 · @Bayd

## Context and goal

We will build a reusable toolkit that profiles customer tables in one Databricks workspace and regenerates statistically similar synthetic tables in another, so a Databricks Genie agent can be demoed without customer data.

The customer data cannot leave its workspace. Only aggregate statistics may be exported, after review. The synthetic data must look plausible, keep the same schema, support the same joins, and give believable answers to typical business questions.

The toolkit must work on almost any set of Delta tables registered in Unity Catalog. Dataset-specific behaviour lives in a small config file, never in code.

Deliverables:

- A profiler notebook and library that writes one JSON profile per table.
- A privacy guard that filters profiles before export.
- A generator that turns profiles into Delta tables in the target workspace.
- A validator that compares the synthetic profile with the original.

## Hard constraints

These rules are non-negotiable and must be enforced in code, not left to the user.

1. **No row-level output.** The profiler never writes, prints, displays or returns individual rows or raw values, except approved category labels (rule 3). No `display(df)`, no `.show()`, no `.collect()` of raw columns in shipped code.
2. **Minimum group size k.** Any count, frequency or joint frequency below k (default 10, configurable, never below 5) is suppressed or merged into an "other" bucket.
3. **Sensitive columns export no values.** A column is sensitive if Unity Catalog tags it as PII, if its name matches a PII heuristic (name, email, phone, address, iban, ssn, dni, nif, birth, ip, etc.), or if the config says so. Sensitive columns export only type, null rate, length stats, format pattern and semantic type.
4. **Free text and identifiers export no values.** Columns with high cardinality (distinct ratio above a threshold, default 0.5) are treated like sensitive columns for value export.
5. **Extremes are clipped.** Exported min and max are the 1st and 99th percentiles, not true extremes.
6. **Human review before export.** Every profile is written to a review folder first. Nothing is copied out until a person has approved it (see Privacy guard).
7. **Read-only on the source.** The profiler only reads. It never creates tables, views or files outside its own output folder.

## Architecture

Everything that reads customer data runs in the customer workspace; everything that creates data runs in ours. Reviewed JSON profiles are the only contract between them.

&#91;embedded content: pipeline across the two workspaces · 10 steps\]

The privacy guard is the only way out of the customer workspace, and `questions.yaml` is the only thing that goes back in. The validator compares the synthetic profile against the approved original profile.

## Pass 1: generic profiler

Pass 1 runs with no knowledge of the business questions. Input is a list of tables or a catalog.schema pattern; output is one JSON profile per table plus one relationships file.

Implement it in PySpark, computing all column statistics in as few passes over the data as possible (one `agg` per table where feasible, `percentile_approx` for quantiles, `approx_count_distinct` for cardinality). Allow optional sampling for very large tables, with the sampling fraction recorded in the profile.

**Table level:** full name, row count, column list in order, Unity Catalog table and column comments, tags, declared primary and foreign keys, partition columns.

**Column level, every type:** Spark type, nullable flag, null rate, approximate distinct count, distinct ratio, detected role (key, foreign key, categorical, numeric, date, text, boolean), sensitive flag and the reason.

| Type | Statistics to export |
| --- | --- |
| Numeric | Clipped min and max, mean, std, quantiles at 1, 5, 10, 25, 50, 75, 90, 95, 99, share of zeros, share of negatives, integer or decimal, decimal places observed |
| Categorical (low cardinality, not sensitive) | Top K values (default 50) with frequencies after k-suppression, share in "other", string length min/mean/max |
| Sensitive or high-cardinality string | Length min/mean/max, format pattern top 5 with shares (letters to A, digits to 9, keep punctuation), guessed semantic type (email, phone, name, postcode, uuid, free text) |
| Date | Clipped min and max, quantiles, share by year-month, share by day of week |
| Timestamp | Everything for dates plus share by hour of day, whether the time part is always midnight, timezone if known |
| Boolean | True, false and null shares |

Categorical versus text is decided by distinct count (default up to 200 distinct values) and distinct ratio. Thresholds live in the config.

**Relationships:**

- Primary key candidates: columns or column pairs where distinct count equals row count.
- Foreign keys: take declared constraints first. Otherwise test candidates by name similarity and type match, then compute the containment ratio (share of child values found in the parent key) and keep pairs above 0.9.
- For each foreign key, export the containment ratio, the orphan rate, and quantiles of children per parent (1, 5, 25, 50, 75, 95, 99), plus the share of parents with zero children.

**Cheap within-table rules** (detected, not just configured):

- Date ordering between pairs of date columns in the same table (share of rows where A is on or before B).
- Numeric identities such as `a = b * c` or `a = b + c`, tested on a sample, with the match rate.

## Profile file format

The profile JSON is the only contract between the two workspaces, so it gets a versioned schema from day one. Define it as Pydantic models (or a JSON Schema file) and validate every profile on write and on read.

One file per table, `<catalog>.<schema>.<table>.profile.json`, plus `relationships.json` and, after pass 2, `joint_stats.json`. Sketch:

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-10-09T10:00:00Z",
  "source_table": "cat.sales.orders",
  "row_count": 1250000,
  "sample_fraction": 1.0,
  "k_threshold": 10,
  "comment": "One row per customer order",
  "columns": [
    {
      "name": "amount",
      "type": "decimal(12,2)",
      "role": "numeric",
      "sensitive": false,
      "null_rate": 0.002,
      "distinct_approx": 48211,
      "numeric": {
        "min_clipped": 3.5, "max_clipped": 1840.0,
        "mean": 112.4, "std": 95.1,
        "quantiles": {"0.01": 3.5, "0.5": 79.9, "0.99": 1840.0},
        "zero_share": 0.0, "negative_share": 0.01, "decimals": 2
      }
    }
  ],
  "rules": [
    {"kind": "date_order", "before": "order_date", "after": "ship_date", "match_rate": 0.998}
  ]
}
```

Keep it human-readable (indented, stable key order) because a person on the customer side must review it.

## Deriving demo questions

No one has given us the Genie questions, so we derive 20 to 30 of them from the pass-1 profile and treat them as the spec for pass 2.

Build a small script, `suggest_questions.py`, that runs outside the customer workspace on the approved pass-1 profiles only. It sends the schema, comments, roles and relationships (no values beyond approved labels) to an LLM and asks for realistic business questions a Genie user would ask. Cover these families:

- Totals and counts over time (by month, quarter, year, year over year).
- Top N and rankings by a dimension.
- Comparisons between segments.
- Trends and seasonality (day of week, hour, month).
- Definition-based questions such as active, churned or late customers or orders.
- Questions that need two or three joins.

A person trims the list to the best demo questions. The script then turns the kept questions into a config file, `questions.yaml`, mapping each question to its tables, columns, joins, filters and time grain:

```yaml
- id: q07
  text: Revenue by region for premium customers, by month
  tables: [sales.orders, sales.customers]
  joins: [[orders.customer_id, customers.id]]
  measures: [orders.amount]
  dimensions: [customers.region, customers.tier]
  filters: [customers.tier = 'premium']
  time: {column: orders.order_date, grain: month}
  definitions: []
```

Before the demo, show the trimmed question list to someone on the business side for a quick sanity check.

## Pass 2: targeted profiler

Pass 2 reads `questions.yaml` and computes only what those questions need, back on the customer workspace. Output goes to `joint_stats.json` and goes through the same privacy guard.

For each question, compute:

- **Joint distributions** of the listed dimensions, as frequencies of value combinations after joining the listed tables, with k-suppression on every cell.
- **Conditional measure stats**: quantiles of each measure per dimension combination (only for combinations above k).
- **Time series** of the measure at the requested grain, as shares of the total, so trends and seasonality survive without exposing absolute values.
- **Join cardinalities** along the exact join path, if not already in pass 1.
- **Definition checks**: for each definition, the share of entities on each side of the rule (for example, share of customers with no order in the last 90 days), so the generator can hit it.

Optionally, compute a Spearman correlation matrix for numeric columns and Cramér's V for categorical pairs within each touched table. This feeds a Gaussian copula later, only if milestone 5 shows independent sampling is not convincing enough.

Pass 2 is idempotent and incremental: adding a question recomputes only what that question needs.

## Privacy guard and export review

The privacy guard is a separate step that re-checks every profile before export, even though the profiler already applies the rules. Two independent layers make a mistake much less likely.

The guard:

1. Validates the file against the profile schema and rejects unknown fields.
2. Re-applies k-suppression to every count and frequency, including joint stats.
3. Strips value lists from any column that is sensitive or high-cardinality.
4. Checks that min and max are clipped percentiles.
5. Flags any category label that looks like PII (email or phone patterns, capitalised first-name/last-name pairs, long digit strings).
6. Produces a short review report per table: what will be exported, what was removed and why, and every category label that would leave.

Category labels are exported only if the column is listed in the `allowed_labels` section of the config, or if the reviewer approves them in the report. For everything else, the generator invents labels.

Flow: profiles are written to `review/`; the guard writes `approved/` plus `review_report.md`; a person reads the report and copies `approved/` out. The guard never copies anything itself.

## Synthetic data generator

The generator runs on our workspace, reads the approved profiles, and writes Delta tables with the same names, types and comments into a target catalog. It must be deterministic for a given seed.

Use [dbldatagen](https://github.com/databrickslabs/dbldatagen) as the engine where it fits, since it runs natively on Spark and scales to large row counts. Use Faker for realistic strings. Do not use libraries that need to train on real rows (for example SDV), since we never have them.

**Order:** build a dependency graph from the foreign keys and generate tables in topological order, parents first. Detect cycles and break them by generating the self- or cross-referencing key last.

**Scale:** a global `scale` parameter (default 0.01) multiplies every row count. Children-per-parent distributions keep proportions consistent at any scale.

**Sampling by column type:**

| Type | Method |
| --- | --- |
| Primary key | Sequential or UUID, matching the observed format pattern |
| Foreign key | For each parent, draw a child count from the cardinality quantiles; add the orphan rate as dangling keys if it is non-zero |
| Numeric | Inverse-CDF sampling: draw uniform, interpolate linearly through the exported quantiles, round to the observed decimals |
| Categorical | Sample approved labels by frequency; fill the "other" share with invented labels following the length stats |
| Sensitive or text | Faker provider chosen by semantic type, constrained to the observed format pattern and lengths |
| Date or timestamp | Pick year-month by weight, then day by day-of-week weight, then hour by hour weight |
| Boolean | Bernoulli with observed shares |
| Any | Apply null rate last, column by column |

**Pass-2 stats override pass-1 stats** where both exist. Joint distributions are sampled together (dimension combination first, then measures from that combination's quantiles), and time-series shares reweight the date sampling for the tables involved.

**Rules** run as a post-processing pass: fix date orderings, recompute derived columns, and enforce definition shares from pass 2.

**Metadata:** copy table and column comments into the synthetic tables, since Genie relies on them. Write a `_synthetic_info` table property with the profile version, seed and scale.

## Validation

Validation reuses the profiler: run pass 1 and pass 2 on the synthetic tables, then diff the two sets of profiles. Because both sides are profiles, the comparison happens without customer data.

The diff report (`validation_report.md` plus a JSON version) shows, per table and column:

- Null rate difference.
- Quantile differences, and a Kolmogorov–Smirnov style distance computed from the two quantile sets.
- Total variation distance between category frequency distributions.
- Differences in month, day-of-week and hour shares.
- Join success rate and children-per-parent quantiles.
- Rule match rates and definition shares.

Each metric gets a pass or warn flag against thresholds in the config (for example, null rate within 0.01, total variation distance below 0.05).

Finally, a Genie smoke test: create a Genie space on the synthetic tables, add the demo questions as sample questions, ask each one, and record whether it returns a sensible, non-empty answer. This is manual at first; automate it later through the Genie API if it is available on the workspace.

## Repository layout and milestones

Build a Python package with thin Databricks notebooks on top, so the logic is testable locally with a small Spark session.

```
synthgen/
  pyproject.toml
  README.md
  config/
    example_dataset.yaml      # thresholds, sensitive overrides, forced FKs, rules, allowed_labels
  synthgen/
    profile_schema.py         # Pydantic models, schema_version
    profiler/
      pass1.py                # table, column, relationship stats
      pass2.py                # question-driven joint stats
      detect.py               # roles, PII heuristics, patterns, FK inference, rules
    privacy/
      guard.py                # k-suppression, stripping, review report
    questions/
      suggest_questions.py    # LLM-based question suggestions from profiles
    generator/
      plan.py                 # dependency graph, ordering, scale
      columns.py              # samplers per type
      rules.py                # post-processing
      write.py                # Delta tables, comments, properties
    validate/
      diff.py                 # profile comparison and report
  notebooks/
    01_profile_pass1.py
    02_privacy_guard.py
    03_profile_pass2.py
    04_generate.py
    05_validate.py
  tests/
    fixtures/                 # small fake retail dataset with known stats
```

Build it in this order. Each milestone ends with tests passing on the fixture dataset.

1. **Profile schema and pass 1.** Acceptance: profiling the fixture dataset produces valid JSON with no raw values in sensitive columns; a unit test asserts no row-level data appears in any output.
2. **Privacy guard.** Acceptance: a fixture with planted rare values and PII-looking labels is cleaned, and the review report lists every removal.
3. **Generator from pass 1.** Acceptance: generated tables have the same schema and comments, all foreign keys join, and the validation diff passes on marginal stats.
4. **Validator.** Acceptance: the report flags a deliberately broken synthetic table.
5. **Questions and pass 2.** Acceptance: for the fixture questions, joint distributions and time shares in the synthetic data match within thresholds.
6. **Optional copula.** Only if milestone 5 is not convincing enough for the demo.

Coding conventions: type hints everywhere, no network calls from profiler code, every threshold read from config, and a `--dry-run` mode for the generator that prints the plan without writing tables.

## Open decisions and assumptions

Assumptions made in this plan, to confirm before or during milestone 1:

- Source tables are Delta tables in Unity Catalog, readable with a standard cluster or SQL warehouse.
- We can run notebooks on the customer workspace but cannot export rows; exporting reviewed JSON is acceptable.
- An LLM can be used on the approved profiles (outside the customer workspace) to suggest questions.
- Default k is 10 and default scale is 1%.

Open questions:

- [ ] Who on the customer side approves the exported profiles?
- [ ] Which low-risk labels may be kept as-is (country, region, product category, channel)?
- [ ] Is the Genie API available on our workspace for automated testing?
- [ ] Which tables are in scope for the demo?
