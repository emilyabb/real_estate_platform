# Issues Encountered

## `replace_using` / `sequence_by` not supported on this runtime

When running the silver pipeline, the pipeline fails with:

```
TypeError: table() got an unexpected keyword argument 'replace_using'
```

The `replace_using` and `sequence_by` parameters on `@dp.table()` require **Databricks Runtime 18.2+** (see [Databricks docs](https://docs.databricks.com/aws/en/ldp/flows-replace-using/)). This workspace is on a pre-18.2 runtime — the `dp.table()` signature has `replace_where` but no `replace_using` or `sequence_by`, and `@dp.replace_flow` doesn't exist in the module at all.

**All tables using `replace_using` are equally affected** — the Redfin silver tables use the same `replace_using` + `sequence_by` pattern and would hit the identical `TypeError` if run now. The only silver table that would work on this runtime is **ACS** (`american_community_survey_zcta`), which doesn't use `replace_using` at all.

**Unclear why this appeared to work before** — we don't know why the runtime would have changed, but best guess is that something changed in the compute environment since then. The parameters have never been valid on the *current* runtime, for any table.

**Options to fix:**

| Option | What it involves | Works now? |
| --- | --- | --- |
| **Upgrade DBR to 18.2+** | Change the pipeline's DBR — existing code works as-is | After upgrade |
| **Use `dp.create_auto_cdc_flow()`** | Available on this runtime — has `keys` + `sequence_by`. Requires CDF enabled on bronze source tables | Yes, if CDF is enabled |
| **Skip dedup in silver** | Append all rows in silver, dedup at query time or in gold with batch read + window function | Yes |

---


# Appendix
When Genie's "thinking" stream has ten "Actually"'s 💀🥀🫠