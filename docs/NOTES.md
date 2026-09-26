## Useful Workflow Tips


### Running the pipeline on your local machine

There are two ways to run the pipeline code, and they behave very differently:

- **Databricks CLI / Databricks Asset Bundles** (`databricks bundle deploy && databricks bundle run`)
  - The code from your PC is **uploaded to Databricks** and executes on **Databricks compute** (the platform).
  - It runs *your local code* but *on the platform* — the `.py` files are read from your PC, shipped up, and run by Databricks infrastructure.
  - The version that runs is whatever is on your PC at deploy time.

- **Plain `python real_estate_pipeline_silver.py`** on your PC
  - Runs **entirely locally** using your local Python/Spark environment.
  - It does not touch Databricks compute unless the code explicitly connects to a Databricks cluster via a Spark connect string.

> **Note:** If you're running from a local branch (e.g. `pc-branch`) and also making pipeline changes in Databricks on a different branch (e.g. `platform-dev`), the two codebases are independent. Push/pull between branches to sync before deploying from either side.

### Syncing between local and Databricks

`databricks sync` syncs a local directory to a **workspace directory** (like `/Workspace/Users/.../my-folder`), not to a **Git repo folder** (`/Repos/...`). Since this project lives in a Git repo, `databricks sync` won't work — use Git itself:

```bash
# Push from your PC (pc-branch)
git push origin pc-branch

# Push from Databricks (platform-dev)
git push origin platform-dev

# Pull the other branch's changes when you need them locally
git fetch origin
git merge origin/platform-dev
```

Other options:

- **`databricks bundle sync --watch`** (part of DABs) — continuously syncs local files to the bundle's deployment target in the workspace. This is for **deployment**, not for keeping two Git branches in sync.
- **A Git alias or script on your PC** — e.g. `git push origin pc-branch && git fetch origin` run periodically, so you can merge `platform-dev` changes whenever they land.

Git push/pull isn't automatic like `--watch`, but it's reliable, handles conflicts explicitly, and keeps a clean history.
