# Databricks_Demos_and_Utilities
Purpose of this repository is to create a collection of notebooks which explore features and concepts as well as utility functions for development within the Databricks ecosystem

## Layout

```
demos/       notebooks exploring Databricks / Spark features
dbx_utils/   importable utility modules
  benchmark.py                        benchmarking decorators (time_runs, track_stages, ...)
  delta_profiler.py                   show_delta_tree() to visualize a Delta table's files
  feature_flag.py                     feature_flag decorator to gate DataFrame transformations
  spark_structure_streaming_plus.py   structured streaming helpers (live_session_window, ...)
```

## Using `dbx_utils` in a Databricks workspace

These steps follow Databricks' guidance on [importing Python modules from Git folders and workspace files](https://docs.databricks.com/aws/en/files/workspace-modules).

### 1. Clone the repo as a Git folder

In the workspace, go to **Workspace → Create → Git folder** and paste this repo's URL. Repeat in each workspace you use, and **Pull** to pick up changes.

### 2. Import

**Notebooks inside this Git folder:** the repo root is on `sys.path` automatically, so import directly:

```python
from dbx_utils.benchmark import time_runs
from dbx_utils.feature_flag import feature_flag
```

**Notebooks outside the Git folder:** add the Git folder's absolute path first. Use an absolute path, since the working directory isn't guaranteed on serverless.

```python
import sys
sys.path.append("/Workspace/Users/<you>/Databricks_Demos_and_Utilities")

from dbx_utils.benchmark import time_runs
```

### 3. Pick up edits without restarting

```python
%load_ext autoreload
%autoreload 2
```

Autoreload only reloads code on the **driver**. Code that runs on executors, such as UDFs or the `StatefulProcessor` in `spark_structure_streaming_plus.py`, won't pick up edits. Restart Python with `dbutils.library.restartPython()` after changing those.

### Notes

- Use DBR 13.3 LTS or later (or serverless). On these runtimes, packages on `sys.path` are distributed to executors automatically.
- `spark_structure_streaming_plus.py` uses `transformWithState` and needs a recent runtime.
