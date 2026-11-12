"""
This script holds benchmarking decorators catered towards Spark DataFrames & Transformations,
however they can also be used for plain Python functions. These decorators are designed to be
stackable with eachother, however the order in which they are invoked matters.

Below are all the decorators available in order of which they should be stacked
along with a short description of their functionality:

    - track_partitions
        records the number of partitions and empty partitions in the
        Spark DataFrame returned by the function.
    - time_runs
        wall-clock timing over repeated runs. Spark DataFrames returned by the
        function are forced to execute with a noop write, so lazy evaluation
        (or shortcuts like count()/collect()) can't skew the result.
    - clear_cache
        clear Spark's in-memory cache prior and after each call.
    - track_stages
        records the Spark stages, and their task counts, launched by each call.

"""
import time
from functools import wraps
from typing import Any


def _isSparkDF(x) -> bool:
    """
    Check if the parameter object provided is a Spark DataFrame
    """
    try:
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDF
        if isinstance(x, ConnectDF): return True
    except ImportError:
        pass
    from pyspark.sql import DataFrame
    return isinstance(x, DataFrame)


def _force(result) -> None:
    """
    Execute a lazy Spark DataFrame with a noop write, at most once per DataFrame
    object, so stacked decorators don't each re-run the whole plan.
    """
    if _isSparkDF(result) and not result.__dict__.get("_forced"):
        result.write.format("noop").mode("overwrite").save()
        result._forced = True


def time_runs(*, runs: int = 5, label: str = ""):
    """
    Run the decorated function `runs` times, then print the best and all timings,
    labelled with the function name and, if given, `label`.
    Returns the decorated function's own result (from the final run); the timings
    are also kept on `<function>.times`.
    """
    if runs < 1: raise ValueError("Argument `runs` must be some positive integer")
 
    def decorator(fn):
        prefix = f"{fn.__name__:<28} " + (f"{label:<28} " if label else "")
        recorded: list[float] = []
 
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # Start each call fresh: clear any per-run records (e.g. track_stages)
            stages = getattr(fn, "stages", [])
            if len(stages):
                stages = []
            
            times: list[float] = []
            for _ in range(runs):
                start = time.perf_counter()
                result = fn(*args, **kwargs)
                _force(result)
                times.append(time.perf_counter() - start)
 
            recorded[:] = times
            rounded = [round(t, 2) for t in times]
            print(f"{prefix}best={min(times):.2f}s  all={rounded}")
            return result
 
        wrapper.times = recorded
        return wrapper
 
    return decorator


def track_stages(fn):
    """
    For a function calling a Spark action/job, record the details of each stage,
    appending to `<function>.stages` with the format:

    [
        {"job": x_0, "stage": y_0, "tasks": z_0, "name": w_0},
        ...
        {"job": x_n, "stage": y_n, "tasks": z_n, "name": w_n},
        {"total_task_count": int}
    ]

    """
    stages_per_call: list[list[dict[str, Any]]] = []
 
    @wraps(fn)
    def wrapper(*args, **kwargs):
        from pyspark.sql import SparkSession

        # Define a unique job group per call, tracking their stages seperately
        sc = SparkSession.builder.getOrCreate().sparkContext
        tr = sc.statusTracker()
        group = f"{fn.__name__}-{time.time_ns()}"
        sc.setJobGroup(group, group)
 
        # Force a Spark action if required
        result = fn(*args, **kwargs)
        _force(result)
 
        # Parse through the statusTracker, grabbing relevant information 
        stages: list[dict[str, Any]] = []
        for j in tr.getJobIdsForGroup(group):
            job = tr.getJobInfo(j)
            if job is None:
                continue
            for s in job.stageIds:
                info = tr.getStageInfo(s)
                if info is not None:
                    stages.append({"job": j, "stage": s, "tasks": info.numTasks, "name": info.name})
 
        # Calculate the total number of task created
        stages.append({"total_task_count": sum(s["tasks"] for s in stages)})
        stages_per_call.append(stages)
        return result
 
    wrapper.stages = stages_per_call
    return wrapper


def track_partitions(fn):
    """
    For a function returning a Spark DataFrame, check how many partitions it holds
    verses how many contain records. Appended to `<function>.partitions` with the format:

    {"total": int, "used": int, "empty": int}

    """
    counts: dict[str, int] = {}

    @wraps(fn)
    def wrapper(*args, **kwargs):
        from pyspark.sql import functions as f
 
        result = fn(*args, **kwargs)
        if not _isSparkDF(result):
            raise TypeError(f"{fn.__name__} must return a Spark DataFrame, got {type(result).__name__}")
 
        total = result.rdd.getNumPartitions()
        used = result.select(f.spark_partition_id()).distinct().count()
        counts.update(total=total, used=used, empty=total - used)
        print(f"{fn.__name__:<28} partitions total={total}  used={used}  empty={total - used}")
        return result
 
    wrapper.partitions = counts
    return wrapper


def clear_cache(func=None, *, before: bool = True, after: bool = False):
    """
    Clear Spark's in-memory cache around a call
    """
 
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            spark = _get_spark()
            if before:
                spark.catalog.clearCache()
            try:
                return fn(*args, **kwargs)
            finally:
                if after:
                    spark.catalog.clearCache()
 
        return wrapper
 
    if callable(func):
        return decorator(func)
    return decorator

