import time
from functools import wraps
from typing import NamedTuple

from pyspark.sql import DataFrame

def benchmark(func=None, *, runs=5):
    """Run the wrapped function `runs` times and return Timing(best, avg) in seconds.

    Works as @benchmark (5 runs) or @benchmark(runs=10).
    The wrapped function's last return value is kept on `wrapper.last_result`
    and every run's duration on `wrapper.times`.
    """
    if runs < 1:
        raise ValueError("runs must be >= 1")

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            times = []
            for _ in range(runs):
                start = time.perf_counter()
                result = fn(*args, **kwargs)
                if isinstance(result, DataFrame):
                    result.count()
                seconds = time.perf_counter() - start
                times.append(seconds)

            wrapper.times = times
            wrapper.last_result = result
            return times, result

        return wrapper

    return decorator(func) if func is not None else decorator