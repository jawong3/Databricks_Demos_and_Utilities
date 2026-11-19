import functools
from pyspark.sql import DataFrame

def feature_flag(flag, fallback = None):
    """
    Decorated function which gatekeeps new transformations,
    only added them into the transformation chain if the flag is True.
    It handles 2 scenarios: the transformation is new or if its
    replacing an older implementation referenced by fallback

    Examples:
        # 1. Brand New Feature
        @feature_flag(DEDUPE)
        def dedupe(df):
            return df.dropDuplicates(["id"])

        # 2. Replacing Old Implementation
        def rename_v0(df):
            return df.withColumn("name", f.trim("name"))

        @feature_flag(USE_NEW_CLEAN_NAMES, fallback=rename_v0)
        def rename_v1(df):
            return df.withColumn("name", f.initcap(f.trim("name")))

        df.transform(dedupe).transform(rename_v1)
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(df: DataFrame, *args, **kwargs) -> DataFrame:
            if flag:
                return func(df, *args, **kwargs)
            if fallback is None:
                return df
            return fallback(df, *args, **kwargs)

        return wrapper

    return decorator

