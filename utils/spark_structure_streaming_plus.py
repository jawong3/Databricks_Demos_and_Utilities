"""
The script contains add-on functions to assist the developement of spark strucutre streaming piplines
"""
import shutil
import functools
import uuid
import pandas as pd
import json
import queue
from collections import deque
import threading
import re
from datetime import timedelta, datetime, timezone
from typing import NamedTuple, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.streaming import StatefulProcessor, StreamingQueryListener
from pyspark.sql.types import *

# --------------------------------------------------------
# Private Helper Functions & Variables
# --------------------------------------------------------
_UNITS_MS = {
    "millisecond": 1,
    "second": 1_000,
    "minute": 60_000,
    "hour": 3_600_000,
    "day": 86_400_000,
    "week": 604_800_000}


def _gap_to_ms(gap) -> int:
    """'10 minutes' / timedelta / int (ms) -> ms."""
    if isinstance(gap, timedelta):
        return int(gap.total_seconds() * 1000)
    if isinstance(gap, int):
        return gap
    m = re.fullmatch(r"\s*(\d+)\s*([a-z]+?)s?\s*", str(gap).lower())
    if not m or m.group(2) not in _UNITS_MS:
        raise ValueError(
            f"Unsupported gapDuration {gap!r}; use e.g. '10 minutes', a timedelta, or ms as int"
        )
    return int(m.group(1)) * _UNITS_MS[m.group(2)]


def _unwrap(c) -> str:
    """Column -> the expression text inside Column<'...'> (works in classic and Connect)."""
    if isinstance(c, str):
        return c
    m = re.fullmatch(r"Column<'(.*)'>", str(c), re.S)
    if not m:
        raise ValueError(f"Cannot interpret column expression: {c!r}")
    return m.group(1)


def _col_name(c) -> str:
    """Plain column reference -> its name."""
    name = _unwrap(c).strip("`")
    if not re.fullmatch(r"[\w.\- ]+", name):
        raise ValueError(f"{c!r} must be a plain column reference (alias expressions first)")
    return name


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _as_tuple(x):
    return (x,) if isinstance(x, (str, Column)) else tuple(x)


def _ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def _int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None
    

class AggSpec(NamedTuple):
    kind: str            # sum | count | min | max
    col: Optional[str]   # None => count every row (count("*") / count(lit(1)))
    out_name: str        # name as Spark would give it (alias or e.g. 'sum(amount)')
    ddl_type: str        # result type as Spark resolves it


_AGG_RE = re.compile(r"(sum|count|min|max)\((.*)\)", re.I | re.S)


def _parse_agg(c: Column, out_name: str, ddl_type: str) -> AggSpec:
    expr = re.sub(r"\s+AS\s+`?[^`]+`?\s*$", "", _unwrap(c), flags=re.I).strip()
    m = _AGG_RE.fullmatch(expr)
    if not m:
        raise ValueError(f"Unsupported aggregation {c!r}; supported: sum, count, min, max")
    kind, arg = m.group(1).lower(), m.group(2).strip()
    if arg.upper().startswith("DISTINCT"):
        raise ValueError(f"DISTINCT aggregations are not supported: {c!r}")
    if kind == "count" and (arg in ("*", "1")):
        return AggSpec(kind, None, out_name, ddl_type)
    arg = arg.strip("`")
    if not re.fullmatch(r"[\w.\- ]+", arg):
        raise ValueError(f"Aggregation input must be a plain column (got {arg!r} in {c!r})")
    return AggSpec(kind, arg, out_name, ddl_type)


LIFECYCLE_SCHEMA = (StructType()
    .add("event_type", StringType())
    .add("query_id", StringType())
    .add("run_id", StringType())
    .add("query_name", StringType())
    .add("event_time", TimestampType())
    .add("error", StringType()))


PROGRESS_SCHEMA = (StructType()
    .add("query_id", StringType())
    .add("query_name", StringType())
    .add("batch_id", LongType())
    .add("batch_timestamp", TimestampType())
    .add("batch_duration", LongType())
    .add("duration_breakdown", MapType(StringType(), LongType()))
    .add("input_rows", LongType())
    .add("sources", ArrayType(StringType()))
    .add("files_outstanding", ArrayType(LongType()))
    .add("bytes_outstanding", ArrayType(LongType())))


# --------------------------------------------------------
# Live Session Window
# --------------------------------------------------------
class Session(NamedTuple):
    start_ms: int
    last_ms: int
    accs: Tuple  # one accumulator per AggSpec, same order


def session_output_type(key_schema: str, aggs: Sequence[AggSpec]) -> str:
    agg_ddl = ", ".join(f"{_q(a.out_name)} {a.ddl_type}" for a in aggs)
    return (f"{key_schema}, session_start_ms long, last_event_ms long, session_end_ms long, "
            f"{agg_ddl}, status string")


class OpenClosedSessions(StatefulProcessor):
    """
    Custom session-window aggregation that publishes progress while a session is
    still ongoing (status 'open') and the final result once the watermark passes
    the end of the session (status 'closed'), like the native session window.
    Supports sum / count / min / max over plain columns.
    """

    def __init__(self, gap_ms, event_time_col, key_names, aggs):
        super().__init__()
        self.gap_ms = gap_ms
        self.event_time_col = event_time_col
        self.key_names = tuple(key_names)
        self.aggs = tuple(aggs)
        self.state_schema = "start_ms long, last_ms long, " + ", ".join(
            f"a{i} {a.ddl_type}" for i, a in enumerate(self.aggs)
        )

    def init(self, handle):
        self.handle = handle
        self.session = handle.getValueState("session", self.state_schema)

    # -- helpers -----------------------------------------------------------
    def _end_ms(self, s):
        return s.last_ms + self.gap_ms

    def _load(self):
        if not self.session.exists():
            return None
        t = tuple(self.session.get())
        return Session(t[0], t[1], tuple(t[2:]))

    def _save(self, s):
        self.session.update((s.start_ms, s.last_ms, *s.accs))

    @staticmethod
    def _step(a, acc, v):
        if a.kind == "count":
            return acc + (1 if a.col is None or not pd.isna(v) else 0)
        if pd.isna(v):
            return acc
        if acc is None:
            return v
        if a.kind == "sum":
            return acc + v
        return min(acc, v) if a.kind == "min" else max(acc, v)

    def _new(self, ts, vals):
        accs = tuple(
            self._step(a, 0 if a.kind == "count" else None, v)
            for a, v in zip(self.aggs, vals)
        )
        return Session(ts, ts, accs)

    def _add(self, s, ts, vals):
        accs = tuple(self._step(a, acc, v) for a, acc, v in zip(self.aggs, s.accs, vals))
        return Session(min(s.start_ms, ts), max(s.last_ms, ts), accs)

    def _row(self, key, s, status):
        row = dict(zip(self.key_names, key))
        row.update(session_start_ms=s.start_ms, last_event_ms=s.last_ms,
                   session_end_ms=self._end_ms(s))
        row.update({a.out_name: v for a, v in zip(self.aggs, s.accs)})
        row["status"] = status
        return row

    # -- stream callbacks --------------------------------------------------
    def handleInputRows(self, key, rows, timerValues):
        watermark = timerValues.getCurrentWatermarkInMs()
        pdf = pd.concat(list(rows)).sort_values(self.event_time_col)

        value_lists = [None if a.col is None else pdf[a.col].tolist() for a in self.aggs]

        cur = self._load()
        old_timer = self._end_ms(cur) if cur else None
        out, changed = [], False

        for i, t in enumerate(pdf[self.event_time_col]):
            ts = t.value // 1_000_000
            if ts < watermark:
                continue
            changed = True
            vals = [None if vl is None else vl[i] for vl in value_lists]
            if cur is None:
                cur = self._new(ts, vals)
            elif ts >= self._end_ms(cur):
                out.append(self._row(key, cur, "closed"))
                cur = self._new(ts, vals)
            else:
                cur = self._add(cur, ts, vals)

        if changed:
            new_timer = self._end_ms(cur)
            if old_timer != new_timer:
                if old_timer is not None:
                    self.handle.deleteTimer(old_timer)
                self.handle.registerTimer(new_timer)
            self._save(cur)
            out.append(self._row(key, cur, "open"))

        if out:
            yield pd.DataFrame(out)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        expiry = expiredTimerInfo.getExpiryTimeInMs()
        s = self._load()
        if s is not None and self._end_ms(s) == expiry:
            self.session.clear()
            yield pd.DataFrame([self._row(key, s, "closed")])

    def close(self):
        pass


def live_session_window(df: DataFrame, groupBy, agg, timeColumn, gapDuration) -> DataFrame:
    """
    Session-window aggregation that emits 'open' progress rows and 'closed' final rows.

        df.live_session_window(
            groupBy=(f.col("region"), f.col("user_id")),
            timeColumn="event_time",
            gapDuration="10 minutes",
            agg=(f.sum("amount"), f.count("*"), f.min("amount").alias("lo"), f.max("amount")),
        )

    groupBy      Column(s)/name(s) to group by (expressions allowed; output name follows Spark).
    timeColumn   Plain event-time column (the DataFrame needs a watermark on it).
    gapDuration  '10 minutes' style string, timedelta, or int milliseconds.
    agg          Column(s) built from f.sum / f.count / f.min / f.max over a plain column.
                 Aliases are respected; otherwise default Spark names (e.g. 'sum(amount)').
    """

    # Since the functions signature mimics a Spark, parse 
    group_cols = _as_tuple(groupBy)
    agg_cols = _as_tuple(agg)
    time_name = _col_name(timeColumn)
    gap_ms = _gap_to_ms(gapDuration)

    # Let Spark resolve key / aggregate names and result types for us.
    key_fields = df.select(*group_cols).schema.fields
    agg_fields = df.groupBy().agg(*agg_cols).schema.fields

    key_names = [fld.name for fld in key_fields]
    key_schema = ", ".join(f"{_q(fld.name)} {fld.dataType.simpleString()}" for fld in key_fields)
    aggs = [
        _parse_agg(c, fld.name, fld.dataType.simpleString())
        for c, fld in zip(agg_cols, agg_fields)
    ]

    processor = OpenClosedSessions(gap_ms, time_name, key_names, aggs)
    return (
        df.groupBy(*group_cols)
        .transformWithStateInPandas(
            statefulProcessor=processor,
            outputStructType=session_output_type(key_schema, aggs),
            outputMode="Update",
            timeMode="EventTime",
        )
        .withColumn("session_start", f.date_trunc("second", f.timestamp_millis("session_start_ms")))
        .withColumn("session_end", f.date_trunc("second", f.timestamp_millis("session_end_ms")))
        .withColumn("window", f.struct(f.col("session_start").alias("start"),
                                       f.col("session_end").alias("end")))
        .select(*[f.col(_q(n)) for n in key_names],
                "window", "session_start", "session_end",
                *[f.col(_q(a.out_name)) for a in aggs],
                "status")
    )


DataFrame.live_session_window = live_session_window


# --------------------------------------------------------
# Temporary Checkpoint Display
# --------------------------------------------------------
class TempCheckpointDisplay:
    """
    Wrap Databricks display() so streaming DataFrames get a managed checkpoint.

    tckp_display = TempCheckpointDisplay(display, "/Volumes/path/to/adhoc_checkpoints")

    tckp_display(stream)    # Calls on display() using a temporary checkpoint
    tckp_display.cleanup()  # Deletes all temporary checkpoints it has created during the session

    Batch DataFrames bypass the checkpoint creation and just uses the native display()
    """

    def __init__(self, display_fn, base_path):
        self._display = display_fn
        self.base_path = base_path
        self._checkpoints = []
    
    def __call__(self, df=None, *args, name=None, base=None, **kwargs):
        # In case a non-streaming table is passed into the function,
        # just call display() normally
        if not getattr(df, "isStreaming", False) or "checkpointLocation" in kwargs:
            return self._display(df, *args, **kwargs)

        path = self._new_path(name, base)
        kwargs["checkpointLocation"] = path
        print(f"[tckp_display] created checkpoint: {path}")

        if path not in self._checkpoints:
            self._checkpoints.append(path)
            return self._display(df, *args, **kwargs)

    # Delete all persisted checkpoints
    def cleanup(self):
        for path in list(self._checkpoints):
            self._remove(path)
            self._checkpoints.remove(path)

    # Create the checkpoint path with either the predefined
    # directory name and base path or generate a random directory 
    def _new_path(self, name, base):
        root = (base or self.base_path).rstrip("/")
        return f"{root}/{name or uuid.uuid4().hex[:12]}"

    @staticmethod
    def _remove(path):
        try:
            shutil.rmtree(path, ignore_errors=True)
            print(f"[tckp_display] removed {path}")
        except Exception as e:
            print(f"[tckp_display] could not remove {path}: {e}")


def register_tckp_display(display, base_path):
    """Create a TempCheckpointDisplay and attach it to DataFrame,
    so both tckp_display(df) and df.tckp_display() work."""
    tckp = TempCheckpointDisplay(display, base_path)

    def _method(self, **kwargs):
        return tckp(self, **kwargs)

    DataFrame.tckp_display = _method
    return tckp


# --------------------------------------------------------
# Table Logging Streaming Query Listener
# --------------------------------------------------------
class TableStreamingQueryListener(StreamingQueryListener):
    def __init__(self, spark, lifecycle_table, progress_table, skip_empty_batches=True):
        super().__init__()
        self._spark = spark
        self.lifecycle_table = lifecycle_table
        self.progress_table = progress_table
        self.skip_empty_batches = skip_empty_batches

        self._table_names = {}
        self._query_names = {str(q.id): q.name for q in spark.streams.active}

        self._queue = queue.Queue()
        threading.Thread(target=self._drain, daemon=True, name="stream-listener-writer").start()

        for table, schema in [(lifecycle_table, LIFECYCLE_SCHEMA), (progress_table, PROGRESS_SCHEMA)]:
            if table is not None:
                spark.createDataFrame([], schema).write.mode("ignore").saveAsTable(table)

    def _log(self, table, schema, row):
        self._queue.put((table, schema, row))

    def _delta_name(self, uuid):
        if uuid not in self._table_names:
            row = self._spark.sql(f"""
                SELECT concat_ws('.', table_catalog, table_schema, table_name) AS name
                FROM system.information_schema.tables
                WHERE storage_path LIKE '%/tables/{uuid}'""").first()
            self._table_names[uuid] = row["name"] if row else None
        return self._table_names[uuid]

    def _source_name(self, source):
        desc = source.get("description", "")
        kind = re.match(r"[^\[]*", desc)[0]
        match = re.search(r"/tables/([0-9a-f-]{36})", desc)

        if kind != "DeltaSource" or not match:
            return kind
        try:
            return self._delta_name(match[1]) or kind
        except Exception:
            return kind

    def _drain(self):
        while True:
            item = self._queue.get()
            try:
                if item is None:
                    return
                table, schema, row = item
                row = row() if callable(row) else row
                self._spark.createDataFrame([row], schema).write.mode("append").saveAsTable(table)
            finally:
                self._queue.task_done()

    def flush(self):
        self._queue.join()

    def close(self):
        self.flush()
        self._queue.put(None)

    # TODO: The logic is correct, however the function doesn't fire consistently regardless of its contents
    def onQueryStarted(self, event):
        pass
        self._query_names[str(event.id)] = event.name

        # if self.lifecycle_table is None: return

        # self._log(self.lifecycle_table, LIFECYCLE_SCHEMA,
        #     dict(
        #         event_type  = "STARTED",
        #         query_id    = str(event.id),
        #         run_id      = str(event.runId),
        #         query_name  = event.name,
        #         event_time  = _ts(event.timestamp)
        #     ))

    # TODO: Use pop instead once onQueryStarted is fixed
    def onQueryTerminated(self, event):
        query_id = str(event.id)
        # query_name = self._query_names.pop(query_id, None)
        query_name = self._query_names[str(event.id)]

        if self.lifecycle_table is None: return

        self._log(self.lifecycle_table, LIFECYCLE_SCHEMA,
            dict(
                event_type  = "TERMINATED",
                query_id    = str(event.id),
                run_id      = str(event.runId),
                query_name  = query_name,
                event_time  = datetime.now(timezone.utc),
                error       = event.exception
            ))

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        if p.get("id") and p.get("name"):
            self._query_names.setdefault(p["id"], p["name"])

        if self.skip_empty_batches and not p.get("numInputRows"):
            return

        sources = p.get("sources") or []
        metric = lambda k: [_int((s.get("metrics") or {}).get(k)) for s in sources]

        self._log(self.progress_table, PROGRESS_SCHEMA,
            dict(
                query_id            = p.get("id"),
                query_name          = p.get("name"),
                batch_id            = p.get("batchId"),
                batch_timestamp     = _ts(p.get("timestamp")),
                batch_duration      = p.get("batchDuration"),
                duration_breakdown  = p.get("durationMs"),
                input_rows          = p.get("numInputRows"),
                sources             = [self._source_name(s) for s in sources],
                files_outstanding   = metric("numFilesOutstanding"),
                bytes_outstanding   = metric("numBytesOutstanding")
            ))

    def onQueryIdle(self, event):
        pass

