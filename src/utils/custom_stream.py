import pandas as pd
from typing import NamedTuple
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StatefulProcessor

class Session(NamedTuple):
    session_id: int
    start_ms: int
    last_ms: int
    cnt: int
    total: float

STATE_SCHEMA = "session_id long, start_ms long, last_ms long, cnt long, total double"

def session_output_type(key_schema):
    """key_schema e.g. 'user_id long' or 'region string, user_id long'"""
    return (f"{key_schema}, session_id long, session_start_ms long, last_event_ms long, "
            "session_end_ms long, count long, sum double, status string")


class OpenClosedSessions(StatefulProcessor):
    """
    Custom session-window aggregation (count and sum per key) that publishes
    progress while a session is still ongoing, signified by the status 'open'.
    It also behaves the same way the native session-window would when the
    watermark passes the end of the session, publishing the final result with
    status 'closed'.
    """
    def __init__(self, gap_ms, event_time_col="event_time", value_col="amount", key_cols=("user_id",)):
        super().__init__()
        self.gap_ms = gap_ms
        self.event_time_col = event_time_col
        self.value_col = value_col
        self.key_cols = tuple(key_cols)

    def init(self, handle):
        self.handle = handle
        self.session = handle.getValueState("session", STATE_SCHEMA)

    def _end_ms(self, s):
        return s.last_ms + self.gap_ms

    def _load(self):
        return Session(*self.session.get()) if self.session.exists() else None

    def _row(self, key, s, status):
        return {**dict(zip(self.key_cols, key)),
                "session_id": s.session_id, "session_start_ms": s.start_ms,
                "last_event_ms": s.last_ms, "session_end_ms": self._end_ms(s),
                "count": s.cnt, "sum": s.total, "status": status}

    def handleInputRows(self, key, rows, timerValues):
        watermark = timerValues.getCurrentWatermarkInMs()
        pdf = pd.concat(list(rows)).sort_values(self.event_time_col)

        cur = self._load()
        old_timer = self._end_ms(cur) if cur else None
        out, changed = [], False

        for ts, amt in zip(pdf[self.event_time_col], pdf[self.value_col]):
            ts, amt = ts.value // 1_000_000, float(amt)
            if ts < watermark:
                continue
            changed = True
            if cur is None:
                cur = Session(ts, ts, ts, 1, amt)
            elif ts >= self._end_ms(cur):
                out.append(self._row(key, cur, "closed"))
                cur = Session(ts, ts, ts, 1, amt)
            else:
                cur = cur._replace(start_ms=min(cur.start_ms, ts),
                                   last_ms=max(cur.last_ms, ts),
                                   cnt=cur.cnt + 1,
                                   total=cur.total + amt)

        if changed:
            new_timer = self._end_ms(cur)
            if old_timer != new_timer:
                if old_timer is not None:
                    self.handle.deleteTimer(old_timer)
                self.handle.registerTimer(new_timer)
            self.session.update(tuple(cur))
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


def session_windows(df, key_cols, key_schema, gap_ms, event_time_col="event_time", value_col="amount"):
    """
    Wrapper for OpenClosedSessions along with handling its input and output formatting.
        key_cols: GroupBy equivalent
        key_schema: Schema of the key columns
        gap_ms: Watermark component, indicating how long till a session is considered closed
        event_time_col: Watermark component, the column containing the event timestamp
        value_col: The aggregated columns (only supports sum() and count() at the moment)
    """
    processor = OpenClosedSessions(gap_ms, event_time_col, value_col, tuple(key_cols))
    return (
        df.groupBy(*key_cols)
        .transformWithStateInPandas(
            statefulProcessor=processor,
            outputStructType=session_output_type(key_schema),
            outputMode="Update",
            timeMode="EventTime",
        )
        .withColumn("session_start", f.date_trunc("second", f.timestamp_millis("session_start_ms")))
        .withColumn("session_end", f.date_trunc("second", f.timestamp_millis("session_end_ms")))
        .withColumn("window", f.struct(f.col("session_start").alias("start"),
                                       f.col("session_end").alias("end")))
        .select(*key_cols, "window", "session_start", "session_end", "count", "sum", "status")
    )


def _session_window(self, *args, **kwargs):
    return session_windows(self, *args, **kwargs)

DataFrame.session_window = _session_window

