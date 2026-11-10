"""
delta_tree.py - print a directory tree of a Delta table's parquet files.

Usage in a Databricks notebook:

    %run ./delta_tree        # or: from delta_tree import show_delta_tree

    show_delta_tree("main.default.events")
    show_delta_tree("main.default.events", row_groups=True)   # + intra-file stats

What you get:
  * the file layout (partition dirs -> parquet files) with rows / size per file
  * files from earlier table versions that are no longer in the latest one
    (found via time travel) crossed out
  * inter-file stats: per-file min/max computed from the data, plus an overlap
    summary and a range chart per column of the active files (how well can
    data skipping prune?)
  * intra-file stats (optional, row_groups=True): row-group row counts and
    min/max from the parquet footers
"""
import json
import re
from decimal import Decimal
from urllib.parse import unquote

_NUMERIC = {"tinyint", "smallint", "int", "bigint", "float", "double", "decimal"}
_STRIKE = "\u0336"


# --------------------------------------------------------------------------- #
# Collecting (needs Spark)
# --------------------------------------------------------------------------- #
def _row_groups(spark, full_path, cols):
    """Row-group row counts and min/max for `cols`, read from the parquet footer
    through the JVM (needs a cluster where spark._jvm is reachable)."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    inp = jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
        jvm.org.apache.hadoop.fs.Path(full_path), conf
    )
    reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(inp)
    try:
        out = []
        for b in reader.getFooter().getBlocks():
            rg = {"rows": b.getRowCount(), "stats": {}}
            for c in b.getColumns():
                name = c.getPath().toDotString()
                if name not in cols:
                    continue
                st = c.getStatistics()
                if st is None or not st.hasNonNullValue():
                    continue
                vals = []
                for v in (st.genericGetMin(), st.genericGetMax()):
                    try:
                        v = v.toStringUsingUTF8()  # Binary (strings)
                    except Exception:
                        pass
                    vals.append(v)
                rg["stats"][name] = tuple(vals)
            out.append(rg)
        return out
    finally:
        reader.close()


def _strip_scheme(p):
    p = unquote(p)
    m = re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://[^/]*", p)
    if m:
        return p[m.end():]
    return re.sub(r"^[A-Za-z][A-Za-z0-9+.-]*:", "", p)


def _rel(path, location):
    p, loc = _strip_scheme(path), _strip_scheme(location).rstrip("/")
    return p[len(loc) + 1:] if p.startswith(loc + "/") else p.rsplit("/", 1)[-1]


def _atomic(dtype):
    return not dtype.startswith(("struct", "array", "map", "variant", "binary"))


def collect_delta_files(spark, table, back=1, versions=None):
    """Collect a Delta table's parquet files using only table reads.

    Active files: scan the table and group by _metadata.file_path.
    Tombstones:   scan earlier versions (time travel) and keep files that are
                  not in the latest version.
    Stats:        min/max/null counts computed from the data itself (exact, not
                  Delta's truncated stats) for the stat columns only: the clustering
                  columns, else the Z-Order columns, else the first 3 non-partition
                  columns of atomic type.
    """
    from pyspark.sql import functions as F

    detail = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict()
    location = detail["location"].rstrip("/")
    schema = spark.table(table).dtypes
    part = list(detail.get("partitionColumns") or [])
    clus = list(detail.get("clusteringColumns") or [])

    history = spark.sql(f"DESCRIBE HISTORY {table}").select("version", "operation", "operationParameters").collect()
    hist = [r["version"] for r in history]

    # Z-Order columns aren't part of the table metadata; they're only recorded on OPTIMIZE commits.
    zorder = []
    for r in sorted(history, key=lambda r: -r["version"]):
        z = (r["operationParameters"] or {}).get("zOrderBy") if r["operation"] == "OPTIMIZE" else None
        if z:
            try:
                zorder = list(json.loads(z))
            except ValueError:
                zorder = [z]
            break

    stat_cols = clus or zorder or [n for n, t in schema if n not in part and _atomic(t)][:3]
    latest = max(hist)
    if versions is None:
        versions = [v for v in range(latest - back, latest + 1) if v >= min(hist)]
    versions = sorted(set(versions) | {latest})

    select = "".join(f", `{c}`" for c in stat_cols)
    seen, scanned = {}, []
    for v in versions:
        try:
            df = spark.sql(
                f"SELECT _metadata.file_path AS _fp, _metadata.file_size AS _fs{select} "
                f"FROM {table} VERSION AS OF {v}"
            )
            aggs = [F.count(F.lit(1)).alias("_rows"), F.first("_fs").alias("_size")]
            for i, c in enumerate(stat_cols):
                col = F.col(f"`{c}`")
                aggs += [F.min(col).alias(f"_min{i}"), F.max(col).alias(f"_max{i}"),
                         F.sum(col.isNull().cast("long")).alias(f"_nulls{i}")]
            rows = df.groupBy("_fp").agg(*aggs).collect()
        except Exception as e:
            print(f"(skipping version {v}: {type(e).__name__}: {str(e)[:100]})")
            continue
        scanned.append(v)
        for r in rows:
            info = seen.setdefault(_rel(r["_fp"], location), {"first": v})
            info["last"], info["row"] = v, r

    if not scanned or scanned[-1] != latest:
        raise RuntimeError(f"could not scan the latest version ({latest}) of {table}")

    files = []
    for rel, info in seen.items():
        r = info["row"]
        active = info["last"] == latest
        f = {
            "rel_path": rel,
            "size": r["_size"],
            "rows": r["_rows"],
            "min": {c: r[f"_min{i}"] for i, c in enumerate(stat_cols) if r[f"_min{i}"] is not None},
            "max": {c: r[f"_max{i}"] for i, c in enumerate(stat_cols) if r[f"_max{i}"] is not None},
            "nulls": {c: r[f"_nulls{i}"] for i, c in enumerate(stat_cols)},
            "state": "active" if active else "tombstone",
            "add_version": info["first"],
            "remove_version": None,
        }
        if not active:
            nxt = next((v for v in scanned if v > info["last"]), None)
            f["remove_version"] = nxt
            f["removed_note"] = (f"removed@v{nxt}" if nxt == info["last"] + 1
                                 else f"removed after v{info['last']}")
        files.append(f)

    meta = {
        "table": table,
        "location": location,
        "partitionColumns": part,
        "clusteringColumns": clus,
        "zOrderColumns": zorder,
        "column_order": [n for n, _ in schema],
        "dtypes": dict(schema),
        "stat_cols": stat_cols,
    }
    return files, meta


# --------------------------------------------------------------------------- #
# Rendering (pure python)
# --------------------------------------------------------------------------- #
def _strike(s):
    return "".join(ch + _STRIKE for ch in s)


def _label(name, width, tomb):
    raw = name if width is None else _shorten(name, width)
    pad = " " * (0 if width is None else width - len(raw))
    return (_strike(raw) if tomb else raw) + pad


def _shorten(name, n=34):
    return name if len(name) <= n else name[:20] + "…" + name[-(n - 21):]


def _h(n):
    if n is None:
        return "?"
    n = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.0f}{u}" if u == "B" else f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}PB"


def _fmt(v):
    if v is None:
        return "-"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return str(v)
    if isinstance(v, (float, Decimal)):
        return f"{v:.2f}"
    s = str(v)
    return s if len(s) <= 19 else s[:18] + "…"


def _is_numeric(dtype):
    return dtype.split("(")[0] in _NUMERIC


def _key(v, numeric):
    return float(v) if numeric else str(v)


def _ranges(files, col, numeric):
    out = []
    for f in files:
        lo, hi = f["min"].get(col), f["max"].get(col)
        if lo is None or hi is None:
            continue
        try:
            out.append((_key(lo, numeric), _key(hi, numeric), f))
        except (TypeError, ValueError):
            continue
    return out


def _partition(f):
    return f["rel_path"].rsplit("/", 1)[0] if "/" in f["rel_path"] else ""


def _by_partition(rs):
    groups = {}
    for r in rs:
        groups.setdefault(_partition(r[2]), []).append(r)
    return groups


def _overlap(groups):
    """avg / max number of files (incl. itself) in the same partition whose
    [min,max] intersects a file's range. Files in other partitions never count,
    since partition pruning already separates them."""
    depths = []
    for g in groups.values():
        depths += [sum(1 for (l2, h2, _) in g if l2 <= h and h2 >= l) for (l, h, _) in g]
    return sum(depths) / len(depths), max(depths)


def _build_tree(files):
    root = {"dirs": {}, "files": []}
    for f in files:
        parts = f["rel_path"].split("/")
        node = root
        for d in parts[:-1]:
            node = node["dirs"].setdefault(d, {"dirs": {}, "files": []})
        f["name"] = parts[-1]
        node["files"].append(f)
    return root


def _all_files(node):
    yield from node["files"]
    for child in node["dirs"].values():
        yield from _all_files(child)


def _chart(rs, col, width, full_names, indent="  "):
    pts = sorted({r[0] for r in rs} | {r[1] for r in rs})
    if isinstance(pts[0], float):
        # numeric: linear on the actual values, each cell covers an equal slice of the range
        mn, span = pts[0], pts[-1] - pts[0]

        def pos(v):
            return 0 if span == 0 else min(int((v - mn) / span * width), width - 1)
    else:
        # strings etc.: only the ordering is meaningful, so scale by rank
        rank = {p: i for i, p in enumerate(pts)}
        denom = max(len(pts) - 1, 1)

        def pos(v):
            return round(rank[v] / denom * (width - 1))

    lines = []
    for lo, hi, f in sorted(rs, key=lambda r: (r[0], r[1])):
        l, r = pos(lo), pos(hi)
        bar = "·" * l + "█" * (r - l + 1) + "·" * (width - 1 - r)
        label = _label(f["name"], None if full_names else 22, False)
        lines.append(f"{indent}{label} {bar}  {_fmt(f['min'][col])} … {_fmt(f['max'][col])}")
    return lines


def render_tree(files, meta, show_tombstones=True, full_names=False,
                chart_cols=2, width=56, max_overlap_frac=0.6):
    dtypes = meta.get("dtypes", {})
    stat_cols = meta["stat_cols"]
    active = [f for f in files if f["state"] == "active"]
    tombs = [f for f in files if f["state"] == "tombstone"]
    shown = active + (tombs if show_tombstones else [])

    out = []
    tot_rows = sum(f["rows"] or 0 for f in active)
    tot_size = sum(f["size"] or 0 for f in active)
    out.append(f"Table      {meta.get('table')}")
    out.append(f"Location   {meta.get('location')}")
    out.append(f"Partitions {meta.get('partitionColumns') or '-'}   "
               f"Clustering {meta.get('clusteringColumns') or '-'}   "
               f"Z-Order {meta.get('zOrderColumns') or '-'}")
    out.append(f"Files      {len(active)} active, {len(tombs)} removed (tombstones)   "
               f"rows={tot_rows:,}   size={_h(tot_size)}")
    if meta.get("truncated_from"):
        out.append(f"(showing {len(files)} of {meta['truncated_from']} files)")
    out.append("")

    root = _build_tree(shown)

    def file_line(f):
        tomb = f["state"] == "tombstone"
        name = _label(f["name"], None if full_names else 34, tomb)
        rows = f"{f['rows']:>10,} rows" if f.get("rows") is not None else " " * 15
        return f"{name} {rows} {_h(f.get('size')):>8}"

    def walk(node, prefix):
        entries = [(n, "dir", c) for n, c in sorted(node["dirs"].items())]
        entries += [(f["name"], "file", f) for f in sorted(node["files"], key=lambda f: f["name"])]
        for i, (name, kind, obj) in enumerate(entries):
            last = i == len(entries) - 1
            branch, ext = ("└── ", "    ") if last else ("├── ", "│   ")
            if kind == "dir":
                fs = list(_all_files(obj))
                a = [x for x in fs if x["state"] == "active"]
                summ = (f"({len(a)} files, {sum(x['rows'] or 0 for x in a):,} rows, "
                        f"{_h(sum(x['size'] or 0 for x in a))}"
                        + (f", +{len(fs) - len(a)} removed" if len(fs) != len(a) else "") + ")")
                out.append(f"{prefix}{branch}{name}/  {summ}")
                walk(obj, prefix + ext)
            else:
                out.append(prefix + branch + file_line(obj))
                for i_rg, rg in enumerate(obj.get("row_groups") or []):
                    st = "  ".join(f"{c}[{_fmt(lo)} … {_fmt(hi)}]" for c, (lo, hi) in rg["stats"].items())
                    out.append(f"{prefix}{ext}    rg{i_rg}: {rg['rows']:,} rows  {st}")

    out.append(".")
    walk(root, "")

    # ---- inter-file summary -------------------------------------------------
    out.append("")
    out.append("Inter-file overlap (active files)")
    out.append("  avg overlap = how many files in the same partition have ranges intersecting "
               "a typical file's range; 1.0 = fully disjoint (best for data skipping)")
    out.append(f"  {'column':<18}{'type':<14}{'files':>6}{'no stats':>10}{'avg overlap':>13}{'max depth':>11}")
    per_col = {}
    for c in stat_cols:
        numeric = _is_numeric(dtypes.get(c, "string"))
        rs = _ranges(active, c, numeric)
        groups = None
        if not rs:
            out.append(f"  {c:<18}{dtypes.get(c, '?'):<14}{0:>6}{len(active):>10}{'-':>13}{'-':>11}")
        elif len(rs) > 2000:
            out.append(f"  {c:<18}{dtypes.get(c, '?'):<14}{len(rs):>6}{len(active) - len(rs):>10}{'(skipped: >2000 files)':>24}")
        else:
            groups = _by_partition(rs)
            avg, mx = _overlap(groups)
            out.append(f"  {c:<18}{dtypes.get(c, '?'):<14}{len(rs):>6}{len(active) - len(rs):>10}{avg:>13.2f}{mx:>11}")
        per_col[c] = groups

    # ---- range charts, one block per partition ------------------------------
    # A partition's chart only helps when ranges are somewhat separated. If a
    # typical file overlaps more than `max_overlap_frac` of the files in its
    # partition (and there are enough files for that to mean something), the
    # chart is just a wall of bars, so that partition is left out.
    for c in stat_cols[:chart_cols]:
        groups = per_col[c]
        if not groups:
            continue
        keep = {p: g for p, g in sorted(groups.items())
                if not (len(g) >= 4 and _overlap({p: g})[0] / len(g) > max_overlap_frac)}
        if not keep:
            continue
        out.append("")
        numeric = _is_numeric(dtypes.get(c, "string"))
        out.append(f"Range chart for '{c}'  (active files only" + ("" if numeric else "; rank-scaled axis") + ")")
        for p, g in keep.items():
            if p:
                out.append(f"  {p}/")
            out.extend(_chart(g, c, width, full_names, indent="    " if p else "  "))

    return "\n".join(out)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def show_delta_tree(table, row_groups=False, show_tombstones=True, full_names=False,
                    max_files=300, chart_cols=2, max_overlap_frac=0.6, back=1,
                    versions=None, spark=None):
    """Print the parquet file tree for a Delta table.

    row_groups  also print intra-file stats under each active file: row-group row counts
                and min/max of the stat columns, read from the parquet footers (needs
                direct storage access, so it usually fails on UC managed tables)
    max_overlap_frac  leave a partition out of a column's range chart when a typical file
                overlaps more than this fraction of the files in that partition
                (default 0.6; only applies with 4+ files in the partition)
    back        also scan this many versions before the latest to find tombstones
    versions    explicit list of versions to scan (the latest is always included)
    """
    from pyspark.sql import SparkSession

    spark = spark or SparkSession.getActiveSession()
    files, meta = collect_delta_files(spark, table, back, versions)
    files.sort(key=lambda f: (f["state"] != "active", f["rel_path"]))
    if len(files) > max_files:
        meta["truncated_from"] = len(files)
        files = files[:max_files]

    if row_groups:
        cols = set(meta["stat_cols"])
        try:
            for f in files:
                if f["state"] == "active":
                    f["row_groups"] = _row_groups(spark, f"{meta['location']}/{f['rel_path']}", cols)
        except Exception as e:  # e.g. shared clusters / serverless have no JVM access
            print(f"(row-group stats unavailable: {type(e).__name__}: {str(e)[:150]})\n")

    print(render_tree(files, meta, show_tombstones, full_names, chart_cols,
                      max_overlap_frac=max_overlap_frac))