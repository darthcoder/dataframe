"""
Python bindings for the Haskell dataframe library via Arrow C Data Interface.

Usage::

    import dataframe_hs as hdf

    df  = hdf.read_csv("data/titanic.csv")
    res = df.groupBy(["Sex"]).aggregate({
        "survived_sum": hdf.sum(hdf.col("Survived")),
        "n":            hdf.count(hdf.col("Sex")),
    })
    print(res.to_pandas())
"""

import ctypes
import json
import os
import glob as _glob

_ARROW_SCHEMA_SIZE = 72   # 9 × 8 bytes
_ARROW_ARRAY_SIZE  = 80   # 10 × 8 bytes

def _find_dylib() -> str:
    """Find libdataframe-arrow.dylib / .so, preferring a copy next to this
    package and falling back to the cabal dist-newstyle build tree."""
    here = os.path.dirname(os.path.abspath(__file__))

    # 1. Adjacent to the package directory (installed / copied manually)
    for ext in ("dylib", "so"):
        candidate = os.path.join(here, f"libdataframe-arrow.{ext}")
        if os.path.exists(candidate):
            return candidate

    # 2. Cabal dist-newstyle build tree (development mode)
    repo_root = here
    for _ in range(6):  # walk up at most 6 levels
        repo_root = os.path.dirname(repo_root)
        patterns = [
            os.path.join(repo_root, "dist-newstyle", "**", "libdataframe-arrow.dylib"),
            os.path.join(repo_root, "dist-newstyle", "**", "libdataframe-arrow.so"),
        ]
        candidates = []
        for pat in patterns:
            candidates.extend(_glob.glob(pat, recursive=True))
        if candidates:
            # Pick the most-recently-modified build
            return max(candidates, key=os.path.getmtime)
        if os.path.exists(os.path.join(repo_root, "dataframe.cabal")):
            break  # we reached the repo root; no point going higher

    raise FileNotFoundError(
        "Could not locate libdataframe-arrow.dylib / .so.  "
        "Run `cabal build dataframe-arrow` first."
    )


_lib_path = _find_dylib()
_lib = ctypes.CDLL(_lib_path)

# ---------------------------------------------------------------------------
# Configure ctypes function signature
# ---------------------------------------------------------------------------

_lib.dfExecutePlan.restype = ctypes.c_int
_lib.dfExecutePlan.argtypes = [
    ctypes.c_char_p,                   # const char* plan_json
    ctypes.POINTER(ctypes.c_uint64),   # uint64_t*   schema_out
    ctypes.POINTER(ctypes.c_uint64),   # uint64_t*   array_out
]

# ---------------------------------------------------------------------------
# Expression helpers
# ---------------------------------------------------------------------------

class ColExpr:
    def __init__(self, name: str):
        self._name = name


class AggExpr:
    def __init__(self, fn: str, col: "ColExpr"):
        self._fn  = fn
        self._col = col._name


def col(name: str) -> ColExpr:
    """Reference a column by name."""
    return ColExpr(name)


def sum(expr: ColExpr) -> AggExpr:
    """Sum aggregation."""
    return AggExpr("sum", expr)


def mean(expr: ColExpr) -> AggExpr:
    """Mean aggregation."""
    return AggExpr("mean", expr)


def count(expr: ColExpr) -> AggExpr:
    """Count aggregation."""
    return AggExpr("count", expr)

# ---------------------------------------------------------------------------
# EagerFrame
# ---------------------------------------------------------------------------

class EagerFrame:
    """Holds a query plan dict; terminal operations execute it immediately."""

    def __init__(self, plan: dict, _refs=None):
        self._plan = plan
        self._refs = _refs or []

    def select(self, cols: list) -> "EagerFrame":
        return EagerFrame({"op": "Select", "cols": cols, "input": self._plan}, self._refs)

    def sort(self, cols: list, ascending: bool = True) -> "EagerFrame":
        return EagerFrame({
            "op": "Sort",
            "cols": cols,
            "ascending": ascending,
            "input": self._plan,
        }, self._refs)

    def limit(self, n: int) -> "EagerFrame":
        return EagerFrame({"op": "Limit", "n": n, "input": self._plan}, self._refs)

    def groupBy(self, keys: list) -> "GroupedEagerFrame":
        return GroupedEagerFrame(self._plan, keys, self._refs)

    def collect(self):
        """Execute the plan and return a pyarrow.RecordBatch."""
        return _execute(self._plan, self._refs)


class GroupedEagerFrame:
    """Holds a plan + grouping keys; aggregate() executes immediately."""

    def __init__(self, plan: dict, keys: list, _refs=None):
        self._plan = plan
        self._keys = keys
        self._refs = _refs or []

    def aggregate(self, aggs: dict):
        """Run aggregation and return a pyarrow.RecordBatch.

        Parameters
        ----------
        aggs:
            ``{output_name: AggExpr}`` mapping, e.g.
            ``{"total": hdf.sum(hdf.col("Amount"))}``.
        """
        agg_list = [
            {"name": k, "agg": v._fn, "col": v._col}
            for k, v in aggs.items()
        ]
        plan = {
            "op": "GroupBy",
            "keys": self._keys,
            "aggregations": agg_list,
            "input": self._plan,
        }
        return _execute(plan, self._refs)

# ---------------------------------------------------------------------------
# Internal execution helper
# ---------------------------------------------------------------------------

def _execute(plan: dict, _refs=None):
    """Serialize *plan* to JSON, call Haskell, and return a RecordBatch."""
    import pyarrow as pa  # deferred so the module loads without pyarrow

    plan_bytes = json.dumps(plan).encode()
    schema_ptr = ctypes.c_uint64(0)
    array_ptr  = ctypes.c_uint64(0)

    rc = _lib.dfExecutePlan(
        plan_bytes,
        ctypes.byref(schema_ptr),
        ctypes.byref(array_ptr),
    )
    if rc != 0:
        raise RuntimeError("dfExecutePlan failed (check stderr for details)")

    # _import_from_c takes (array_addr, schema_addr) – note the order.
    result = pa.RecordBatch._import_from_c(array_ptr.value, schema_ptr.value)
    del _refs  # keep alive until after import, then release
    return result

# ---------------------------------------------------------------------------
# Public constructors
# ---------------------------------------------------------------------------

def read_csv(path: str) -> EagerFrame:
    """Create an EagerFrame that reads a CSV file."""
    return EagerFrame({"op": "ReadCsv", "path": path})


def read_tsv(path: str) -> EagerFrame:
    """Create an EagerFrame that reads a TSV file."""
    return EagerFrame({"op": "ReadTsv", "path": path})


def from_arrow(arrow_obj) -> EagerFrame:
    """Create an EagerFrame from any Arrow-compatible object.

    Parameters
    ----------
    arrow_obj:
        A pyarrow RecordBatch or Table (or any object with _export_to_c).
        Use ``polars_df.to_arrow()`` or ``pandas_df.to_arrow()`` to convert first.
    """
    import pyarrow as pa
    # Normalize to a single contiguous RecordBatch
    if isinstance(arrow_obj, pa.Table):
        rb = arrow_obj.combine_chunks().to_batches()[0]
    else:
        rb = arrow_obj  # RecordBatch or compatible
    schema_buf = ctypes.create_string_buffer(_ARROW_SCHEMA_SIZE)
    array_buf  = ctypes.create_string_buffer(_ARROW_ARRAY_SIZE)
    schema_addr = ctypes.addressof(schema_buf)
    array_addr  = ctypes.addressof(array_buf)
    rb._export_to_c(array_addr, schema_addr)   # pyarrow: (array, schema) order
    plan = {"op": "FromArrow", "schema": schema_addr, "array": array_addr}
    return EagerFrame(plan, _refs=[schema_buf, array_buf, rb])
