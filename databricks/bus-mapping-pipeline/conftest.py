"""pytest configuration: install Spark/DLT mocks before the pipeline module is imported.

The sibling pipeline directories install their own stubs with
`sys.modules.setdefault`, so whichever conftest pytest loads first owns the
`pyspark` and `dlt` module objects for the whole session. This one therefore
*augments* whatever is already registered instead of assuming it can create it,
adding only the surfaces bus_mapping_pipeline needs (real StructType/StructField
so schema.fields[i].name stays readable, a catch-all attribute fallback,
dlt.read). The other suites treat their schemas as opaque, so widening the
shared stubs cannot affect them.
"""

import builtins
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# Ensure the bus-mapping-pipeline directory is importable so
# `import bus_mapping_pipeline` works.
sys.path.insert(0, str(Path(__file__).parent))


class _StructField:
    """Stand-in for pyspark.sql.types.StructField that keeps its name readable.

    The pipeline derives pandas column names from schema.fields, so unlike the
    other type stubs this one cannot be a MagicMock.
    """

    def __init__(self, name, dataType=None, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable


class _StructType:
    def __init__(self, fields=None):
        self.fields = list(fields or [])


def _module(name: str) -> types.ModuleType:
    """Return the registered stub for name, creating and registering it if absent."""
    existing = sys.modules.get(name)
    if existing is None:
        existing = types.ModuleType(name)
        sys.modules[name] = existing
    return existing


def _install_pyspark() -> None:
    ps = _module("pyspark")
    pss = _module("pyspark.sql")
    psf = _module("pyspark.sql.functions")
    pst = _module("pyspark.sql.types")

    ps.sql = getattr(ps, "sql", pss)
    pss.functions = getattr(pss, "functions", psf)
    pss.types = getattr(pss, "types", pst)

    # Catch-all so every pyspark.sql.functions name (col, lit, count,
    # collect_set, array_intersect, try_divide, broadcast, ...) resolves
    # without enumerating everything this pipeline touches.
    if "__getattr__" not in vars(psf):
        psf.__getattr__ = lambda name: MagicMock()

    pst.StructField = _StructField
    pst.StructType = _StructType
    if "__getattr__" not in vars(pst):
        pst.__getattr__ = lambda name: MagicMock()


def _install_dlt() -> None:
    dlt = _module("dlt")
    if not hasattr(dlt, "table"):
        dlt.table = lambda **kwargs: lambda f: f
    for attr in ("read", "read_stream"):
        if not hasattr(dlt, attr):
            setattr(dlt, attr, MagicMock())


def _install_spark_builtin() -> None:
    """Inject a mock `spark` into builtins -- the DLT runtime provides this automatically."""
    if hasattr(builtins, "spark"):
        return
    mock_spark = MagicMock()
    # Every blf.bus_mapping_* parameter falls through to its default, so the
    # module imports with exactly the configuration a fresh pipeline would have
    # (empty reference paths -> _BUS_NATIVE_ROWS == []).
    mock_spark.conf.get.side_effect = lambda key, default="": {
        "blf.target_catalog": "main",
        "blf.target_schema": "blf_test",
    }.get(key, default)
    builtins.spark = mock_spark  # type: ignore[attr-defined]


_install_pyspark()
_install_dlt()
_install_spark_builtin()
