"""pytest configuration: install Spark/DLT mocks before the pipeline module is imported."""

import builtins
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# Ensure this directory is importable so `import signal_docs_pipeline` works.
sys.path.insert(0, str(Path(__file__).parent))


def _install_pyspark() -> None:
    pst = types.ModuleType("pyspark.sql.types")
    for _cls in [
        "IntegerType",
        "LongType",
        "StringType",
        "StructField",
        "StructType",
        "TimestampType",
    ]:
        setattr(pst, _cls, MagicMock())

    psf = types.ModuleType("pyspark.sql.functions")
    for _name in ["col", "lit", "expr"]:
        setattr(psf, _name, MagicMock())
    psf.Column = MagicMock  # used in type annotations e.g. -> F.Column

    pss = types.ModuleType("pyspark.sql")
    pss.functions = psf
    pss.types = pst

    ps = types.ModuleType("pyspark")
    ps.sql = pss

    sys.modules.setdefault("pyspark", ps)
    sys.modules.setdefault("pyspark.sql", pss)
    sys.modules.setdefault("pyspark.sql.functions", psf)
    sys.modules.setdefault("pyspark.sql.types", pst)


def _install_dlt() -> None:
    dlt = types.ModuleType("dlt")
    dlt.table = lambda **kwargs: lambda f: f
    dlt.read_stream = MagicMock()
    sys.modules.setdefault("dlt", dlt)


def _install_spark_builtin() -> None:
    """Inject a mock `spark` into builtins — the DLT runtime provides this automatically."""
    mock_spark = MagicMock()
    mock_spark.conf.get.side_effect = lambda key, default="": {
        "blf.signal_docs_path": "/Volumes/test/blf/docs",
    }.get(key, default)
    builtins.spark = mock_spark  # type: ignore[attr-defined]


_install_pyspark()
_install_dlt()
_install_spark_builtin()
