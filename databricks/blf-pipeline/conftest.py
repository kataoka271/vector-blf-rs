"""pytest configuration: install Spark/DLT mocks before the pipeline module is imported."""

import builtins
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# Ensure the databricks/ directory is importable so `import dlt_blf_pipeline` works.
sys.path.insert(0, str(Path(__file__).parent))


def _install_pyspark() -> None:
    pst = types.ModuleType("pyspark.sql.types")
    for _cls in [
        "ArrayType",
        "BinaryType",
        "BooleanType",
        "ByteType",
        "DoubleType",
        "IntegerType",
        "LongType",
        "StringType",
        "StructField",
        "StructType",
        "TimestampType",
    ]:
        setattr(pst, _cls, MagicMock())

    psf = types.ModuleType("pyspark.sql.functions")
    # pandas_udf must be an identity decorator so decorated functions stay callable.
    psf.pandas_udf = lambda schema: lambda f: f
    for _name in [
        "col",
        "lit",
        "when",
        "concat_ws",
        "format_string",
        "hex",
        "lpad",
        "upper",
        "substring",
        "size",
        "length",
        "explode",
    ]:
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
        "blf.source_path": "/Volumes/test/blf/raw",
        "blf.container_long_header": "false",
    }.get(key, default)
    builtins.spark = mock_spark  # type: ignore[attr-defined]


_install_pyspark()
_install_dlt()
_install_spark_builtin()
