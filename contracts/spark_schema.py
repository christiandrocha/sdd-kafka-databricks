from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

_PYSPARK_TYPE_NAMES: dict[str, str] = {
    "string":    "StringType",
    "integer":   "IntegerType",
    "long":      "LongType",
    "double":    "DoubleType",
    "boolean":   "BooleanType",
    "timestamp": "TimestampType",
    "date":      "DateType",
}

_SQL_TYPE_NAMES: dict[str, str] = {
    "string":    "STRING",
    "integer":   "INT",
    "long":      "BIGINT",
    "double":    "DOUBLE",
    "boolean":   "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date":      "DATE",
}


def _serialize_property_value(v: Any) -> str:
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, int):
        return str(v)
    return str(v)


def to_struct_type(contract: dict) -> StructType:
    """Gera StructType PySpark. Requer PySpark instalado no ambiente."""
    from pyspark.sql.types import (  # noqa: PLC0415
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    _type_map = {
        "string":    StringType(),
        "integer":   IntegerType(),
        "long":      LongType(),
        "double":    DoubleType(),
        "boolean":   BooleanType(),
        "timestamp": TimestampType(),
        "date":      DateType(),
    }

    return StructType([
        StructField(f["name"], _type_map[f["type"]], f["nullable"])
        for f in contract["schema"]
    ])


def to_tblproperties(contract: dict) -> dict[str, str]:
    """Gera TBLPROPERTIES como dict[str, str]. Não requer PySpark."""
    props = contract["storage"].get("properties", {})
    return {k: _serialize_property_value(v) for k, v in props.items()}


def to_cluster_by_sql(contract: dict) -> str:
    """Retorna 'CLUSTER BY (col1, col2)' ou '' se cluster_by vazio. Não requer PySpark."""
    cols: list[str] = contract["storage"].get("cluster_by", [])
    if not cols:
        return ""
    return f"CLUSTER BY ({', '.join(cols)})"


def to_create_table_ddl(contract: dict, table_fqn: str) -> str:
    """DDL completo CREATE TABLE IF NOT EXISTS. Não requer PySpark."""
    col_defs = [
        f"  {f['name']} {_SQL_TYPE_NAMES[f['type']]}"
        for f in contract["schema"]
    ]
    cols_sql = ",\n".join(col_defs)

    tblprops = to_tblproperties(contract)
    tblprops_lines = ",\n  ".join(f"'{k}' = '{v}'" for k, v in tblprops.items())

    cluster_clause = to_cluster_by_sql(contract)
    cluster_line = f"\n{cluster_clause}" if cluster_clause else ""

    return (
        f"CREATE TABLE IF NOT EXISTS {table_fqn} (\n"
        f"{cols_sql}\n"
        f") USING DELTA"
        f"{cluster_line}\n"
        f"TBLPROPERTIES (\n"
        f"  {tblprops_lines}\n"
        f")"
    )
