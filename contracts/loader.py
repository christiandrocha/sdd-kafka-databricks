from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

VALID_TYPES: frozenset[str] = frozenset(
    {"string", "integer", "long", "double", "boolean", "timestamp", "date"}
)
VALID_CHECKS: frozenset[str] = frozenset({"not_null", "allowed_values", "not_future", "unique"})
VALID_SCOPES: frozenset[str] = frozenset({"bronze", "silver"})


def _validate_schema(table_name: str, schema: list[dict]) -> None:
    seen: set[str] = set()
    for field in schema:
        name = field.get("name")
        if not isinstance(name, str):
            raise ValueError(f"{table_name}: schema field missing string 'name'")
        if name in seen:
            raise ValueError(f"{table_name}: duplicate field '{name}'")
        seen.add(name)

        ftype = field.get("type")
        if ftype not in VALID_TYPES:
            raise ValueError(
                f"{table_name}.{name}: invalid type '{ftype}',"
                f" must be one of {sorted(VALID_TYPES)}"
            )

        nullable = field.get("nullable")
        if not isinstance(nullable, bool):
            raise ValueError(
                f"{table_name}.{name}: 'nullable' must be bool,"
                f" got {type(nullable).__name__!r} (value: {nullable!r})"
            )


def _validate_quality(
    table_name: str, field_names: set[str], quality: dict
) -> None:
    for rule in quality.get("rules", []):
        field = rule.get("field")
        if field not in field_names:
            raise ValueError(
                f"{table_name}: quality rule references field '{field}' not in schema"
            )

        check = rule.get("check")
        if check not in VALID_CHECKS:
            raise ValueError(
                f"{table_name}.{field}: invalid check '{check}',"
                f" must be one of {sorted(VALID_CHECKS)}"
            )

        if check == "allowed_values":
            values = rule.get("values")
            if not isinstance(values, list) or len(values) == 0:
                raise ValueError(
                    f"{table_name}.{field}: 'allowed_values' check requires non-empty 'values' list"
                )

        scope = rule.get("scope", [])
        if not isinstance(scope, list):
            raise ValueError(
                f"{table_name}.{field}: 'scope' must be a list,"
                f" got {type(scope).__name__!r}"
            )
        for s in scope:
            if s not in VALID_SCOPES:
                raise ValueError(
                    f"{table_name}.{field}: invalid scope '{s}',"
                    f" must be one of {sorted(VALID_SCOPES)}"
                )


def _validate_storage(
    table_name: str, field_names: set[str], storage: dict, merge_key: str
) -> None:
    cluster_by = storage.get("cluster_by")
    if not isinstance(cluster_by, list):
        raise ValueError(f"{table_name}: storage.cluster_by must be a list")

    for col in cluster_by:
        if col not in field_names:
            raise ValueError(
                f"{table_name}: cluster_by column '{col}' not found in schema fields"
            )

    if merge_key and merge_key not in cluster_by:
        raise ValueError(
            f"{table_name}: merge_key '{merge_key}' not in cluster_by {cluster_by} (ADR-04)"
        )


def load_contract(path: str | Path) -> dict[str, Any]:
    """
    Carrega e valida semanticamente um contrato YAML.

    Raises:
        FileNotFoundError: se o arquivo não existe
        ValueError: se o contrato é semanticamente inválido,
                    com mensagem incluindo table.name e campo problemático
    """
    path = Path(path)
    contract: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))

    required_keys = {"table", "schema", "quality", "storage", "schema_evolution"}
    missing = required_keys - contract.keys()
    if missing:
        raise ValueError(f"{path.name}: missing required keys {sorted(missing)}")

    table_name: str = contract["table"]["name"]
    merge_key: str = contract["table"].get("merge_key", "")
    field_names = {f["name"] for f in contract["schema"]}

    _validate_schema(table_name, contract["schema"])
    _validate_quality(table_name, field_names, contract["quality"])
    _validate_storage(table_name, field_names, contract["storage"], merge_key)

    return contract
