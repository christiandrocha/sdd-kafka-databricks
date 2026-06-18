from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, create_model

from contracts.loader import load_contract

_CONTRACTS_DIR: Path = Path(__file__).parent

_YAML_TO_PYTHON: dict[str, type] = {
    "string":    str,
    "integer":   int,
    "long":      int,
    "double":    float,
    "boolean":   bool,
    "timestamp": datetime,
    "date":      date,
}


def _yaml_type_to_annotation(yaml_type: str, nullable: bool) -> tuple[Any, Any]:
    python_type = _YAML_TO_PYTHON[yaml_type]
    if nullable:
        return python_type | None, None
    return python_type, ...


def _contract_to_model(contract: dict) -> type[BaseModel]:
    fields_kwargs: dict[str, Any] = {}
    for field in contract["schema"]:
        name: str = field["name"]
        if name.startswith("_"):
            continue
        annotation, default = _yaml_type_to_annotation(field["type"], field["nullable"])
        fields_kwargs[name] = (annotation, default)
    table_name: str = contract["table"]["name"]
    return create_model(table_name, **fields_kwargs)


def _load_all_models() -> dict[str, type[BaseModel]]:
    models: dict[str, type[BaseModel]] = {}
    for path in sorted(_CONTRACTS_DIR.glob("*.yml")):
        contract = load_contract(path)
        table_name: str = contract["table"]["name"]
        models[table_name] = _contract_to_model(contract)
    return models


_MODELS: dict[str, type[BaseModel]] = _load_all_models()


def get_model(table_name: str) -> type[BaseModel]:
    """
    Retorna modelo Pydantic v2 para o domínio especificado.

    Args:
        table_name: nome da tabela (ex: "payment_events", "orders")

    Raises:
        KeyError: se table_name não corresponde a nenhum contrato YAML
    """
    if table_name not in _MODELS:
        raise KeyError(
            f"'{table_name}' not found in models. Available: {sorted(_MODELS)}"
        )
    return _MODELS[table_name]
