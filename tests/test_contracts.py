from pathlib import Path

import pytest

from contracts.loader import load_contract
from contracts.spark_schema import to_tblproperties

CONTRACTS_DIR = Path("contracts")
ALL_CONTRACTS = sorted(CONTRACTS_DIR.glob("*.yml"))
_IDS = [p.stem for p in ALL_CONTRACTS]


def test_contract_count() -> None:
    assert len(ALL_CONTRACTS) == 20, f"Expected 20 contracts, found {len(ALL_CONTRACTS)}"


@pytest.mark.parametrize("contract_path", ALL_CONTRACTS, ids=_IDS)
def test_01_all_contracts_load_without_error(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    assert set(contract.keys()) >= {"table", "schema", "quality", "storage", "schema_evolution"}


@pytest.mark.parametrize("contract_path", ALL_CONTRACTS, ids=_IDS)
def test_02_nullable_is_bool_not_string(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    for field in contract["schema"]:
        assert isinstance(field["nullable"], bool), (
            f"{contract_path.stem}.{field['name']}:"
            f" nullable={field['nullable']!r} must be bool, not {type(field['nullable']).__name__}"
        )


@pytest.mark.parametrize("contract_path", ALL_CONTRACTS, ids=_IDS)
def test_03_quality_rules_reference_existing_fields(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    field_names = {f["name"] for f in contract["schema"]}
    for rule in contract["quality"]["rules"]:
        assert rule["field"] in field_names, (
            f"{contract_path.stem}: quality rule references"
            f" '{rule['field']}' which is not in schema"
        )


@pytest.mark.parametrize("contract_path", ALL_CONTRACTS, ids=_IDS)
def test_04_allowed_values_are_non_empty(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    for rule in contract["quality"]["rules"]:
        if rule.get("check") == "allowed_values":
            values = rule.get("values")
            assert isinstance(values, list) and len(values) > 0, (
                f"{contract_path.stem}.{rule['field']}:"
                f" allowed_values must be a non-empty list, got {values!r}"
            )


@pytest.mark.parametrize("contract_path", ALL_CONTRACTS, ids=_IDS)
def test_05_cluster_by_is_subset_of_schema_fields(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    field_names = {f["name"] for f in contract["schema"]}
    for col in contract["storage"]["cluster_by"]:
        assert col in field_names, (
            f"{contract_path.stem}: cluster_by column '{col}' not found in schema fields"
        )


@pytest.mark.parametrize("contract_path", ALL_CONTRACTS, ids=_IDS)
def test_06_merge_key_in_cluster_by(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    merge_key: str = contract["table"]["merge_key"]
    cluster_by: list[str] = contract["storage"]["cluster_by"]
    assert merge_key in cluster_by, (
        f"{contract_path.stem}: merge_key '{merge_key}'"
        f" not in cluster_by {cluster_by} (ADR-04 violation)"
    )


@pytest.mark.parametrize("contract_path", ALL_CONTRACTS, ids=_IDS)
def test_07_tblproperties_values_are_strings(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    props = to_tblproperties(contract)
    for key, value in props.items():
        assert isinstance(value, str), (
            f"{contract_path.stem}: TBLPROPERTIES['{key}'] = {value!r}"
            f" must be str, not {type(value).__name__}"
        )
