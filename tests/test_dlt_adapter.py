from pathlib import Path

import pytest

from contracts.dlt_adapter import (
    _condition_sql,
    quarantine_row_level_predicate,
    to_reject_expectations,
    to_warn_expectations,
    unique_check_fields,
)
from contracts.loader import load_contract

CONTRACTS_DIR = Path("contracts")
ALL_CONTRACTS = sorted(CONTRACTS_DIR.glob("*.yml"))
_IDS = [p.stem for p in ALL_CONTRACTS]


@pytest.mark.parametrize("contract_path", ALL_CONTRACTS, ids=_IDS)
def test_adapter_runs_on_every_real_contract(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    to_reject_expectations(contract, scope="bronze")
    to_warn_expectations(contract, scope="silver")
    quarantine_row_level_predicate(contract, scope="silver")
    unique_check_fields(contract, scope="silver")


def test_condition_sql_not_null() -> None:
    assert _condition_sql({"field": "uuid", "check": "not_null"}) == "uuid IS NOT NULL"


def test_condition_sql_allowed_values() -> None:
    rule = {"field": "status", "check": "allowed_values", "values": ["a", "b"]}
    assert _condition_sql(rule) == "status IS NULL OR status IN ('a', 'b')"


def test_condition_sql_not_future() -> None:
    rule = {"field": "dt", "check": "not_future"}
    assert _condition_sql(rule) == "dt IS NULL OR dt <= current_timestamp()"


def test_condition_sql_unique_is_always_true() -> None:
    assert _condition_sql({"field": "cnpj", "check": "unique"}) == "true"


def test_condition_sql_unknown_check_raises() -> None:
    with pytest.raises(ValueError, match="unknown check type"):
        _condition_sql({"field": "x", "check": "bogus"})


def test_to_reject_expectations_filters_by_scope_and_on_failure() -> None:
    contract = {
        "quality": {
            "rules": [
                {"field": "a", "check": "not_null", "on_failure": "reject", "scope": ["bronze"]},
                {
                    "field": "b", "check": "not_null",
                    "on_failure": "quarantine", "scope": ["bronze"],
                },
                {"field": "c", "check": "not_null", "on_failure": "reject", "scope": ["silver"]},
            ]
        }
    }
    result = to_reject_expectations(contract, scope="bronze")
    assert result == {"a_not_null": "a IS NOT NULL"}


def test_to_warn_expectations_filters_by_scope_and_on_failure() -> None:
    contract = {
        "quality": {
            "rules": [
                {"field": "a", "check": "not_future", "on_failure": "warn", "scope": ["silver"]},
                {"field": "b", "check": "not_future", "on_failure": "reject", "scope": ["silver"]},
            ]
        }
    }
    result = to_warn_expectations(contract, scope="silver")
    assert result == {"a_not_future": "a IS NULL OR a <= current_timestamp()"}


def test_quarantine_row_level_predicate_combines_with_or() -> None:
    contract = {
        "quality": {
            "rules": [
                {
                    "field": "a", "check": "not_null",
                    "on_failure": "quarantine", "scope": ["silver"],
                },
                {
                    "field": "b", "check": "not_null",
                    "on_failure": "quarantine", "scope": ["silver"],
                },
            ]
        }
    }
    predicate = quarantine_row_level_predicate(contract, scope="silver")
    assert predicate == "NOT (a IS NOT NULL) OR NOT (b IS NOT NULL)"


def test_quarantine_row_level_predicate_excludes_unique_check() -> None:
    contract = {
        "quality": {
            "rules": [
                {"field": "a", "check": "unique", "on_failure": "quarantine", "scope": ["silver"]},
            ]
        }
    }
    assert quarantine_row_level_predicate(contract, scope="silver") is None


def test_quarantine_row_level_predicate_none_when_no_rules() -> None:
    contract = {"quality": {"rules": []}}
    assert quarantine_row_level_predicate(contract, scope="silver") is None


def test_unique_check_fields_returns_only_unique_quarantine_rules() -> None:
    contract = {
        "quality": {
            "rules": [
                {
                    "field": "a", "check": "unique",
                    "on_failure": "quarantine", "scope": ["silver"],
                },
                {
                    "field": "b", "check": "not_null",
                    "on_failure": "quarantine", "scope": ["silver"],
                },
                {"field": "c", "check": "unique", "on_failure": "warn", "scope": ["silver"]},
            ]
        }
    }
    assert unique_check_fields(contract, scope="silver") == ["a"]


@pytest.mark.parametrize("domain", ["drivers", "restaurants"])
def test_drivers_and_restaurants_unique_rule_reaches_adapter(domain: str) -> None:
    contract = load_contract(CONTRACTS_DIR / f"{domain}.yml")
    fields = unique_check_fields(contract, scope="silver")
    assert len(fields) == 1
