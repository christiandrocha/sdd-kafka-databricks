from pathlib import Path

import pytest

from contracts.dlt_adapter import (
    _condition_sql,
    quarantine_row_level_predicate,
    to_reject_expectations,
    to_warn_expectations,
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


@pytest.mark.parametrize("contract_path", ALL_CONTRACTS, ids=_IDS)
def test_quarantine_predicate_never_references_op_or_deleted(contract_path: Path) -> None:
    """DESIGN_DELETE_HANDLING.md Decision 1/2: a Debezium delete-rewrite row
    must be evaluated against the SAME quarantine predicate any other row
    would — its real field values (non-null thanks to REPLICA IDENTITY FULL,
    sql/init.sql) either satisfy each rule or don't, exactly like a live row.
    There is no __op/__deleted special-case here by design — that logic
    lives in register_silver()'s create_auto_cdc_flow(apply_as_deletes=...)
    instead. If this test ever fails, someone added op-aware routing to the
    quarantine gate, which would defeat Decision 1 (C08 fix)."""
    contract = load_contract(contract_path)
    predicate = quarantine_row_level_predicate(contract, scope="silver")
    if predicate:
        assert "__op" not in predicate
        assert "__deleted" not in predicate
