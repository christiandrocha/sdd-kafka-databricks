from __future__ import annotations


def to_reject_expectations(contract: dict, scope: str) -> dict[str, str]:
    """Regras on_failure=reject do scope dado -> dict para @dp.expect_all_or_drop."""
    return {
        f"{r['field']}_{r['check']}": _condition_sql(r)
        for r in contract["quality"]["rules"]
        if scope in r["scope"] and r["on_failure"] == "reject"
    }


def to_warn_expectations(contract: dict, scope: str) -> dict[str, str]:
    """Regras on_failure=warn do scope dado -> dict para @dp.expect_all (nao bloqueia)."""
    return {
        f"{r['field']}_{r['check']}": _condition_sql(r)
        for r in contract["quality"]["rules"]
        if scope in r["scope"] and r["on_failure"] == "warn"
    }


def quarantine_row_level_predicate(contract: dict, scope: str) -> str | None:
    """SQL boolean: True quando a linha FALHA alguma regra quarantine que nao seja 'unique'.

    check=unique e' tratado separadamente (unique_check_fields) porque exige um anti-join
    contra o estado atual da tabela Silver, nao uma expressao linha-a-linha.
    """
    rules = [
        r for r in contract["quality"]["rules"]
        if scope in r["scope"] and r["on_failure"] == "quarantine" and r["check"] != "unique"
    ]
    if not rules:
        return None
    fail_conditions = [f"NOT ({_condition_sql(r)})" for r in rules]
    return " OR ".join(fail_conditions)


def unique_check_fields(contract: dict, scope: str) -> list[str]:
    """Campos com check=unique, on_failure=quarantine, no scope dado."""
    return [
        r["field"] for r in contract["quality"]["rules"]
        if scope in r["scope"] and r["on_failure"] == "quarantine" and r["check"] == "unique"
    ]


def _condition_sql(rule: dict) -> str:
    """SQL boolean: True quando a linha PASSA a regra (semantica de _rule_fail_expr invertida)."""
    field, check = rule["field"], rule["check"]
    if check == "not_null":
        return f"{field} IS NOT NULL"
    if check == "allowed_values":
        values = ", ".join(f"'{v}'" for v in rule["values"])
        return f"{field} IS NULL OR {field} IN ({values})"
    if check == "not_future":
        return f"{field} IS NULL OR {field} <= current_timestamp()"
    if check == "unique":
        return "true"
    raise ValueError(f"unknown check type for DLT translation: {check!r}")
