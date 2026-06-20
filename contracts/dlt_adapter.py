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

    check=unique nao tem expressao linha-a-linha (exige comparar contra outras linhas) e
    nao e' enforced em quarantine — e' uma propriedade de merge-time do Silver, nao do
    estagio Bronze->Silver pre-merge; ver docs/adr/007_pipeline_unification.md Addendum 6.
    """
    rules = [
        r for r in contract["quality"]["rules"]
        if scope in r["scope"] and r["on_failure"] == "quarantine" and r["check"] != "unique"
    ]
    if not rules:
        return None
    fail_conditions = [f"NOT ({_condition_sql(r)})" for r in rules]
    return " OR ".join(fail_conditions)


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
