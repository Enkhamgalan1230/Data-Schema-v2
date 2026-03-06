from __future__ import annotations

from decimal import Decimal, InvalidOperation


def dtype_family(dtype: str) -> str:
    d = (dtype or "").upper()

    if "BOOL" in d:
        return "boolean"
    if "INT" in d:
        return "integer"
    if any(x in d for x in ["DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"]):
        return "float"
    if any(x in d for x in ["DATE", "TIME"]):
        return "datetime"
    if any(x in d for x in ["CHAR", "TEXT", "VARCHAR", "STRING"]):
        return "text"
    return "other"


def is_whole_number_decimal_string(value: str) -> bool:
    try:
        d = Decimal(value.strip())
        return d == d.to_integral_value()
    except (InvalidOperation, AttributeError):
        return False