"""JSON Schema equivalent of pi's clone, optional-null cleanup, coercion, validation."""

import math
from copy import deepcopy

from jsonschema import Draft202012Validator


def _coerce(value, schema):
    for nested in schema.get("allOf", []):
        value = _coerce(value, nested)
    for keyword in ("anyOf", "oneOf"):
        choices = schema.get(keyword, [])
        if choices and not any(Draft202012Validator(s).is_valid(value) for s in choices):
            for choice in choices:
                candidate = _coerce(deepcopy(value), choice)
                if Draft202012Validator(choice).is_valid(candidate):
                    value = candidate
                    break
    types = schema.get("type", [])
    types = [types] if isinstance(types, str) else types
    if not any(Draft202012Validator({"type": t}).is_valid(value) for t in types):
        for kind in types:
            candidate = value
            if kind in ("number", "integer"):
                if value is None or isinstance(value, bool):
                    candidate = int(value or 0)
                elif isinstance(value, str) and value.strip():
                    try:
                        number = float(value)
                        if math.isfinite(number) and (kind == "number" or number.is_integer()):
                            candidate = int(number) if kind == "integer" else number
                    except ValueError:
                        pass
            elif kind == "boolean":
                if value is None or value == "false" or value == 0:
                    candidate = False
                elif value == "true" or value == 1:
                    candidate = True
            elif kind == "string" and (value is None or isinstance(value, (bool, int, float))):
                candidate = (
                    ""
                    if value is None
                    else str(value).lower()
                    if isinstance(value, bool)
                    else str(value)
                )
            elif kind == "null" and value in ("", 0, False):
                candidate = None
            if Draft202012Validator({"type": kind}).is_valid(candidate):
                value = candidate
                break
    if isinstance(value, dict):
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        for key in list(value):
            child = properties.get(key, schema.get("additionalProperties", {}))
            if not isinstance(child, dict):
                continue
            if (
                value[key] is None
                and key in properties
                and key not in required
                and "$ref" not in child
                and not Draft202012Validator(child).is_valid(None)
            ):
                del value[key]
            else:
                value[key] = _coerce(value[key], child)
    elif isinstance(value, list):
        items = schema.get("items", {})
        if isinstance(items, dict):
            value = [_coerce(item, items) for item in value]
    return value


def validate_tool_arguments(tool, arguments):
    schema = tool.parameters
    Draft202012Validator.check_schema(schema)
    result = _coerce(deepcopy(arguments), schema)
    errors = list(Draft202012Validator(schema).iter_errors(result))
    if errors:
        details = "\n".join(
            f"  - {'.'.join(map(str, e.path)) or 'root'}: {e.message}" for e in errors
        )
        raise ValueError(f'Validation failed for tool "{tool.name}":\n{details}')
    return result
