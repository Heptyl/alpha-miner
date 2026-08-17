"""Small, dependency-free JSON Schema validator for brain handoff files."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


class SchemaValidationError(ValueError):
    """Raised when a handoff document does not match its JSON Schema."""


class SensitiveContentError(SchemaValidationError):
    """Raised when a handoff contains content that must not be persisted."""


_SENSITIVE_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"(?i)\b(password|passwd|token|secret|api[_-]?key)\s*[:=]\s*\S+"),
    re.compile(r"(?i)https?://\S+"),
    re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b"),
    re.compile(r"(?i)(?:[A-Z]:\\Users\\|/home/)[^/\\\s]+"),
)


def load_and_validate_json(document_path: Path, schema_path: Path) -> dict[str, Any]:
    """Load a JSON object and validate it against a local schema."""
    try:
        document = json.loads(document_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SchemaValidationError(f"invalid JSON in {document_path.name}") from exc

    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SchemaValidationError(f"invalid schema {schema_path.name}") from exc

    validate(document, schema)
    if not isinstance(document, dict):
        raise SchemaValidationError(f"{document_path.name} must contain a JSON object")
    reject_sensitive_content(document)
    return document


def validate(instance: Any, schema: dict[str, Any], path: str = "$") -> None:
    """Validate the JSON Schema keywords used by the three handoff schemas."""
    if "const" in schema and instance != schema["const"]:
        raise SchemaValidationError(f"{path} must equal {schema['const']!r}")
    if "enum" in schema and instance not in schema["enum"]:
        raise SchemaValidationError(f"{path} is not one of {schema['enum']!r}")

    expected_type = schema.get("type")
    if expected_type is not None and not _matches_type(instance, expected_type):
        raise SchemaValidationError(f"{path} must be {expected_type}")

    if isinstance(instance, dict):
        required = schema.get("required", [])
        missing = [key for key in required if key not in instance]
        if missing:
            raise SchemaValidationError(f"{path} missing required fields: {', '.join(missing)}")

        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            extras = sorted(set(instance) - set(properties))
            if extras:
                raise SchemaValidationError(
                    f"{path} contains unsupported fields: {', '.join(extras)}"
                )
        for key, child_schema in properties.items():
            if key in instance:
                validate(instance[key], child_schema, f"{path}.{key}")

    if isinstance(instance, list):
        _check_size(instance, schema, path, "Items")
        if schema.get("uniqueItems"):
            serialized = [json.dumps(item, sort_keys=True, ensure_ascii=False) for item in instance]
            if len(serialized) != len(set(serialized)):
                raise SchemaValidationError(f"{path} must contain unique items")
        item_schema = schema.get("items")
        if item_schema:
            for index, item in enumerate(instance):
                validate(item, item_schema, f"{path}[{index}]")

    if isinstance(instance, str):
        _check_size(instance, schema, path, "Length")
        pattern = schema.get("pattern")
        if pattern and re.search(pattern, instance) is None:
            raise SchemaValidationError(f"{path} does not match the required pattern")

    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        if "minimum" in schema and instance < schema["minimum"]:
            raise SchemaValidationError(f"{path} must be at least {schema['minimum']}")
        if "maximum" in schema and instance > schema["maximum"]:
            raise SchemaValidationError(f"{path} must be at most {schema['maximum']}")

    if "oneOf" in schema:
        matches = 0
        for candidate in schema["oneOf"]:
            try:
                validate(instance, candidate, path)
            except SchemaValidationError:
                continue
            matches += 1
        if matches != 1:
            raise SchemaValidationError(f"{path} must match exactly one allowed shape")


def reject_sensitive_content(value: Any, path: str = "$") -> None:
    """Reject common secrets, server URLs, and absolute user-home paths."""
    if isinstance(value, dict):
        for key, child in value.items():
            reject_sensitive_content(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            reject_sensitive_content(child, f"{path}[{index}]")
    elif isinstance(value, str):
        for pattern in _SENSITIVE_PATTERNS:
            if pattern.search(value):
                raise SensitiveContentError(f"{path} contains disallowed sensitive content")


def _matches_type(instance: Any, expected: str | list[str]) -> bool:
    if isinstance(expected, list):
        return any(_matches_type(instance, candidate) for candidate in expected)
    checks = {
        "object": lambda value: isinstance(value, dict),
        "array": lambda value: isinstance(value, list),
        "string": lambda value: isinstance(value, str),
        "integer": lambda value: isinstance(value, int) and not isinstance(value, bool),
        "number": lambda value: isinstance(value, (int, float)) and not isinstance(value, bool),
        "boolean": lambda value: isinstance(value, bool),
        "null": lambda value: value is None,
    }
    if expected not in checks:
        raise SchemaValidationError(f"unsupported schema type: {expected}")
    return checks[expected](instance)


def _check_size(value: list[Any] | str, schema: dict[str, Any], path: str, suffix: str) -> None:
    minimum = schema.get(f"min{suffix}")
    maximum = schema.get(f"max{suffix}")
    if minimum is not None and len(value) < minimum:
        raise SchemaValidationError(f"{path} must contain at least {minimum} items/characters")
    if maximum is not None and len(value) > maximum:
        raise SchemaValidationError(f"{path} must contain at most {maximum} items/characters")
