#!/usr/bin/env python3
"""Validate local or S3 JSON/JSONL files against a JSON schema."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Iterable, List, Optional, Sequence

import jsonschema  # type: ignore
from smart_open import open as smart_open  # type: ignore

from impresso_cookbook import (  # type: ignore
    get_s3_client,
    get_transport_params,
    parse_s3_path,
    setup_logging,
)

log = logging.getLogger(__name__)

JSONL_SUFFIXES = (".jsonl", ".jsonl.gz", ".jsonl.bz2")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Validate local/S3 JSON or JSONL files against a JSON schema."
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Schema path (local or s3://...).",
    )
    parser.add_argument(
        "--input",
        nargs="*",
        default=[],
        help="Input file(s), local or s3://...",
    )
    parser.add_argument(
        "--input-prefix",
        action="append",
        default=[],
        help="Local/S3 folder prefix; all JSONL files directly below it are validated.",
    )
    parser.add_argument(
        "--file-format",
        choices=["auto", "json", "jsonl"],
        default="auto",
        help="Input file format (default: auto by filename).",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=20,
        help="Stop after this many validation errors (default: 20).",
    )
    parser.add_argument(
        "--log-file",
        dest="log_file",
        default=None,
        help="Optional log output file (local or S3 path).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: %(default)s).",
    )
    args = parser.parse_args(argv)

    if not args.input and not args.input_prefix:
        parser.error("At least one --input or --input-prefix is required")
    if args.max_errors <= 0:
        parser.error("--max-errors must be > 0")
    return args


def is_jsonl_path(path: str) -> bool:
    """Return whether a path has a JSONL-like extension."""
    return path.lower().endswith(JSONL_SUFFIXES)


def infer_format(path: str, requested: str) -> str:
    """Infer json/jsonl from file name unless explicitly requested."""
    if requested != "auto":
        return requested
    return "jsonl" if is_jsonl_path(path) else "json"


def list_prefix_jsonl_files(prefix: str) -> List[str]:
    """List JSONL files directly below a local or S3 prefix."""
    files: List[str] = []
    if prefix.startswith("s3://"):
        bucket, key_prefix = parse_s3_path(prefix)
        normalized_prefix = key_prefix.rstrip("/")
        if normalized_prefix:
            normalized_prefix += "/"

        client = get_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                if not key or key.endswith("/"):
                    continue
                name = os.path.basename(key)
                if is_jsonl_path(name):
                    files.append(f"s3://{bucket}/{key}")
    else:
        if not os.path.isdir(prefix):
            raise ValueError(f"Input prefix is not a directory: {prefix}")
        for name in sorted(os.listdir(prefix)):
            path = os.path.join(prefix, name)
            if os.path.isfile(path) and is_jsonl_path(name):
                files.append(path)
    return sorted(files)


def load_schema(path: str) -> dict[str, Any]:
    """Load schema JSON."""
    with smart_open(
        path,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(path),
    ) as src:
        schema = json.load(src)
    if not isinstance(schema, dict):
        raise ValueError(f"Schema must be a JSON object: {path}")
    jsonschema.Draft202012Validator.check_schema(schema)
    return schema


def iter_jsonl(path: str) -> Iterable[tuple[int, Any]]:
    """Yield JSONL records with line numbers."""
    with smart_open(
        path,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(path),
    ) as src:
        for line_no, raw_line in enumerate(src, start=1):
            line = raw_line.strip()
            if not line:
                continue
            yield line_no, json.loads(line)


def iter_json(path: str) -> Iterable[tuple[int, Any]]:
    """Yield one JSON document, or each item if the document is an array."""
    with smart_open(
        path,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(path),
    ) as src:
        data = json.load(src)
    if isinstance(data, list):
        for idx, item in enumerate(data, start=1):
            yield idx, item
    else:
        yield 1, data


def validate_file(
    path: str,
    validator: jsonschema.Draft202012Validator,
    file_format: str,
    max_errors: int,
) -> tuple[int, int]:
    """Validate a single file and return (records, errors)."""
    records = 0
    errors = 0
    iterator = iter_jsonl(path) if file_format == "jsonl" else iter_json(path)

    for position, payload in iterator:
        records += 1
        first_error = next(validator.iter_errors(payload), None)
        if first_error is None:
            continue

        errors += 1
        log.error(
            "Schema validation failed in %s at record/line %d: %s",
            path,
            position,
            first_error.message,
        )
        if errors >= max_errors:
            break

    return records, errors


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    setup_logging(args.log_level, args.log_file, logger=log)

    try:
        schema = load_schema(args.schema)
        validator = jsonschema.Draft202012Validator(schema)
        inputs = list(args.input)
        for prefix in args.input_prefix:
            inputs.extend(list_prefix_jsonl_files(prefix))
    except Exception:
        log.exception("Validation initialization failed")
        return 1

    if not inputs:
        log.error("No input files found")
        return 1

    total_records = 0
    total_errors = 0
    for input_path in sorted(inputs):
        file_format = infer_format(input_path, args.file_format)
        try:
            records, errors = validate_file(
                input_path,
                validator=validator,
                file_format=file_format,
                max_errors=args.max_errors - total_errors,
            )
        except Exception:
            log.exception("Could not validate %s", input_path)
            return 1

        total_records += records
        total_errors += errors
        log.info(
            "Validated %s: records=%d errors=%d",
            input_path,
            records,
            errors,
        )
        if total_errors >= args.max_errors:
            break

    if total_errors:
        log.error(
            "Validation failed: files=%d records=%d errors=%d",
            len(inputs),
            total_records,
            total_errors,
        )
        return 1

    log.info("Validation passed: files=%d records=%d", len(inputs), total_records)
    return 0


if __name__ == "__main__":
    sys.exit(main())
