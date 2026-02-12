#!/usr/bin/env python3
"""
Split a classified JSONL dataset into per-language files and write a summary JSON.

Input can be local or S3, and can be `.jsonl`, `.jsonl.gz`, or `.jsnol`.
Output can be local or S3.

Examples:
    python lib/split_by_language_jsonl_s3.py \
      --input-path ./data/sample_classified.jsonl.gz \
      --output-prefix ./data/per_language

    python lib/split_by_language_jsonl_s3.py \
      --input-path s3://my-bucket/data/sample_classified.jsonl.gz \
      --output-prefix s3://my-bucket/data/per_language \
      --summary-name languages_summary.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, TextIO

from smart_open import open as smart_open  # type: ignore
from impresso_cookbook import get_transport_params, setup_logging  # type: ignore

log = logging.getLogger(__name__)


@dataclass
class LanguageStats:
    """Aggregate statistics for one language."""

    records: int = 0
    total_len: int = 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Split local/S3 JSONL into per-language files and write language summary JSON."
        )
    )
    parser.add_argument(
        "--input-path",
        required=True,
        help="Input path (local or s3://...) to .jsonl/.jsonl.gz/.jsnol.",
    )
    parser.add_argument(
        "--output-prefix",
        required=True,
        help=(
            "Output folder/prefix (local dir or s3://bucket/prefix) where per-language "
            "files and summary JSON are written."
        ),
    )
    parser.add_argument(
        "--language-field",
        default="lg",
        help="Field containing language code (default: lg).",
    )
    parser.add_argument(
        "--length-field",
        default="len",
        help="Field used to sum lengths per language (default: len).",
    )
    parser.add_argument(
        "--text-field",
        default="ft",
        help="Fallback text field when length is missing (default: ft).",
    )
    parser.add_argument(
        "--unknown-language",
        default="unknown",
        help="Language bucket used when language field is missing (default: unknown).",
    )
    parser.add_argument(
        "--summary-name",
        default="languages_summary.json",
        help="Summary JSON filename (default: languages_summary.json).",
    )
    parser.add_argument(
        "--gzip-output",
        action="store_true",
        help="Force per-language outputs to .jsonl.gz (default: follow input extension).",
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

    lowered = args.input_path.lower()
    if not (
        lowered.endswith(".jsonl")
        or lowered.endswith(".jsonl.gz")
        or lowered.endswith(".jsnol")
    ):
        parser.error("--input-path must end with .jsonl, .jsonl.gz, or .jsnol")

    if args.output_prefix.startswith("s3://") and args.output_prefix == "s3://":
        parser.error("--output-prefix must include bucket and key prefix")

    return args


def as_str(value: Any) -> str:
    """Convert value to trimmed string."""
    if value is None:
        return ""
    return str(value).strip()


def to_int(value: Any) -> Optional[int]:
    """Convert value to int when possible."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def sanitize_for_filename(value: str, fallback: str) -> str:
    """Make a stable safe filename fragment from language value."""
    candidate = as_str(value) or fallback
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", candidate)
    return safe.strip("._-") or fallback


def derive_output_extension(input_path: str, force_gzip: bool) -> str:
    """Pick per-language output extension."""
    if force_gzip:
        return ".jsonl.gz"
    lowered = input_path.lower()
    if lowered.endswith(".jsonl.gz"):
        return ".jsonl.gz"
    return ".jsonl"


def join_output_path(prefix: str, filename: str) -> str:
    """Join filename under local or S3 output prefix."""
    if prefix.startswith("s3://"):
        base = prefix.rstrip("/")
        return f"{base}/{filename}"
    return os.path.join(prefix, filename)


def get_record_length(record: Dict[str, Any], length_field: str, text_field: str) -> int:
    """Get record length from length_field or text fallback."""
    length = to_int(record.get(length_field))
    if length is not None and length >= 0:
        return length
    return len(as_str(record.get(text_field)))


def ensure_local_dir(prefix: str) -> None:
    """Create local output directory if needed."""
    if prefix.startswith("s3://"):
        return
    os.makedirs(prefix, exist_ok=True)


def process_file(args: argparse.Namespace) -> Dict[str, Any]:
    """Split input by language and return summary object."""
    ensure_local_dir(args.output_prefix)

    output_ext = derive_output_extension(args.input_path, args.gzip_output)
    per_language_paths: Dict[str, str] = {}
    writers: Dict[str, TextIO] = {}
    stats: Dict[str, LanguageStats] = {}

    total_records = 0
    total_len = 0
    invalid_json = 0

    try:
        with smart_open(
            args.input_path,
            "rt",
            encoding="utf-8",
            transport_params=get_transport_params(args.input_path),
        ) as src:
            for line_no, raw_line in enumerate(src, start=1):
                line = raw_line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    invalid_json += 1
                    log.warning("Skipping invalid JSON at line %d", line_no)
                    continue

                language_raw = as_str(record.get(args.language_field))
                language = language_raw or args.unknown_language
                safe_language = sanitize_for_filename(language, args.unknown_language)

                if safe_language not in writers:
                    filename = f"{safe_language}{output_ext}"
                    output_path = join_output_path(args.output_prefix, filename)
                    writers[safe_language] = smart_open(
                        output_path,
                        "wt",
                        encoding="utf-8",
                        transport_params=get_transport_params(output_path),
                    )
                    per_language_paths[safe_language] = output_path
                    stats[safe_language] = LanguageStats()
                    log.info("Opened output for language '%s': %s", safe_language, output_path)

                writers[safe_language].write(json.dumps(record, ensure_ascii=False) + "\n")

                length = get_record_length(record, args.length_field, args.text_field)
                stats[safe_language].records += 1
                stats[safe_language].total_len += length
                total_records += 1
                total_len += length

    finally:
        for handle in writers.values():
            handle.close()

    languages_summary = []
    for language in sorted(stats.keys()):
        languages_summary.append(
            {
                "language": language,
                "records": stats[language].records,
                "total_len": stats[language].total_len,
                "output_path": per_language_paths[language],
            }
        )

    return {
        "input_path": args.input_path,
        "output_prefix": args.output_prefix,
        "language_field": args.language_field,
        "length_field": args.length_field,
        "total_records": total_records,
        "total_len": total_len,
        "invalid_json_lines": invalid_json,
        "languages": [entry["language"] for entry in languages_summary],
        "languages_summary": languages_summary,
    }


def write_summary(output_prefix: str, summary_name: str, summary: Dict[str, Any]) -> str:
    """Write summary JSON under output prefix."""
    summary_path = join_output_path(output_prefix, summary_name)
    with smart_open(
        summary_path,
        "wt",
        encoding="utf-8",
        transport_params=get_transport_params(summary_path),
    ) as dst:
        dst.write(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    return summary_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    setup_logging(args.log_level, args.log_file, logger=log)

    log.info("Input: %s", args.input_path)
    log.info("Output prefix: %s", args.output_prefix)

    try:
        summary = process_file(args)
        summary_path = write_summary(args.output_prefix, args.summary_name, summary)
    except Exception:
        log.exception("Splitting failed")
        return 1

    log.info(
        "Done. languages=%d total_records=%d total_len=%d invalid_json=%d",
        len(summary["languages"]),
        summary["total_records"],
        summary["total_len"],
        summary["invalid_json_lines"],
    )
    log.info("Summary: %s", summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
