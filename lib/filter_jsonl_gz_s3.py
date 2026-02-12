#!/usr/bin/env python3
"""
Filter an S3 .jsonl.gz file by `tp`, minimum `ocrqa`, and minimum `len`.

The output is written to the same S3 folder with `_filtered` added to the
filename stem by default.

Example:
    python lib/filter_jsonl_gz_s3.py \
      --input-s3 s3://115-canonical-processed-final/langident/langident-lid-ensemble_multilingual_v2-0-2__AGGREGATED.jsonl.gz \
      --tp article \
      --min-ocrqa 0.7 \
      --min-len 550
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Optional, Sequence

from smart_open import open as smart_open  # type: ignore
from impresso_cookbook import get_transport_params, setup_logging  # type: ignore

log = logging.getLogger(__name__)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Filter a .jsonl.gz file in S3 and write *_filtered.jsonl.gz."
    )
    parser.add_argument(
        "--input-s3",
        required=True,
        help="Input S3 path to .jsonl.gz (example: s3://bucket/path/file.jsonl.gz).",
    )
    parser.add_argument(
        "--output-s3",
        default=None,
        help="Optional output S3 path. Default is input name + '_filtered'.",
    )
    parser.add_argument(
        "--tp",
        default=None,
        help="Exact value for field `tp` (for example: page).",
    )
    parser.add_argument(
        "--min-ocrqa",
        type=float,
        default=None,
        help="Keep records with ocrqa >= this value.",
    )
    parser.add_argument(
        "--min-len",
        type=int,
        default=None,
        help="Keep records with len >= this value.",
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

    if not args.input_s3.startswith("s3://"):
        parser.error("--input-s3 must be an S3 path starting with s3://")
    if args.output_s3 and not args.output_s3.startswith("s3://"):
        parser.error("--output-s3 must be an S3 path starting with s3://")

    return args


def derive_output_path(input_s3: str) -> str:
    """Derive output path from input by appending `_filtered` to file stem."""
    if input_s3.endswith(".jsonl.gz"):
        return f"{input_s3[:-9]}_filtered.jsonl.gz"
    return f"{input_s3}_filtered"


def to_float(value: Any) -> Optional[float]:
    """Convert value to float, returning None when conversion is not possible."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> Optional[int]:
    """Convert value to int, returning None when conversion is not possible."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def matches_filters(
    record: dict[str, Any],
    tp: Optional[str],
    min_ocrqa: Optional[float],
    min_len: Optional[int],
) -> bool:
    """Check whether a record matches all requested filters."""
    if tp is not None and record.get("tp") != tp:
        return False

    if min_ocrqa is not None:
        score = to_float(record.get("ocrqa"))
        if score is None or score < min_ocrqa:
            return False

    if min_len is not None:
        length = to_int(record.get("len"))
        if length is None or length < min_len:
            return False

    return True


def filter_file(
    input_s3: str,
    output_s3: str,
    tp: Optional[str],
    min_ocrqa: Optional[float],
    min_len: Optional[int],
) -> tuple[int, int, int]:
    """
    Filter the input JSONL file and write the filtered output.

    Returns:
        Tuple with (total_records, kept_records, invalid_json_lines).
    """
    total = 0
    kept = 0
    invalid = 0

    with smart_open(
        input_s3,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(input_s3),
    ) as src, smart_open(
        output_s3,
        "wt",
        encoding="utf-8",
        transport_params=get_transport_params(output_s3),
    ) as dst:
        for line_no, raw_line in enumerate(src, start=1):
            line = raw_line.strip()
            if not line:
                continue

            total += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid += 1
                log.warning("Skipping invalid JSON at line %d", line_no)
                continue

            if matches_filters(record, tp, min_ocrqa, min_len):
                dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                kept += 1

    return total, kept, invalid


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    setup_logging(args.log_level, args.log_file, logger=log)

    output_s3 = args.output_s3 or derive_output_path(args.input_s3)
    log.info("Input: %s", args.input_s3)
    log.info("Output: %s", output_s3)
    log.info(
        "Filters: tp=%s, min_ocrqa=%s, min_len=%s",
        args.tp,
        args.min_ocrqa,
        args.min_len,
    )

    try:
        total, kept, invalid = filter_file(
            input_s3=args.input_s3,
            output_s3=output_s3,
            tp=args.tp,
            min_ocrqa=args.min_ocrqa,
            min_len=args.min_len,
        )
    except Exception:
        log.exception("Filtering failed")
        return 1

    log.info(
        "Done. total=%d kept=%d dropped=%d invalid_json=%d",
        total,
        kept,
        total - kept,
        invalid,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
