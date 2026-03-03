#!/usr/bin/env python3
"""
Enrich JSONL records with `title` fetched via s3 compiler by record ID.

Input/output can be local paths or S3 paths supported by smart_open.

Example:
    python lib/enrich_jsonl_title_with_compiler.py \
      --input-path ./data/all/sample_non_ads.jsonl \
      --output-path ./data/all/sample_non_ads_with_title.jsonl \
      --compiler-s3-prefix s3://122-rebuilt-final/
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from smart_open import open as smart_open  # type: ignore
from impresso_cookbook import get_transport_params, setup_logging  # type: ignore

log = logging.getLogger(__name__)


@dataclass
class CompilerBatch:
    """One s3 compiler batch grouped by newspaper-year patterns."""

    ids: List[str]
    pattern_count: int


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Fill missing title field in JSONL records using s3 compiler lookup by ID."
    )
    parser.add_argument(
        "--input-path",
        required=True,
        help="Input JSONL(.gz) path (local or s3://...).",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Output JSONL(.gz) path (local or s3://...).",
    )
    parser.add_argument(
        "--compiler-s3-prefix",
        required=True,
        help="S3 prefix for source JSONL.bz2 files (example: s3://122-rebuilt-final/).",
    )
    parser.add_argument(
        "--id-field",
        default="id",
        help="ID field used for lookup (default: id).",
    )
    parser.add_argument(
        "--title-field",
        default="title",
        help="Title field to write when available (default: title).",
    )
    parser.add_argument(
        "--compiler-match-field",
        default="id",
        help="Field name in compiler source to match IDs (default: id).",
    )
    parser.add_argument(
        "--compiler-workers",
        type=int,
        default=4,
        help="Parallel compiler workers (default: 4). Set 1 to disable parallelism.",
    )
    parser.add_argument(
        "--compiler-pattern-batch-size",
        type=int,
        default=120,
        help=(
            "Approximate newspaper-year patterns per compiler batch (default: 120). "
            "Lower gives more frequent ETA updates."
        ),
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=500,
        help="Write-phase progress log interval in records (default: 500).",
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

    if args.input_path == args.output_path:
        parser.error("--input-path and --output-path must be different")
    if not args.compiler_s3_prefix.startswith("s3://"):
        parser.error("--compiler-s3-prefix must start with s3://")
    if args.compiler_workers <= 0:
        parser.error("--compiler-workers must be > 0")
    if args.compiler_pattern_batch_size <= 0:
        parser.error("--compiler-pattern-batch-size must be > 0")
    if args.progress_every <= 0:
        parser.error("--progress-every must be > 0")

    return args


def as_str(value: Any) -> str:
    """Convert value to trimmed string."""
    if value is None:
        return ""
    return str(value).strip()


def _jq_accessor(field_name: str) -> str:
    """Build robust JQ field accessor for arbitrary field names."""
    return f".[{json.dumps(field_name)}]"


def _build_compiler_transform_expr(title_field: str, match_field: str) -> str:
    """Build transform expression requesting only id and title."""
    return (
        "{"
        + f"\"id\": {_jq_accessor(match_field)}, "
        + f"{json.dumps(title_field)}: {_jq_accessor(title_field)}"
        + "}"
    )


def _ensure_local_parent(path: str) -> None:
    """Create local output parent directory when needed."""
    if path.startswith("s3://"):
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _format_eta(seconds: float) -> str:
    """Format ETA seconds as HH:MM:SS."""
    if seconds < 0:
        seconds = 0
    whole = int(round(seconds))
    hours, rem = divmod(whole, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _compiler_pattern_for_id(record_id: str) -> str:
    """Best-effort newspaper-year pattern extraction from canonical IDs."""
    parts = record_id.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return record_id


def _build_compiler_batches(
    record_ids: Sequence[str],
    pattern_batch_size: int,
) -> Tuple[List[CompilerBatch], int]:
    """
    Group IDs by newspaper-year and pack into batches by number of patterns.

    Returns:
        (batches, total_unique_patterns)
    """
    ids_by_pattern: Dict[str, List[str]] = {}
    for record_id in record_ids:
        pattern = _compiler_pattern_for_id(record_id)
        ids_by_pattern.setdefault(pattern, []).append(record_id)

    patterns = sorted(ids_by_pattern.keys())
    batches: List[CompilerBatch] = []

    current_ids: List[str] = []
    current_patterns = 0
    for pattern in patterns:
        current_ids.extend(ids_by_pattern[pattern])
        current_patterns += 1

        if current_patterns >= pattern_batch_size:
            batches.append(CompilerBatch(ids=current_ids, pattern_count=current_patterns))
            current_ids = []
            current_patterns = 0

    if current_ids:
        batches.append(CompilerBatch(ids=current_ids, pattern_count=current_patterns))

    return batches, len(patterns)


def _log_compiler_progress(
    started: float,
    done_batches: int,
    total_batches: int,
    done_patterns: int,
    total_patterns: int,
    done_ids: int,
    total_ids: int,
) -> None:
    """Emit compiler-phase progress and ETA."""
    elapsed = time.monotonic() - started
    pattern_rate = done_patterns / elapsed if elapsed > 0 else 0.0
    remaining_patterns = max(total_patterns - done_patterns, 0)
    eta = (remaining_patterns / pattern_rate) if pattern_rate > 0 else 0.0
    progress_pct = (done_patterns / total_patterns) * 100.0 if total_patterns else 100.0
    log.info(
        "Compiler progress: batches=%d/%d patterns=%d/%d ids=%d/%d (%.1f%%), eta=%s",
        done_batches,
        total_batches,
        done_patterns,
        total_patterns,
        done_ids,
        total_ids,
        progress_pct,
        _format_eta(eta),
    )


def collect_missing_title_ids(args: argparse.Namespace) -> Tuple[List[str], int]:
    """Collect IDs for records that do not already have title."""
    missing_ids: Set[str] = set()
    invalid_json = 0
    missing_id = 0
    valid_records = 0

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

            valid_records += 1
            if as_str(record.get(args.title_field)):
                continue

            record_id = as_str(record.get(args.id_field))
            if not record_id:
                missing_id += 1
                continue

            missing_ids.add(record_id)

    if invalid_json:
        log.warning("Skipped %d invalid JSON lines while scanning input", invalid_json)
    if missing_id:
        log.warning(
            "Found %d records missing '%s' and '%s'; cannot enrich those",
            missing_id,
            args.title_field,
            args.id_field,
        )
    return sorted(missing_ids), valid_records


def _fetch_title_map_batch_with_s3_compiler(
    batch_ids: Sequence[str],
    args: argparse.Namespace,
) -> Dict[str, str]:
    """Fetch title map for one batch of IDs via impresso s3 compiler."""
    try:
        from impresso_cookbook.s3_compiler import S3CompilerProcessor  # type: ignore
    except Exception as exc:
        raise RuntimeError("Could not import impresso_cookbook.s3_compiler") from exc

    fd_ids, ids_path = tempfile.mkstemp(prefix="title_ids_", suffix=".jsonl")
    os.close(fd_ids)
    fd_out, out_path = tempfile.mkstemp(prefix="title_compiled_", suffix=".jsonl")
    os.close(fd_out)

    try:
        with smart_open(ids_path, "wt", encoding="utf-8") as dst:
            for record_id in sorted(set(batch_ids)):
                dst.write(json.dumps({"id": record_id}, ensure_ascii=False) + "\n")

        processor = S3CompilerProcessor(
            input_file=ids_path,
            s3_prefix=args.compiler_s3_prefix,
            output_file=out_path,
            id_field="id",
            transform_expr=_build_compiler_transform_expr(
                title_field=args.title_field,
                match_field=args.compiler_match_field,
            ),
            match_field=args.compiler_match_field,
            log_level=args.log_level,
            log_file=None,
        )

        try:
            processor.run()
        except SystemExit as exc:
            raise RuntimeError(f"s3 compiler failed with exit code {exc.code}") from exc

        title_map: Dict[str, str] = {}
        with smart_open(out_path, "rt", encoding="utf-8") as src:
            for line_no, raw_line in enumerate(src, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    log.warning("Skipping invalid JSON from compiler at line %d", line_no)
                    continue

                record_id = as_str(record.get("id"))
                title_value = as_str(record.get(args.title_field))
                if record_id and title_value:
                    title_map[record_id] = title_value

        return title_map
    finally:
        if os.path.exists(ids_path):
            os.remove(ids_path)
        if os.path.exists(out_path):
            os.remove(out_path)


def fetch_title_map_with_s3_compiler(
    missing_ids: Sequence[str],
    args: argparse.Namespace,
) -> Dict[str, str]:
    """Fetch title values for IDs via impresso s3 compiler with progress and ETA."""
    if not missing_ids:
        return {}

    unique_ids = sorted(set(missing_ids))
    batches, total_patterns = _build_compiler_batches(
        record_ids=unique_ids,
        pattern_batch_size=args.compiler_pattern_batch_size,
    )
    total_batches = len(batches)

    log.info(
        "Compiler lookup plan: ids=%d patterns=%d batches=%d workers=%d batch_patterns<=%d",
        len(unique_ids),
        total_patterns,
        total_batches,
        args.compiler_workers,
        args.compiler_pattern_batch_size,
    )

    title_map: Dict[str, str] = {}
    done_batches = 0
    done_patterns = 0
    done_ids = 0
    started = time.monotonic()

    if args.compiler_workers == 1 or total_batches == 1:
        for batch in batches:
            batch_map = _fetch_title_map_batch_with_s3_compiler(batch.ids, args)
            title_map.update(batch_map)

            done_batches += 1
            done_patterns += batch.pattern_count
            done_ids += len(batch.ids)
            _log_compiler_progress(
                started=started,
                done_batches=done_batches,
                total_batches=total_batches,
                done_patterns=done_patterns,
                total_patterns=total_patterns,
                done_ids=done_ids,
                total_ids=len(unique_ids),
            )
        return title_map

    with ThreadPoolExecutor(max_workers=args.compiler_workers) as executor:
        future_to_batch = {
            executor.submit(_fetch_title_map_batch_with_s3_compiler, batch.ids, args): batch
            for batch in batches
        }
        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            batch_map = future.result()
            title_map.update(batch_map)

            done_batches += 1
            done_patterns += batch.pattern_count
            done_ids += len(batch.ids)
            _log_compiler_progress(
                started=started,
                done_batches=done_batches,
                total_batches=total_batches,
                done_patterns=done_patterns,
                total_patterns=total_patterns,
                done_ids=done_ids,
                total_ids=len(unique_ids),
            )

    return title_map


def enrich_file_with_titles(
    args: argparse.Namespace,
    title_map: Dict[str, str],
    total_expected_records: Optional[int] = None,
) -> Dict[str, int]:
    """Write output with title field filled when available."""
    total = 0
    enriched = 0
    already_had_title = 0
    still_missing_title = 0
    invalid_json = 0
    started = time.monotonic()
    progress_every = args.progress_every

    _ensure_local_parent(args.output_path)

    with smart_open(
        args.input_path,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(args.input_path),
    ) as src, smart_open(
        args.output_path,
        "wt",
        encoding="utf-8",
        transport_params=get_transport_params(args.output_path),
    ) as dst:
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

            current_title = as_str(record.get(args.title_field))
            if current_title:
                already_had_title += 1
            else:
                record_id = as_str(record.get(args.id_field))
                fetched_title = title_map.get(record_id, "")
                if fetched_title:
                    record[args.title_field] = fetched_title
                    enriched += 1
                else:
                    still_missing_title += 1

            dst.write(json.dumps(record, ensure_ascii=False) + "\n")
            total += 1

            if total_expected_records and total % progress_every == 0:
                elapsed = time.monotonic() - started
                rate = total / elapsed if elapsed > 0 else 0.0
                remaining = max(total_expected_records - total, 0)
                eta = (remaining / rate) if rate > 0 else 0.0
                progress_pct = (total / total_expected_records) * 100.0
                log.info(
                    "Output progress: %d/%d (%.1f%%), eta=%s",
                    total,
                    total_expected_records,
                    progress_pct,
                    _format_eta(eta),
                )

    return {
        "total_records_written": total,
        "records_enriched_with_title": enriched,
        "records_already_had_title": already_had_title,
        "records_still_missing_title": still_missing_title,
        "invalid_json_lines_skipped": invalid_json,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    setup_logging(args.log_level, args.log_file, logger=log)

    log.info("Input: %s", args.input_path)
    log.info("Output: %s", args.output_path)
    log.info("Compiler source prefix: %s", args.compiler_s3_prefix)

    try:
        missing_ids, total_expected_records = collect_missing_title_ids(args)
        log.info("Need title enrichment for %d unique IDs", len(missing_ids))

        title_map = fetch_title_map_with_s3_compiler(missing_ids, args)
        log.info("Fetched titles for %d IDs via s3 compiler", len(title_map))

        stats = enrich_file_with_titles(
            args,
            title_map,
            total_expected_records=total_expected_records,
        )
    except Exception:
        log.exception("Title enrichment failed")
        return 1

    log.info(
        "Done. written=%d enriched=%d already_had=%d still_missing=%d invalid_json=%d",
        stats["total_records_written"],
        stats["records_enriched_with_title"],
        stats["records_already_had_title"],
        stats["records_still_missing_title"],
        stats["invalid_json_lines_skipped"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
