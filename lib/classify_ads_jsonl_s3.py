#!/usr/bin/env python3
"""
Classify article records as ad/non-ad from local or S3 JSONL/JSONL.BZ2 input.

Only records with `tp == "ar"` are classified.
Output always contains `id` and `tp`, and article rows also contain:
    {"id": "...", "tp": "ar", "ad_classification": "ad" | "non-ad"}

Examples:
    python lib/classify_ads_jsonl_s3.py \
      --input-path ./data/input.jsonl.bz2 \
      --output-path ./data/input_ad_classification.jsonl.bz2

    python lib/classify_ads_jsonl_s3.py \
      --input-path s3://bucket/path/input.jsonl \
      --output-path s3://bucket/path/input_ad_classification.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from smart_open import open as smart_open  # type: ignore
from impresso_cookbook import get_transport_params, setup_logging  # type: ignore

log = logging.getLogger(__name__)

DEFAULT_AD_VALUE = "ad"
DEFAULT_NON_AD_VALUE = "non-ad"


@dataclass
class ClassifierConfig:
    """Runtime config for ad classifier calls."""

    diagnostics: bool
    precision: Optional[int]
    batch_size: int


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Classify local/S3 JSONL(.bz2) article records with the impresso ad classifier "
            "and write minimal output with id, tp, and optional decision."
        )
    )
    parser.add_argument(
        "--input-path",
        required=True,
        help="Input path (local or s3://...) to .jsonl or .jsonl.bz2.",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Optional output path. Default is input name + '_ad_classification'.",
    )
    parser.add_argument(
        "--id-field",
        default="id",
        help="ID field name (default: id).",
    )
    parser.add_argument(
        "--text-field",
        default="ft",
        help="Text field used by classifier (default: ft).",
    )
    parser.add_argument(
        "--type-field",
        default="tp",
        help="Type field name used to detect articles (default: tp).",
    )
    parser.add_argument(
        "--type-value",
        default="ar",
        help="Required article type value (default: ar).",
    )
    parser.add_argument(
        "--class-field",
        default="ad_classification",
        help="Output class field name (default: ad_classification).",
    )
    parser.add_argument(
        "--classifier-batch-size",
        type=int,
        default=64,
        help="Batch size for classifier inference (default: 64).",
    )
    parser.add_argument(
        "--pipeline-diagnostics",
        action="store_true",
        help="Enable diagnostics mode when constructing AdClassifierPipeline.",
    )
    parser.add_argument(
        "--pipeline-precision",
        type=int,
        default=2,
        help="Precision argument forwarded to pipeline(..., precision=...).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=500,
        help="Log progress every N classified records (default: 500).",
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
    if not (lowered.endswith(".jsonl") or lowered.endswith(".jsonl.bz2")):
        parser.error("--input-path must end with .jsonl or .jsonl.bz2")
    if args.output_path is not None:
        lowered_out = args.output_path.lower()
        if not (lowered_out.endswith(".jsonl") or lowered_out.endswith(".jsonl.bz2")):
            parser.error("--output-path must end with .jsonl or .jsonl.bz2")
    if args.classifier_batch_size <= 0:
        parser.error("--classifier-batch-size must be > 0")
    if args.progress_every <= 0:
        parser.error("--progress-every must be > 0")

    return args


def derive_output_path(input_path: str) -> str:
    """Derive output path from input by appending `_ad_classification` to the stem."""
    lowered = input_path.lower()
    if lowered.endswith(".jsonl.bz2"):
        return f"{input_path[:-10]}_ad_classification.jsonl.bz2"
    if lowered.endswith(".jsonl"):
        return f"{input_path[:-6]}_ad_classification.jsonl"
    return f"{input_path}_ad_classification"


def ensure_local_parent(path: str) -> None:
    """Create local output directory if needed."""
    if path.startswith("s3://"):
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def as_str(value: Any) -> str:
    """Convert value to a normalized string."""
    if value is None:
        return ""
    return str(value).strip()


def _extract_type_label_from_output(output: Any) -> Optional[str]:
    """Extract raw classifier type label when available."""
    if isinstance(output, list):
        if not output:
            return None
        return _extract_type_label_from_output(output[0])

    if isinstance(output, dict) and "type" in output:
        label = as_str(output.get("type"))
        if label:
            return label
    return None


def _build_ad_classifier(config: ClassifierConfig) -> Any:
    """Instantiate impresso internal AdClassifierPipeline."""
    try:
        from impresso_pipelines.adclassifier import AdClassifierPipeline  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Could not import impresso internal ad classifier. "
            "Install with: pip install 'impresso-pipelines[adclassifier]'"
        ) from exc

    return AdClassifierPipeline(diagnostics=config.diagnostics)


def _pipeline_predict_batch(
    pipeline: Any,
    texts: Sequence[str],
    precision: Optional[int],
) -> List[Any]:
    """Run pipeline on a batch, with compatibility fallbacks."""

    def _call(payload: Any) -> Any:
        if precision is None:
            return pipeline(payload)
        try:
            return pipeline(payload, precision=precision)
        except TypeError:
            return pipeline(payload)

    result = _call(list(texts))
    if isinstance(result, list) and len(result) == len(texts):
        return result
    if len(texts) == 1:
        return result if isinstance(result, list) else [result]

    outputs: List[Any] = []
    for text in texts:
        single = _call(text)
        if isinstance(single, list):
            outputs.append(single[0] if single else {})
        else:
            outputs.append(single)
    return outputs


def classify_batch(
    records: Sequence[Tuple[str, str, str]],
    pipeline: Any,
    config: ClassifierConfig,
) -> List[Dict[str, str]]:
    """Classify a batch of (id, tp, text) tuples and return minimal output records."""
    texts = [text for _, _, text in records]
    outputs = _pipeline_predict_batch(
        pipeline=pipeline,
        texts=texts,
        precision=config.precision,
    )

    if len(outputs) != len(records):
        raise RuntimeError(
            f"Classifier returned {len(outputs)} results for {len(records)} inputs"
        )

    classified: List[Dict[str, str]] = []
    for (record_id, record_type, _text), output in zip(records, outputs):
        type_label = _extract_type_label_from_output(output)
        if not type_label:
            raise RuntimeError(
                "Classifier output is missing required 'type' field for "
                f"record id={record_id}"
            )

        final_label = type_label.lower()
        if final_label not in {DEFAULT_AD_VALUE, DEFAULT_NON_AD_VALUE}:
            raise RuntimeError(
                "Unexpected classifier 'type' value "
                f"'{type_label}' for record id={record_id}"
            )

        classified.append(
            {
                "id": record_id,
                "tp": record_type,
                "ad_classification": final_label,
            }
        )

    return classified


def classify_file(
    input_path: str,
    output_path: str,
    args: argparse.Namespace,
    pipeline: Any,
) -> Dict[str, int]:
    """Classify eligible article records from input and write minimal output."""
    total_lines = 0
    classified_records = 0
    invalid_json = 0
    passthrough_non_articles = 0
    skipped_missing_id = 0
    skipped_missing_text = 0
    pending: List[Tuple[str, str, str]] = []

    def flush_batch(dst: Any) -> None:
        nonlocal pending, classified_records
        if not pending:
            return
        classified = classify_batch(
            records=pending,
            pipeline=pipeline,
            config=ClassifierConfig(
                diagnostics=args.pipeline_diagnostics,
                precision=args.pipeline_precision,
                batch_size=args.classifier_batch_size,
            ),
        )
        for record in classified:
            record[args.class_field] = record.pop("ad_classification")
            dst.write(json.dumps(record, ensure_ascii=False) + "\n")
        classified_records += len(classified)
        pending = []

        if classified_records % args.progress_every == 0:
            log.info(
                "Progress: classified=%d passthrough_non_articles=%d invalid_json=%d",
                classified_records,
                passthrough_non_articles,
                invalid_json,
            )

    with smart_open(
        input_path,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(input_path),
    ) as src, smart_open(
        output_path,
        "wt",
        encoding="utf-8",
        transport_params=get_transport_params(output_path),
    ) as dst:
        for line_no, raw_line in enumerate(src, start=1):
            line = raw_line.strip()
            if not line:
                continue

            total_lines += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid_json += 1
                log.warning("Skipping invalid JSON at line %d", line_no)
                continue

            record_id = as_str(record.get(args.id_field))
            if not record_id:
                skipped_missing_id += 1
                continue

            record_type = as_str(record.get(args.type_field))
            if record_type != as_str(args.type_value):
                dst.write(
                    json.dumps(
                        {
                            "id": record_id,
                            args.type_field: record_type,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                passthrough_non_articles += 1
                continue

            text = as_str(record.get(args.text_field))
            if not text:
                skipped_missing_text += 1
                continue

            pending.append((record_id, record_type, text))
            if len(pending) >= args.classifier_batch_size:
                flush_batch(dst)

        flush_batch(dst)

    return {
        "total_lines": total_lines,
        "classified_records": classified_records,
        "invalid_json": invalid_json,
        "passthrough_non_articles": passthrough_non_articles,
        "skipped_missing_id": skipped_missing_id,
        "skipped_missing_text": skipped_missing_text,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    setup_logging(args.log_level, args.log_file, logger=log)

    output_path = args.output_path or derive_output_path(args.input_path)
    ensure_local_parent(output_path)

    log.info("Input: %s", args.input_path)
    log.info("Output: %s", output_path)
    log.info(
        "Filtering/classification: %s == %s, text_field=%s, batch_size=%d",
        args.type_field,
        args.type_value,
        args.text_field,
        args.classifier_batch_size,
    )

    try:
        pipeline = _build_ad_classifier(
            ClassifierConfig(
                diagnostics=args.pipeline_diagnostics,
                precision=args.pipeline_precision,
                batch_size=args.classifier_batch_size,
            )
        )
        stats = classify_file(
            input_path=args.input_path,
            output_path=output_path,
            args=args,
            pipeline=pipeline,
        )
    except Exception:
        log.exception("Classification failed")
        return 1

    log.info(
        (
            "Done. total_lines=%d classified=%d passthrough_non_articles=%d "
            "skipped_missing_id=%d skipped_missing_text=%d invalid_json=%d"
        ),
        stats["total_lines"],
        stats["classified_records"],
        stats["passthrough_non_articles"],
        stats["skipped_missing_id"],
        stats["skipped_missing_text"],
        stats["invalid_json"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
