#!/usr/bin/env python3
"""
Sample and classify articles as ad/non-ad from an S3 .jsonl.gz input.

Workflow:
1. Load existing classified records from *_classified.jsonl.gz (if present)
   and collect already processed IDs.
2. Repeatedly sample random batches per language and classify them.
3. Keep classifying until reaching target non-ad count per language, or no
   unprocessed candidates remain.
4. Optionally classify all remaining unprocessed records to estimate retained
   article count and retained character volume after ad filtering.
5. Save results back to the same S3 folder with `_classified` suffix.

All classified records (ads + non-ads) are stored with original fields plus
an additional field (default: `ad_class`) holding `ad` or `non-ad`.


python lib/sample_classify_ads_s3.py \
  --input-s3 s3://115-canonical-processed-final/langident/langident-lid-ensemble_multilingual_v2-0-2__AGGREGATED_filtered.jsonl.gz \
  --compiler-s3-prefix s3://142-processed-data-final/lingproc/lingproc-pos-spacy_v3.6.0-multilingual_v1-0-3/ \
  --batch-size-per-language 100 \
  --target-non-ad-per-language 1000 \
  --download-local ./data/all/sample_classified.jsonl.gz

"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from smart_open import open as smart_open  # type: ignore
from impresso_cookbook import (  # type: ignore
    get_s3_client,
    get_transport_params,
    parse_s3_path,
    s3_file_exists,
    setup_logging,
)

log = logging.getLogger(__name__)

AD_TRUE_VALUES = {
    "ad",
    "ads",
    "advertisement",
    "1",
    "true",
    "t",
    "yes",
    "y",
}
AD_FALSE_VALUES = {
    "non-ad",
    "nonad",
    "non_ad",
    "0",
    "false",
    "f",
    "no",
    "n",
}

DEFAULT_AD_FIELD = "ad_class"
DEFAULT_AD_VALUE = "ad"
DEFAULT_NON_AD_VALUE = "non-ad"
DEFAULT_TEXT_FIELD = "ft"


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
            "Randomly sample and classify ads/non-ads from an S3 .jsonl.gz file, "
            "persisting incremental results in *_classified.jsonl.gz."
        )
    )
    parser.add_argument(
        "--input-s3",
        required=True,
        help="Input S3 path to .jsonl.gz (example: s3://bucket/path/file.jsonl.gz).",
    )
    parser.add_argument(
        "--output-s3",
        default=None,
        help="Optional output S3 path. Default is input name + '_classified'.",
    )
    parser.add_argument(
        "--id-field",
        default="id",
        help="Primary ID field used to avoid reclassification (default: id).",
    )
    parser.add_argument(
        "--language-field",
        default="lg",
        help="Language field name (default: lg).",
    )
    parser.add_argument(
        "--type-field",
        default="tp",
        help="Optional record type field for filtering (default: tp).",
    )
    parser.add_argument(
        "--type-value",
        default="article",
        help="Required value of --type-field (default: article).",
    )
    parser.add_argument(
        "--length-field",
        default="len",
        help="Length field used for retained-character estimation (default: len).",
    )
    parser.add_argument(
        "--text-field",
        default=DEFAULT_TEXT_FIELD,
        help="Text field used by classifier (default: ft).",
    )
    parser.add_argument(
        "--compiler-s3-prefix",
        default=None,
        help=(
            "S3 prefix with source article JSONL.bz2 files for s3 compiler text "
            "retrieval (required when input records do not contain --text-field)."
        ),
    )
    parser.add_argument(
        "--compiler-match-field",
        default="id",
        help="Field name to match in compiler source records (default: id).",
    )
    parser.add_argument(
        "--class-field",
        default=DEFAULT_AD_FIELD,
        help=f"Output class field name (default: {DEFAULT_AD_FIELD}).",
    )
    parser.add_argument(
        "--ad-value",
        default=DEFAULT_AD_VALUE,
        help=f"Label value for ads (default: {DEFAULT_AD_VALUE}).",
    )
    parser.add_argument(
        "--non-ad-value",
        dest="non_ad_value",
        default=DEFAULT_NON_AD_VALUE,
        help=f"Label value for non-ads (default: {DEFAULT_NON_AD_VALUE}).",
    )
    parser.add_argument(
        "--languages",
        nargs="*",
        default=None,
        help=(
            "Optional language whitelist (space-separated). If omitted, "
            "languages are discovered from input."
        ),
    )
    parser.add_argument(
        "--batch-size-per-language",
        type=int,
        default=100,
        help="Sampling batch size per language for each round (default: 100).",
    )
    parser.add_argument(
        "--target-non-ad-per-language",
        type=int,
        default=1000,
        help="Target number of non-ads per language (default: 1000).",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=None,
        help="Optional hard cap for sampling rounds.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=42,
        help="Random seed for reproducible batch sampling (default: 42).",
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
        "--classify-remaining",
        action="store_true",
        help=(
            "After sampling target set, classify all remaining unprocessed records "
            "and log retention estimate."
        ),
    )
    parser.add_argument(
        "--remaining-batch-size",
        type=int,
        default=500,
        help="Batch size for --classify-remaining (default: 500).",
    )
    parser.add_argument(
        "--download-local",
        default=None,
        help="Optional local .jsonl.gz destination containing only non-ad records.",
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
        parser.error("--input-s3 must start with s3://")
    if args.output_s3 and not args.output_s3.startswith("s3://"):
        parser.error("--output-s3 must start with s3://")
    if args.batch_size_per_language <= 0:
        parser.error("--batch-size-per-language must be > 0")
    if args.target_non_ad_per_language <= 0:
        parser.error("--target-non-ad-per-language must be > 0")
    if args.classifier_batch_size <= 0:
        parser.error("--classifier-batch-size must be > 0")
    if args.remaining_batch_size <= 0:
        parser.error("--remaining-batch-size must be > 0")
    if args.compiler_s3_prefix and not args.compiler_s3_prefix.startswith("s3://"):
        parser.error("--compiler-s3-prefix must start with s3://")

    return args


def derive_output_path(input_s3: str) -> str:
    """Derive output path from input by appending `_classified` to file stem."""
    if input_s3.endswith(".jsonl.gz"):
        return f"{input_s3[:-9]}_classified.jsonl.gz"
    return f"{input_s3}_classified"


def s3_uri_exists(path: str) -> bool:
    """Check if an S3 or local file exists."""
    if path.startswith("s3://"):
        s3_client = get_s3_client()
        bucket, key = parse_s3_path(path)
        return s3_file_exists(s3_client, bucket, key)
    return os.path.exists(path)


def as_str(value: Any) -> str:
    """Convert value to a normalized string."""
    if value is None:
        return ""
    return str(value).strip()


def to_int(value: Any) -> Optional[int]:
    """Convert value to int when possible."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_is_ad(value: Any) -> Optional[bool]:
    """Parse ad flag from heterogeneous classifier outputs."""
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        if value == 0:
            return False
        if value == 1:
            return True

    text = as_str(value).lower()
    if not text:
        return None
    if text in AD_TRUE_VALUES:
        return True
    if text in AD_FALSE_VALUES:
        return False

    if text.startswith("ad") or "advert" in text:
        return True
    if text.startswith("non"):
        return False

    return None


def record_matches_type(record: Dict[str, Any], type_field: str, type_value: str) -> bool:
    """Filter records by a type field/value pair."""
    if not type_field:
        return True
    return as_str(record.get(type_field)) == as_str(type_value)


def stable_record_id(record: Dict[str, Any], id_field: str, class_field: str) -> str:
    """Build stable record ID for deduplication across reruns."""
    for key in (id_field, "id", "c_id"):
        value = as_str(record.get(key))
        if value:
            return value

    base_record = dict(record)
    base_record.pop(class_field, None)
    canonical = json.dumps(base_record, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


def iter_input_records(
    input_s3: str,
    type_field: str,
    type_value: str,
) -> Iterable[Tuple[int, Dict[str, Any]]]:
    """Yield filtered JSON records from input."""
    with smart_open(
        input_s3,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(input_s3),
    ) as src:
        for line_no, raw_line in enumerate(src, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                log.warning("Skipping invalid JSON in input at line %d", line_no)
                continue

            if not record_matches_type(record, type_field, type_value):
                continue

            yield line_no, record


def preload_existing_classified(
    output_s3: str,
    tmp_writer: Any,
    id_field: str,
    language_field: str,
    class_field: str,
    ad_value: str,
    non_ad_value: str,
) -> Tuple[Set[str], Dict[str, int], int]:
    """
    Load existing classified file and copy it to tmp writer.

    Returns:
        processed_ids, non_ad_counts_by_language, existing_record_count
    """
    processed_ids: Set[str] = set()
    non_ad_counts: Dict[str, int] = defaultdict(int)
    existing_records = 0

    if not s3_uri_exists(output_s3):
        return processed_ids, non_ad_counts, existing_records

    with smart_open(
        output_s3,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(output_s3),
    ) as src:
        for line_no, raw_line in enumerate(src, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                log.warning("Skipping invalid JSON in existing output at line %d", line_no)
                continue

            record_id = stable_record_id(record, id_field=id_field, class_field=class_field)
            processed_ids.add(record_id)
            tmp_writer.write(json.dumps(record, ensure_ascii=False) + "\n")
            existing_records += 1

            class_value = as_str(record.get(class_field)).lower()
            parsed_flag = parse_is_ad(class_value)
            if parsed_flag is None:
                if class_value == as_str(non_ad_value).lower():
                    parsed_flag = False
                elif class_value == as_str(ad_value).lower():
                    parsed_flag = True

            if parsed_flag is False:
                lang = as_str(record.get(language_field))
                if lang:
                    non_ad_counts[lang] += 1

    return processed_ids, non_ad_counts, existing_records


def discover_languages(
    input_s3: str,
    language_field: str,
    type_field: str,
    type_value: str,
) -> Set[str]:
    """Discover language values from the input stream."""
    languages: Set[str] = set()
    for _, record in iter_input_records(input_s3, type_field, type_value):
        language = as_str(record.get(language_field))
        if language:
            languages.add(language)
    return languages


def sample_batch_per_language(
    input_s3: str,
    id_field: str,
    class_field: str,
    language_field: str,
    type_field: str,
    type_value: str,
    needed_languages: Sequence[str],
    processed_ids: Set[str],
    batch_size_per_language: int,
    random_seed: int,
) -> Dict[str, List[Tuple[str, Dict[str, Any]]]]:
    """
    Reservoir-sample up to N unprocessed records per requested language.

    This performs a full pass over input for one sampling round.
    """
    rng = random.Random(random_seed)
    needed = set(needed_languages)

    reservoirs: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {
        language: [] for language in needed
    }
    seen_counts: Dict[str, int] = defaultdict(int)
    selected_ids: Set[str] = set()

    for _, record in iter_input_records(input_s3, type_field, type_value):
        language = as_str(record.get(language_field))
        if language not in needed:
            continue

        record_id = stable_record_id(record, id_field=id_field, class_field=class_field)
        if record_id in processed_ids or record_id in selected_ids:
            continue

        seen_counts[language] += 1
        reservoir = reservoirs[language]

        if len(reservoir) < batch_size_per_language:
            reservoir.append((record_id, record))
            selected_ids.add(record_id)
            continue

        replacement_index = rng.randint(1, seen_counts[language])
        if replacement_index <= batch_size_per_language:
            idx = replacement_index - 1
            old_id, _ = reservoir[idx]
            selected_ids.discard(old_id)
            reservoir[idx] = (record_id, record)
            selected_ids.add(record_id)

    return reservoirs


def _extract_is_ad_from_output(output: Any) -> Optional[bool]:
    """Try hard to interpret model output as ad/non-ad flag."""
    if isinstance(output, list):
        if not output:
            return None
        return _extract_is_ad_from_output(output[0])

    if not isinstance(output, dict):
        return parse_is_ad(output)

    for key in (
        "is_ad",
        "ad",
        "ad_flag",
        "prediction_is_ad",
        "advertisement",
        "type",
        "label",
        "prediction",
        "predicted_label",
        "class",
    ):
        if key in output:
            parsed = parse_is_ad(output.get(key))
            if parsed is not None:
                return parsed

    promotion_final = output.get("promotion_prob_final")
    threshold_used = output.get("threshold_used")
    if promotion_final is not None:
        try:
            score = float(promotion_final)
            threshold = float(threshold_used) if threshold_used is not None else 0.5
            return score >= threshold
        except (TypeError, ValueError):
            pass

    for key in ("promotion_prob", "score", "probability"):
        if key in output:
            try:
                return float(output[key]) >= 0.5
            except (TypeError, ValueError):
                continue

    return None


def _build_ad_classifier(config: ClassifierConfig) -> Any:
    """Instantiate impresso internal AdClassifierPipeline."""
    try:
        from impresso_pipelines.adclassifier import (  # type: ignore
            AdClassifierPipeline,
        )
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

    # Fallback to one-by-one if batch output shape is incompatible.
    outputs: List[Any] = []
    for text in texts:
        single = _call(text)
        if isinstance(single, list):
            outputs.append(single[0] if single else {})
        else:
            outputs.append(single)
    return outputs


def _jq_accessor(field_name: str) -> str:
    """Build a robust JQ field accessor."""
    return f".[{json.dumps(field_name)}]"


def _build_compiler_transform_expr(
    text_field: str,
    length_field: str,
    match_field: str,
) -> str:
    """Build minimal transform expression for compiler output."""
    return (
        "{"
        + f"\"id\": {_jq_accessor(match_field)}, "
        + f"{json.dumps(text_field)}: {_jq_accessor(text_field)}, "
        + f"{json.dumps(length_field)}: {_jq_accessor(length_field)}"
        + "}"
    )


def _fetch_text_map_with_s3_compiler(
    missing_ids: Sequence[str],
    args: argparse.Namespace,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch text-bearing records for IDs via s3 compiler.

    Returns:
        Mapping id -> compiled record containing at least id/text/len fields.
    """
    if not missing_ids:
        return {}
    if not args.compiler_s3_prefix:
        raise RuntimeError(
            f"Missing '{args.text_field}' in input records but no --compiler-s3-prefix provided."
        )

    try:
        from impresso_cookbook.s3_compiler import S3CompilerProcessor  # type: ignore
    except Exception as exc:
        raise RuntimeError("Could not import impresso_cookbook.s3_compiler") from exc

    unique_ids = sorted(set(missing_ids))
    fd_ids, ids_path = tempfile.mkstemp(prefix="ad_ids_", suffix=".jsonl")
    os.close(fd_ids)
    fd_out, out_path = tempfile.mkstemp(prefix="ad_compiled_", suffix=".jsonl")
    os.close(fd_out)

    try:
        with smart_open(ids_path, "wt", encoding="utf-8") as dst:
            for record_id in unique_ids:
                dst.write(json.dumps({"id": record_id}, ensure_ascii=False) + "\n")

        transform_expr = _build_compiler_transform_expr(
            text_field=args.text_field,
            length_field=args.length_field,
            match_field=args.compiler_match_field,
        )

        processor = S3CompilerProcessor(
            input_file=ids_path,
            s3_prefix=args.compiler_s3_prefix,
            output_file=out_path,
            id_field="id",
            transform_expr=transform_expr,
            match_field=args.compiler_match_field,
            log_level=args.log_level,
            log_file=None,
        )

        try:
            processor.run()
        except SystemExit as exc:
            raise RuntimeError(f"s3 compiler failed with exit code {exc.code}") from exc

        compiled_map: Dict[str, Dict[str, Any]] = {}
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
                if record_id:
                    compiled_map[record_id] = record
        return compiled_map
    finally:
        if os.path.exists(ids_path):
            os.remove(ids_path)
        if os.path.exists(out_path):
            os.remove(out_path)


def enrich_records_with_text(
    records: Sequence[Tuple[str, Dict[str, Any]]],
    args: argparse.Namespace,
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Ensure text exists in each record by retrieving missing text through s3 compiler.
    """
    missing_ids = [
        record_id
        for record_id, record in records
        if not as_str(record.get(args.text_field))
    ]
    if not missing_ids:
        return [(record_id, dict(record)) for record_id, record in records]

    log.info(
        "Retrieving missing '%s' via s3 compiler for %d records",
        args.text_field,
        len(set(missing_ids)),
    )
    compiled_map = _fetch_text_map_with_s3_compiler(missing_ids, args)

    enriched: List[Tuple[str, Dict[str, Any]]] = []
    unresolved = 0
    for record_id, record in records:
        updated = dict(record)
        if not as_str(updated.get(args.text_field)):
            compiled = compiled_map.get(record_id)
            if compiled is not None:
                compiled_text = as_str(compiled.get(args.text_field))
                if compiled_text:
                    updated[args.text_field] = compiled_text
                if to_int(updated.get(args.length_field)) is None:
                    compiled_len = to_int(compiled.get(args.length_field))
                    if compiled_len is not None:
                        updated[args.length_field] = compiled_len

        if not as_str(updated.get(args.text_field)):
            unresolved += 1
        enriched.append((record_id, updated))

    if unresolved:
        raise RuntimeError(
            f"Could not retrieve '{args.text_field}' for {unresolved} records via s3 compiler."
        )

    return enriched


def classify_records(
    records: Sequence[Tuple[str, Dict[str, Any]]],
    text_field: str,
    config: ClassifierConfig,
    pipeline: Any,
) -> Dict[str, bool]:
    """Classify records with impresso internal AdClassifierPipeline."""
    decisions: Dict[str, bool] = {}

    for start in range(0, len(records), config.batch_size):
        chunk = records[start : start + config.batch_size]
        texts = [as_str(record.get(text_field)) for _, record in chunk]
        outputs = _pipeline_predict_batch(
            pipeline=pipeline,
            texts=texts,
            precision=config.precision,
        )

        if len(outputs) != len(chunk):
            raise RuntimeError(
                f"Classifier returned {len(outputs)} results for {len(chunk)} inputs"
            )

        for (record_id, record), output in zip(chunk, outputs):
            parsed = _extract_is_ad_from_output(output)
            if parsed is None:
                raise RuntimeError(
                    "Could not parse classifier output for "
                    f"record id={record_id}"
                )
            decisions[record_id] = parsed

    return decisions


def record_length(record: Dict[str, Any], length_field: str, text_field: str) -> int:
    """Get record length from explicit length field or fallback text length."""
    length = to_int(record.get(length_field))
    if length is not None and length >= 0:
        return length
    return len(as_str(record.get(text_field)))


def append_classified_records(
    records: Sequence[Tuple[str, Dict[str, Any]]],
    decisions: Dict[str, bool],
    tmp_writer: Any,
    processed_ids: Set[str],
    non_ad_counts: Dict[str, int],
    language_field: str,
    class_field: str,
    ad_value: str,
    non_ad_value: str,
    length_field: str,
    text_field: str,
) -> Tuple[int, int, int]:
    """
    Append classified records to writer and update in-memory state.

    Returns:
        appended_records, newly_added_non_ads, newly_added_non_ad_chars
    """
    appended = 0
    non_ads = 0
    non_ad_chars = 0

    for record_id, record in records:
        if record_id in processed_ids:
            continue
        if record_id not in decisions:
            continue

        is_ad = decisions[record_id]
        classified_record = dict(record)
        classified_record[class_field] = ad_value if is_ad else non_ad_value

        tmp_writer.write(json.dumps(classified_record, ensure_ascii=False) + "\n")
        processed_ids.add(record_id)
        appended += 1

        if not is_ad:
            language = as_str(record.get(language_field))
            if language:
                non_ad_counts[language] += 1
            non_ads += 1
            non_ad_chars += record_length(record, length_field=length_field, text_field=text_field)

    return appended, non_ads, non_ad_chars


def run_sampling_phase(
    input_s3: str,
    target_languages: Sequence[str],
    args: argparse.Namespace,
    tmp_writer: Any,
    processed_ids: Set[str],
    non_ad_counts: Dict[str, int],
    classifier: Any,
) -> Tuple[int, int]:
    """Run iterative per-language random sampling until non-ad targets are met."""
    rounds_completed = 0
    total_appended = 0

    while True:
        needed_languages = [
            language
            for language in target_languages
            if non_ad_counts.get(language, 0) < args.target_non_ad_per_language
        ]

        if not needed_languages:
            break

        if args.max_rounds is not None and rounds_completed >= args.max_rounds:
            log.info("Reached max rounds (%d), stopping sampling phase", args.max_rounds)
            break

        batches = sample_batch_per_language(
            input_s3=input_s3,
            id_field=args.id_field,
            class_field=args.class_field,
            language_field=args.language_field,
            type_field=args.type_field,
            type_value=args.type_value,
            needed_languages=needed_languages,
            processed_ids=processed_ids,
            batch_size_per_language=args.batch_size_per_language,
            random_seed=args.random_seed + rounds_completed,
        )

        sampled_records: List[Tuple[str, Dict[str, Any]]] = []
        for language in needed_languages:
            sampled_records.extend(batches.get(language, []))

        if not sampled_records:
            log.info(
                "No more unprocessed candidates for needed languages: %s",
                ", ".join(sorted(needed_languages)),
            )
            break

        sampled_records = enrich_records_with_text(sampled_records, args)
        decisions = classify_records(
            records=sampled_records,
            text_field=args.text_field,
            config=ClassifierConfig(
                diagnostics=args.pipeline_diagnostics,
                precision=args.pipeline_precision,
                batch_size=args.classifier_batch_size,
            ),
            pipeline=classifier,
        )

        appended, non_ads_added, _ = append_classified_records(
            records=sampled_records,
            decisions=decisions,
            tmp_writer=tmp_writer,
            processed_ids=processed_ids,
            non_ad_counts=non_ad_counts,
            language_field=args.language_field,
            class_field=args.class_field,
            ad_value=args.ad_value,
            non_ad_value=args.non_ad_value,
            length_field=args.length_field,
            text_field=args.text_field,
        )

        total_appended += appended
        rounds_completed += 1

        progress = ", ".join(
            f"{lang}:{non_ad_counts.get(lang, 0)}/{args.target_non_ad_per_language}"
            for lang in target_languages
        )
        log.info(
            "Round %d -> sampled=%d appended=%d non_ads_added=%d progress=[%s]",
            rounds_completed,
            len(sampled_records),
            appended,
            non_ads_added,
            progress,
        )

    return total_appended, rounds_completed


def iter_unprocessed_records(
    input_s3: str,
    id_field: str,
    class_field: str,
    processed_ids: Set[str],
    type_field: str,
    type_value: str,
) -> Iterable[Tuple[str, Dict[str, Any]]]:
    """Yield records that have not yet been classified."""
    for _, record in iter_input_records(input_s3, type_field, type_value):
        record_id = stable_record_id(record, id_field=id_field, class_field=class_field)
        if record_id in processed_ids:
            continue
        yield record_id, record


def run_remaining_estimation(
    input_s3: str,
    args: argparse.Namespace,
    tmp_writer: Any,
    processed_ids: Set[str],
    non_ad_counts: Dict[str, int],
    classifier: Any,
) -> Tuple[int, int, int]:
    """
    Classify all remaining unprocessed records and compute retention estimate.

    Returns:
        (newly_classified_records, remaining_non_ad_count, remaining_non_ad_chars)
    """
    total_appended = 0
    total_remaining_non_ads = 0
    total_remaining_non_ad_chars = 0
    pending: List[Tuple[str, Dict[str, Any]]] = []

    def flush_batch(batch: List[Tuple[str, Dict[str, Any]]]) -> Tuple[int, int, int]:
        if not batch:
            return 0, 0, 0

        enriched_batch = enrich_records_with_text(batch, args)
        decisions = classify_records(
            records=enriched_batch,
            text_field=args.text_field,
            config=ClassifierConfig(
                diagnostics=args.pipeline_diagnostics,
                precision=args.pipeline_precision,
                batch_size=args.classifier_batch_size,
            ),
            pipeline=classifier,
        )

        return append_classified_records(
            records=enriched_batch,
            decisions=decisions,
            tmp_writer=tmp_writer,
            processed_ids=processed_ids,
            non_ad_counts=non_ad_counts,
            language_field=args.language_field,
            class_field=args.class_field,
            ad_value=args.ad_value,
            non_ad_value=args.non_ad_value,
            length_field=args.length_field,
            text_field=args.text_field,
        )

    for record_id, record in iter_unprocessed_records(
        input_s3=input_s3,
        id_field=args.id_field,
        class_field=args.class_field,
        processed_ids=processed_ids,
        type_field=args.type_field,
        type_value=args.type_value,
    ):
        pending.append((record_id, record))
        if len(pending) >= args.remaining_batch_size:
            appended, non_ads, non_ad_chars = flush_batch(pending)
            total_appended += appended
            total_remaining_non_ads += non_ads
            total_remaining_non_ad_chars += non_ad_chars
            pending = []

    if pending:
        appended, non_ads, non_ad_chars = flush_batch(pending)
        total_appended += appended
        total_remaining_non_ads += non_ads
        total_remaining_non_ad_chars += non_ad_chars

    return total_appended, total_remaining_non_ads, total_remaining_non_ad_chars


def copy_jsonl_lines(source_path: str, destination_path: str) -> None:
    """Copy JSONL(.gz) content between local/S3 paths using smart_open."""
    with smart_open(
        source_path,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(source_path),
    ) as src, smart_open(
        destination_path,
        "wt",
        encoding="utf-8",
        transport_params=get_transport_params(destination_path),
    ) as dst:
        for line in src:
            dst.write(line)


def copy_non_ad_lines(
    source_path: str,
    destination_path: str,
    class_field: str,
    ad_value: str,
    non_ad_value: str,
) -> int:
    """Copy only non-ad records to destination and return copied count."""
    copied = 0
    non_ad_label = as_str(non_ad_value).lower()
    ad_label = as_str(ad_value).lower()

    with smart_open(
        source_path,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(source_path),
    ) as src, smart_open(
        destination_path,
        "wt",
        encoding="utf-8",
        transport_params=get_transport_params(destination_path),
    ) as dst:
        for line_no, raw_line in enumerate(src, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                log.warning("Skipping invalid JSON while exporting local file at line %d", line_no)
                continue

            class_value = as_str(record.get(class_field)).lower()
            parsed_flag = parse_is_ad(class_value)
            if parsed_flag is None:
                if class_value == non_ad_label:
                    parsed_flag = False
                elif class_value == ad_label:
                    parsed_flag = True

            if parsed_flag is False:
                dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                copied += 1

    return copied


def _ensure_parent_dir(path: str) -> None:
    """Create local parent directory if needed."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    setup_logging(args.log_level, args.log_file, logger=log)

    input_s3 = args.input_s3
    output_s3 = args.output_s3 or derive_output_path(input_s3)

    log.info("Input: %s", input_s3)
    log.info("Output: %s", output_s3)

    classifier = _build_ad_classifier(
        ClassifierConfig(
            diagnostics=args.pipeline_diagnostics,
            precision=args.pipeline_precision,
            batch_size=args.classifier_batch_size,
        )
    )

    fd, tmp_path = tempfile.mkstemp(prefix="ad_classifier_state_", suffix=".jsonl.gz")
    os.close(fd)

    try:
        with smart_open(tmp_path, "wt", encoding="utf-8") as tmp_writer:
            processed_ids, non_ad_counts, existing_records = preload_existing_classified(
                output_s3=output_s3,
                tmp_writer=tmp_writer,
                id_field=args.id_field,
                language_field=args.language_field,
                class_field=args.class_field,
                ad_value=args.ad_value,
                non_ad_value=args.non_ad_value,
            )

            log.info(
                "Loaded existing classified state: records=%d processed_ids=%d",
                existing_records,
                len(processed_ids),
            )

            if args.languages:
                target_languages = sorted({as_str(language) for language in args.languages if as_str(language)})
            else:
                discovered_languages = discover_languages(
                    input_s3=input_s3,
                    language_field=args.language_field,
                    type_field=args.type_field,
                    type_value=args.type_value,
                )
                target_languages = sorted(discovered_languages)

            if not target_languages:
                log.warning("No languages found to process. Writing unchanged output.")
            else:
                log.info("Target languages: %s", ", ".join(target_languages))

            sampled_appended = 0
            rounds_completed = 0
            if target_languages:
                sampled_appended, rounds_completed = run_sampling_phase(
                    input_s3=input_s3,
                    target_languages=target_languages,
                    args=args,
                    tmp_writer=tmp_writer,
                    processed_ids=processed_ids,
                    non_ad_counts=non_ad_counts,
                    classifier=classifier,
                )

                progress = ", ".join(
                    f"{language}:{non_ad_counts.get(language, 0)}"
                    for language in target_languages
                )
                log.info(
                    "Sampling phase done: rounds=%d newly_appended=%d non_ad_counts=[%s]",
                    rounds_completed,
                    sampled_appended,
                    progress,
                )

            remaining_appended = 0
            remaining_non_ads = 0
            remaining_non_ad_chars = 0
            if args.classify_remaining:
                (
                    remaining_appended,
                    remaining_non_ads,
                    remaining_non_ad_chars,
                ) = run_remaining_estimation(
                    input_s3=input_s3,
                    args=args,
                    tmp_writer=tmp_writer,
                    processed_ids=processed_ids,
                    non_ad_counts=non_ad_counts,
                    classifier=classifier,
                )

                log.info(
                    "Remaining estimation: newly_classified=%d retained_non_ads=%d retained_chars=%d",
                    remaining_appended,
                    remaining_non_ads,
                    remaining_non_ad_chars,
                )

        copy_jsonl_lines(tmp_path, output_s3)
        log.info("Wrote classified output: %s", output_s3)

        if args.download_local:
            _ensure_parent_dir(args.download_local)
            non_ad_local_count = copy_non_ad_lines(
                source_path=tmp_path,
                destination_path=args.download_local,
                class_field=args.class_field,
                ad_value=args.ad_value,
                non_ad_value=args.non_ad_value,
            )
            log.info(
                "Wrote local non-ad file: %s (records=%d)",
                args.download_local,
                non_ad_local_count,
            )

        log.info(
            "Done. existing=%d sampled_new=%d remaining_new=%d total_processed_ids=%d",
            existing_records,
            sampled_appended,
            remaining_appended,
            len(processed_ids),
        )

        return 0

    except Exception:
        log.exception("Processing failed")
        return 1
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


if __name__ == "__main__":
    sys.exit(main())
