#!/usr/bin/env python3
"""
Generate DeepSeek keywords for per-language JSONL files from local or S3 folders.

Input folder is expected to contain files like:
    de.jsonl, en.jsonl, ..., languages_summary.json

Output folder receives files like:
    keywords_de.jsonl, keywords_en.jsonl, ...
and one token-usage summary file:
    deepseek_summary.json

Run example:
DEEPSEEK_API_KEY=... python lib/generate_keywords_deepseek_s3.py \
  --input-prefix ./data/per_language \
  --output-prefix ./data/per_language_keywords \
  --providers-title-path ./data/providers-title.json


S3 example:
DEEPSEEK_API_KEY=... python lib/generate_keywords_deepseek_s3.py \
  --input-prefix s3://your-bucket/per_language \
  --output-prefix s3://your-bucket/per_language_keywords \
  --providers-title-path ./data/providers-title.json


"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from openai import OpenAI  # type: ignore
from smart_open import open as smart_open  # type: ignore
from impresso_cookbook import (  # type: ignore
    get_s3_client,
    get_transport_params,
    parse_s3_path,
    setup_logging,
)

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_MODEL = "deepseek-chat"
DEFAULT_SUMMARY_NAME = "languages_summary.json"
DEFAULT_DEEPSEEK_SUMMARY_NAME = "deepseek_summary.json"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate DeepSeek keywords for all per-language JSONL files in a local/S3 folder."
        )
    )
    parser.add_argument(
        "--input-prefix",
        required=True,
        help="Input folder path (local dir or s3://bucket/prefix).",
    )
    parser.add_argument(
        "--output-prefix",
        required=True,
        help="Output folder path (local dir or s3://bucket/prefix).",
    )
    parser.add_argument(
        "--providers-title-path",
        default="./data/providers-title.json",
        help="Path to provider/title/country mapping JSON (local or s3://...).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="DeepSeek API key. If omitted, reads DEEPSEEK_API_KEY or OPENAI_API_KEY.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"DeepSeek base URL (default: {DEFAULT_BASE_URL}).",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Model name (default: {DEFAULT_MODEL}).",
    )
    parser.add_argument(
        "--summary-name",
        default=DEFAULT_SUMMARY_NAME,
        help=f"Expected summary filename in input folder (default: {DEFAULT_SUMMARY_NAME}).",
    )
    parser.add_argument(
        "--deepseek-summary-name",
        default=DEFAULT_DEEPSEEK_SUMMARY_NAME,
        help=(
            "Output summary filename with token usage by language "
            f"(default: {DEFAULT_DEEPSEEK_SUMMARY_NAME})."
        ),
    )
    parser.add_argument(
        "--id-field",
        default="id",
        help="ID field name (default: id).",
    )
    parser.add_argument(
        "--language-field",
        default="lg",
        help="Language field name (default: lg).",
    )
    parser.add_argument(
        "--year-field",
        default="year",
        help="Year field name (default: year).",
    )
    parser.add_argument(
        "--title-field",
        default="title",
        help="Title field name (default: title).",
    )
    parser.add_argument(
        "--text-field",
        default="ft",
        help="Article text field name (default: ft).",
    )
    parser.add_argument(
        "--keywords-field",
        default="keywords",
        help="Output keywords field name inserted before text field (default: keywords).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=50,
        help="Log progress every N written records per file (default: 50).",
    )
    parser.add_argument(
        "--max-records-per-file",
        type=int,
        default=None,
        help="Optional limit for debugging; process only first N records per file.",
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

    if args.progress_every <= 0:
        parser.error("--progress-every must be > 0")
    if args.max_records_per_file is not None and args.max_records_per_file <= 0:
        parser.error("--max-records-per-file must be > 0")
    if not args.base_url:
        parser.error("--base-url must not be empty")
    if not args.model:
        parser.error("--model must not be empty")

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


def join_prefix(prefix: str, filename: str) -> str:
    """Join filename under local or S3 prefix."""
    if prefix.startswith("s3://"):
        return f"{prefix.rstrip('/')}/{filename}"
    return os.path.join(prefix, filename)


def ensure_local_dir(prefix: str) -> None:
    """Create local output directory when needed."""
    if prefix.startswith("s3://"):
        return
    os.makedirs(prefix, exist_ok=True)


def _is_jsonl_name(name: str) -> bool:
    """Check if filename looks like JSONL."""
    lowered = name.lower()
    return lowered.endswith(".jsonl") or lowered.endswith(".jsonl.gz")


def infer_language_from_filename(path: str) -> str:
    """Infer language code from input filename stem."""
    base = os.path.basename(path)
    lowered = base.lower()
    if lowered.endswith(".jsonl.gz"):
        stem = base[:-9]
    elif lowered.endswith(".jsonl"):
        stem = base[:-6]
    else:
        stem = os.path.splitext(base)[0]
    if stem.startswith("keywords_"):
        stem = stem[len("keywords_") :]
    return stem


def list_input_jsonl_files(input_prefix: str, summary_name: str) -> List[str]:
    """List all input JSONL files from local or S3 folder."""
    files: List[str] = []
    summary_found = False

    if input_prefix.startswith("s3://"):
        bucket, key_prefix = parse_s3_path(input_prefix)
        prefix = key_prefix.rstrip("/")
        if prefix:
            prefix = prefix + "/"

        client = get_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj.get("Key", "")
                if not key or key.endswith("/"):
                    continue
                name = os.path.basename(key)
                if name == summary_name:
                    summary_found = True
                    continue
                if name.startswith("keywords_"):
                    continue
                if _is_jsonl_name(name):
                    files.append(f"s3://{bucket}/{key}")
    else:
        if not os.path.isdir(input_prefix):
            raise ValueError(f"Input prefix is not a directory: {input_prefix}")
        for name in sorted(os.listdir(input_prefix)):
            path = os.path.join(input_prefix, name)
            if not os.path.isfile(path):
                continue
            if name == summary_name:
                summary_found = True
                continue
            if name.startswith("keywords_"):
                continue
            if _is_jsonl_name(name):
                files.append(path)

    files = sorted(files)
    if not files:
        raise ValueError(f"No input JSONL files found under: {input_prefix}")
    if not summary_found:
        log.warning(
            "Did not find %s under input prefix: %s",
            summary_name,
            input_prefix,
        )
    return files


def load_newspaper_country_map(path: str) -> Dict[str, str]:
    """
    Build newspaper -> country mapping from providers-title JSON.

    providers-title JSON format:
    {
      "BL": {"country": "UK", "titles": ["BQGA", ...]},
      ...
    }
    """
    with smart_open(
        path,
        "rt",
        encoding="utf-8",
        transport_params=get_transport_params(path),
    ) as src:
        data = json.load(src)

    if not isinstance(data, dict):
        raise ValueError("providers-title JSON must be a dictionary at top level")

    newspaper_to_country: Dict[str, str] = {}
    duplicate_newspapers: List[str] = []
    for _provider, payload in data.items():
        if not isinstance(payload, dict):
            continue
        country = as_str(payload.get("country"))
        titles = payload.get("titles")
        if not isinstance(titles, list):
            continue

        for item in titles:
            newspaper = as_str(item)
            if not newspaper:
                continue
            prev_country = newspaper_to_country.get(newspaper)
            if prev_country is not None and prev_country != country:
                duplicate_newspapers.append(newspaper)
            newspaper_to_country[newspaper] = country

    if duplicate_newspapers:
        log.warning(
            "Found %d newspapers mapped to multiple countries; using last value",
            len(set(duplicate_newspapers)),
        )

    return newspaper_to_country


def _build_messages(
    language: str,
    year: str,
    newspaper: str,
    country: str,
    title: str,
    article: str,
) -> List[Dict[str, str]]:
    """
    Build DeepSeek prompt payload.

    This mirrors the notebook prompt and message structure.
    """
    system_prompt = """
    You are an archivist for historical newspaper articles who indexes historical newspaper articles 
    with conceptual keyphrases in English. Given a JSON object containing metadata (language, year of publication, 
    newspaper name, country of publication, the article's title) and a historical 
    newspaper article, please index with an adequate number (between 3 and 5) of most relevant keywords 
    in English in JSON format, like so {"english_keywords": ["KEYWORD1",...]}. 
    Do not create keywords consisting of names for persons, locations, or events.
    """

    user_prompt = f"""{{
    Language: {language}, 
    Year: {year}, 
    Newspaper: {newspaper},
    Country: {country},
    Title: {title},
    Article: {article}
    }}
    """

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def get_deepseek_response(
    client: OpenAI,
    model: str,
    language: str,
    year: str,
    newspaper: str,
    country: str,
    title: str,
    article: str,
) -> Any:
    """Call DeepSeek Chat Completions with notebook-equivalent structure."""
    messages = _build_messages(language, year, newspaper, country, title, article)
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        response_format={
            "type": "json_object",
        },
    )
    return response


def extract_token_usage(response: Any) -> Tuple[int, int, int, bool]:
    """
    Extract prompt/completion/total tokens from chat completion response.

    Returns:
        (prompt_tokens, completion_tokens, total_tokens, usage_present)
    """
    usage = getattr(response, "usage", None)
    if usage is None:
        return 0, 0, 0, False

    if isinstance(usage, dict):
        prompt_tokens = to_int(usage.get("prompt_tokens")) or 0
        completion_tokens = to_int(usage.get("completion_tokens")) or 0
        total_tokens = to_int(usage.get("total_tokens")) or 0
    else:
        prompt_tokens = to_int(getattr(usage, "prompt_tokens", None)) or 0
        completion_tokens = to_int(getattr(usage, "completion_tokens", None)) or 0
        total_tokens = to_int(getattr(usage, "total_tokens", None)) or 0

    if total_tokens <= 0 and (prompt_tokens > 0 or completion_tokens > 0):
        total_tokens = prompt_tokens + completion_tokens

    usage_present = (prompt_tokens > 0) or (completion_tokens > 0) or (total_tokens > 0)
    return prompt_tokens, completion_tokens, total_tokens, usage_present


def parse_keywords_from_response(response: Any) -> List[str]:
    """Parse english_keywords from completion response content JSON."""
    content = as_str(response.choices[0].message.content)
    payload = json.loads(content)
    keywords = payload.get("english_keywords", [])
    if not isinstance(keywords, list):
        return []
    clean: List[str] = []
    for item in keywords:
        value = as_str(item)
        if value:
            clean.append(value)
    return clean


def with_keywords_before_text(
    record: Dict[str, Any],
    keywords: List[str],
    keywords_field: str,
    text_field: str,
) -> Dict[str, Any]:
    """Create a record copy with keywords inserted before text field."""
    updated: Dict[str, Any] = {}
    inserted = False
    for key, value in record.items():
        if key == text_field and not inserted:
            updated[keywords_field] = keywords
            inserted = True
        updated[key] = value
    if not inserted:
        updated[keywords_field] = keywords
    return updated


def iter_jsonl_records(path: str) -> Iterable[Tuple[int, Dict[str, Any]]]:
    """Yield parsed JSON records with line numbers."""
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
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                log.warning("Skipping invalid JSON in %s at line %d", path, line_no)
                continue
            if not isinstance(record, dict):
                log.warning("Skipping non-object JSON in %s at line %d", path, line_no)
                continue
            yield line_no, record


def process_file(
    input_path: str,
    output_path: str,
    args: argparse.Namespace,
    client: OpenAI,
    newspaper_to_country: Dict[str, str],
) -> Dict[str, int]:
    """Generate keywords for one input file and write output JSONL."""
    processed = 0
    failed = 0
    with_keywords = 0
    article_chars = 0
    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0
    usage_missing = 0

    with smart_open(
        output_path,
        "wt",
        encoding="utf-8",
        transport_params=get_transport_params(output_path),
    ) as dst:
        for _line_no, record in iter_jsonl_records(input_path):
            if args.max_records_per_file is not None and processed >= args.max_records_per_file:
                break

            record_id = as_str(record.get(args.id_field))
            language = as_str(record.get(args.language_field))
            year = as_str(record.get(args.year_field))
            title = as_str(record.get(args.title_field))
            article = as_str(record.get(args.text_field))
            article_chars += len(article)

            newspaper = record_id.split("-", 1)[0] if record_id else ""
            country = newspaper_to_country.get(newspaper, "")

            keywords: List[str] = []
            if article:
                try:
                    response = get_deepseek_response(
                        client=client,
                        model=args.model,
                        language=language,
                        year=year,
                        newspaper=newspaper,
                        country=country,
                        title=title,
                        article=article,
                    )
                    in_toks, out_toks, all_toks, usage_present = extract_token_usage(response)
                    prompt_tokens += in_toks
                    completion_tokens += out_toks
                    total_tokens += all_toks
                    if not usage_present:
                        usage_missing += 1
                    keywords = parse_keywords_from_response(response)
                except Exception:
                    failed += 1
                    log.exception("Keyword generation failed for id=%s", record_id)
            else:
                failed += 1
                log.warning("Missing article text field '%s' for id=%s", args.text_field, record_id)

            if keywords:
                with_keywords += 1

            updated = with_keywords_before_text(
                record=record,
                keywords=keywords,
                keywords_field=args.keywords_field,
                text_field=args.text_field,
            )
            dst.write(json.dumps(updated, ensure_ascii=False) + "\n")
            processed += 1

            if processed % args.progress_every == 0:
                log.info(
                    "Progress %s -> %s: processed=%d failed=%d",
                    input_path,
                    output_path,
                    processed,
                    failed,
                )

    return {
        "processed": processed,
        "failed": failed,
        "with_keywords": with_keywords,
        "article_chars": article_chars,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "usage_missing": usage_missing,
    }


def write_deepseek_summary(
    output_prefix: str,
    summary_name: str,
    summary: Dict[str, Any],
) -> str:
    """Write DeepSeek token summary JSON to local or S3 output prefix."""
    summary_path = join_prefix(output_prefix, summary_name)
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

    api_key = args.api_key or os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        log.error("Missing API key. Set --api-key or DEEPSEEK_API_KEY/OPENAI_API_KEY.")
        return 1

    log.info("Input prefix: %s", args.input_prefix)
    log.info("Output prefix: %s", args.output_prefix)
    log.info("Providers title path: %s", args.providers_title_path)
    log.info("Model: %s", args.model)

    ensure_local_dir(args.output_prefix)

    try:
        input_files = list_input_jsonl_files(args.input_prefix, args.summary_name)
        newspaper_to_country = load_newspaper_country_map(args.providers_title_path)
    except Exception:
        log.exception("Initialization failed")
        return 1

    if not newspaper_to_country:
        log.warning("Newspaper->country map is empty; countries in prompts will be blank")

    client = OpenAI(
        api_key=api_key,
        base_url=args.base_url,
    )

    total_processed = 0
    total_failed = 0
    total_with_keywords = 0
    total_article_chars = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    total_usage_missing = 0
    language_summaries: List[Dict[str, Any]] = []

    for input_path in input_files:
        lang = infer_language_from_filename(input_path)
        output_name = f"keywords_{lang}.jsonl"
        output_path = join_prefix(args.output_prefix, output_name)

        log.info("Processing file: %s", input_path)
        log.info("Writing file: %s", output_path)
        try:
            stats = process_file(
                input_path=input_path,
                output_path=output_path,
                args=args,
                client=client,
                newspaper_to_country=newspaper_to_country,
            )
        except Exception:
            log.exception("Failed processing file: %s", input_path)
            return 1

        total_processed += stats["processed"]
        total_failed += stats["failed"]
        total_with_keywords += stats["with_keywords"]
        total_article_chars += stats["article_chars"]
        total_prompt_tokens += stats["prompt_tokens"]
        total_completion_tokens += stats["completion_tokens"]
        total_tokens += stats["total_tokens"]
        total_usage_missing += stats["usage_missing"]

        language_summaries.append(
            {
                "language": lang,
                "records": stats["processed"],
                "failed_records": stats["failed"],
                "records_with_keywords": stats["with_keywords"],
                "article_chars": stats["article_chars"],
                "prompt_tokens": stats["prompt_tokens"],
                "completion_tokens": stats["completion_tokens"],
                "total_tokens": stats["total_tokens"],
                "usage_missing_records": stats["usage_missing"],
                "input_path": input_path,
                "output_path": output_path,
            }
        )

        log.info(
            (
                "Done file: %s processed=%d failed=%d with_keywords=%d "
                "prompt_tokens=%d completion_tokens=%d total_tokens=%d"
            ),
            input_path,
            stats["processed"],
            stats["failed"],
            stats["with_keywords"],
            stats["prompt_tokens"],
            stats["completion_tokens"],
            stats["total_tokens"],
        )

    language_summaries = sorted(language_summaries, key=lambda item: item["language"])
    summary_payload = {
        "input_prefix": args.input_prefix,
        "output_prefix": args.output_prefix,
        "providers_title_path": args.providers_title_path,
        "model": args.model,
        "base_url": args.base_url,
        "keywords_field": args.keywords_field,
        "total_records": total_processed,
        "total_failed_records": total_failed,
        "total_records_with_keywords": total_with_keywords,
        "total_article_chars": total_article_chars,
        "total_prompt_tokens": total_prompt_tokens,
        "total_completion_tokens": total_completion_tokens,
        "total_tokens": total_tokens,
        "total_usage_missing_records": total_usage_missing,
        "languages": [item["language"] for item in language_summaries],
        "languages_summary": language_summaries,
    }

    try:
        summary_path = write_deepseek_summary(
            output_prefix=args.output_prefix,
            summary_name=args.deepseek_summary_name,
            summary=summary_payload,
        )
    except Exception:
        log.exception("Failed writing DeepSeek summary")
        return 1

    log.info(
        (
            "Done all files. input_files=%d processed=%d failed=%d "
            "prompt_tokens=%d completion_tokens=%d total_tokens=%d"
        ),
        len(input_files),
        total_processed,
        total_failed,
        total_prompt_tokens,
        total_completion_tokens,
        total_tokens,
    )
    log.info("DeepSeek summary: %s", summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
