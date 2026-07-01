# Impresso Keyphrase

This repository contains the Impresso keyphrase-generation workflow. It is used
to build curated, language-specific article samples from Impresso processed data,
remove advertisements, generate English conceptual keyphrases for each retained
article with a DeepSeek-compatible chat model, and publish the run outputs to S3.

The pipeline starts from the aggregated language-identification JSONL.GZ output
and produces versioned keyphrase datasets under an Impresso processing run prefix.
It is intended for reproducible batch runs over large S3-hosted corpora, with
local Make stamp files used only for dependency tracking.

## What the Pipeline Does

The Make entrypoint mirrors the script order in `lib/`:

1. `lib/filter_jsonl_gz_s3.py`
   Filters the aggregated input to article records that satisfy OCR-quality and
   text-length thresholds.
2. `lib/sample_classify_ads_s3.py`
   Samples candidates per language, retrieves missing full text from the compiler
   source when needed, classifies records as `ad` or `non-ad`, and exports a
   balanced non-ad sample.
3. `lib/split_by_language_jsonl_s3.py`
   Splits the non-ad sample into one JSONL file per language and writes
   `languages_summary.json`.
4. `lib/generate_keywords_deepseek_s3.py`
   Calls a DeepSeek-compatible OpenAI client for each per-language record, writes
   `keywords_<language>.jsonl`, and records token usage in
   `deepseek_summary.json`.

JSON schema validation is enabled by default after each stage. The schemas live
in `schemas/keyphrase/`.

## Repository Layout

```text
README.md                         Main repository documentation
Makefile                          Make entrypoint including the keyphrase addon
cookbook-repo-addons/keyphrase.mk Keyphrase-specific Make targets and variables
lib/                              Pipeline scripts and schema validator
schemas/keyphrase/                JSON schemas for pipeline outputs
data/                             Small local sample and lookup files
test-data/                        Small test fixture inputs
cookbook/                         Vendored Impresso Make cookbook helpers
build.d/                          Local Make stamps and transient files
```

The canonical pipeline outputs are S3 objects. Files under `build.d/` are local
stamps or transient artifacts used by Make.

## Requirements

- Python 3.11.
- GNU Make 4.0 or later. On macOS, Homebrew installs this as `gmake`.
- S3 credentials in `.env`: `SE_ACCESS_KEY`, `SE_SECRET_KEY`, and `SE_HOST_URL`.
- Python dependencies from `Pipfile`, including `impresso-cookbook`,
  `impresso-pipelines[adclassifier]`, `smart-open`, and `openai`.
- `DEEPSEEK_API_KEY` or `OPENAI_API_KEY` for keyword generation.
- `data/providers-title.json`, unless `KEYPHRASE_PROVIDERS_TITLE_PATH` is
  overridden.

The Make addon uses `.venv/bin/python` when present, otherwise `python3`. Override
`PYTHON_KEYPHRASE` to use another interpreter.

## Setup

Create an environment file and add the S3 endpoint if it is not already present:

```bash
cp dotenv.sample .env
```

Minimum `.env` content:

```bash
SE_ACCESS_KEY=...
SE_SECRET_KEY=...
SE_HOST_URL=https://os.zhdk.cloud.switch.ch/
PIPENV_VENV_IN_PROJECT=enabled
```

Install the Python dependencies:

```bash
pipenv install
```

Then either run commands through `pipenv run` or activate the environment:

```bash
pipenv shell
```

## Running

The examples below use `make`; use `gmake` instead if GNU Make is not installed
as `make` on the local system.

Print the effective keyphrase configuration:

```bash
make keyphrase-config
```

Run the full pipeline:

```bash
DEEPSEEK_API_KEY=... make keyphrase-all
```

Run individual stages:

```bash
make keyphrase-step-1-filter
make keyphrase-step-2-sample
make keyphrase-step-3-split
make keyphrase-step-4-keywords
```

Sync existing S3 outputs into local Make stamps:

```bash
make keyphrase-sync-output
```

## Main Configuration

The default input is the current language-identification aggregate:

```make
KEYPHRASE_INPUT_S3=s3://115-canonical-processed-final/langident/langident-lid-ensemble_multilingual_v2-0-2__AGGREGATED.jsonl.gz
```

The default output run prefix is:

```make
S3_PATH_KEYPHRASE=s3://140-processed-data-sandbox/keyphrase/keyphrase-keywords-deepseek-chat_v1-0-0
```

Common overrides:

```bash
make keyphrase-all \
  KEYPHRASE_INPUT_S3=s3://bucket/path/input.jsonl.gz \
  RUN_VERSION_KEYPHRASE=v1-0-1 \
  KEYPHRASE_TARGET_NON_AD_PER_LANGUAGE=1000 \
  KEYPHRASE_LANGUAGES="de en fr lb"
```

Useful variables:

- `S3_BUCKET_KEYPHRASE`: Output bucket, default `140-processed-data-sandbox`.
- `RUN_VERSION_KEYPHRASE`: Version component in the output run ID.
- `KEYPHRASE_FILTER_TP_OPTION`: Type filter, default `--tp article`.
- `KEYPHRASE_FILTER_MIN_OCRQA_OPTION`: OCR-quality threshold, default
  `--min-ocrqa 0.7`.
- `KEYPHRASE_FILTER_MIN_LEN_OPTION`: Minimum text length, default `--min-len 550`.
- `KEYPHRASE_COMPILER_S3_PREFIX`: Compiler source prefix for missing full text,
  default `s3://122-rebuilt-final/`.
- `KEYPHRASE_TARGET_NON_AD_PER_LANGUAGE`: Non-ad sample target per language,
  default `1000`.
- `KEYPHRASE_LANGUAGES`: Optional space-separated language whitelist.
- `KEYPHRASE_DEEPSEEK_MODEL`: Chat model, default `deepseek-chat`.
- `KEYPHRASE_DEEPSEEK_BASE_URL`: OpenAI-compatible API base URL, default
  `https://api.deepseek.com`.
- `KEYPHRASE_VALIDATE`: Set to `0` to skip JSON schema validation.

## Outputs

For the default run prefix, the pipeline writes:

```text
s3://140-processed-data-sandbox/keyphrase/<RUN_ID>/
  01-filter/filtered.jsonl.gz
  02-classified/classified.jsonl.gz
  02-classified/sample_non_ads.jsonl.gz
  03-per-language/<language>.jsonl.gz
  03-per-language/languages_summary.json
  04-keywords/keywords_<language>.jsonl
  04-keywords/deepseek_summary.json
```

Keyword records keep the original article fields and add a `keywords` array of
English conceptual keyphrases. Per-language records include `ad_class: "non-ad"`
for traceability.

## Direct Script Use

Make is the preferred interface for production runs, but the scripts in `lib/`
can be run directly for debugging and small local/S3 experiments. See
`lib/README.md` for the manual script order.

## About Impresso

[Impresso - Media Monitoring of the Past](https://impresso-project.ch) is an
interdisciplinary research project that aims to develop and consolidate tools for
processing and exploring large collections of media archives across modalities,
time, languages and national borders.

The project is funded by:

- Swiss National Science Foundation (grants
  [CRSII5_173719](http://p3.snf.ch/project-173719) and
  [CRSII5_213585](https://data.snf.ch/grants/grant/213585))
- Luxembourg National Research Fund (grant 17498891)

### Copyright

Copyright (C) 2024 The Impresso team.

### License

This program is provided as open source under the
[GNU Affero General Public License](https://github.com/impresso/impresso-pyindexation/blob/master/LICENSE)
v3 or later.

---

<p align="center">
  <img src="https://github.com/impresso/impresso.github.io/blob/master/assets/images/3x1--Yellow-Impresso-Black-on-White--transparent.png?raw=true" width="350" alt="Impresso Project Logo"/>
</p>
