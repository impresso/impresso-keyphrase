Order of running files:
1. `filter_jsonl_gz_s3.py`  
   First-pass cleaner for large corpora. It reads JSONL/JSONL.GZ from local disk or S3, applies field/value filters, and writes a reduced dataset you want to process downstream.

2. `sample_classify_ads_s3.py`  
   Builds the ad-filtered test pool. It samples candidates per language, retrieves missing full text (`ft`) via the compiler when needed, runs the impresso ad-classifier (`ad` / `non-ad`), and appends only new classifications to a persistent `_classified` file so reruns do not reprocess old IDs.

3. `split_by_language_jsonl_s3.py`  
   Final organization/export step. It takes the classified JSONL/JSONL.GZ, writes one output file per language, and creates a `languages_summary.json` with available languages and aggregated text length totals per language. Supports local output and direct S3 output.

4. `generate_keywords_deepseek_s3.py`  
   Keyword-generation step. It reads per-language JSONL files from a local/S3 folder (plus `languages_summary.json`), calls the DeepSeek chat model with the notebook prompt structure, enriches each article with a `keywords` field, writes `keywords_<language>.jsonl` files, and emits a `deepseek_summary.json` with per-language token usage and totals.
