$(call log.debug, BEGIN INCLUDE: cookbook-repo-addons/keyphrase.mk)
###############################################################################
# Keyphrase pipeline targets
#
# This is a collection-level pipeline over an aggregated JSONL.GZ input. It keeps
# the existing manual script order:
#   1. filter_jsonl_gz_s3.py
#   2. sample_classify_ads_s3.py
#   3. split_by_language_jsonl_s3.py
#   4. generate_keywords_deepseek_s3.py
###############################################################################

define KeyphraseS3ToLocal
$(subst s3://,$(BUILD_DIR)/,$(1))
endef

define KeyphraseS3Parent
$(patsubst %/,%,$(dir $(1)))
endef


###############################################################################
# Path and run configuration
###############################################################################

S3_BUCKET_KEYPHRASE ?= 140-processed-data-sandbox
  $(call log.debug, S3_BUCKET_KEYPHRASE)

PROCESS_LABEL_KEYPHRASE ?= keyphrase
  $(call log.debug, PROCESS_LABEL_KEYPHRASE)

TASK_KEYPHRASE ?= keywords
  $(call log.debug, TASK_KEYPHRASE)

MODEL_ID_KEYPHRASE ?= deepseek-chat
  $(call log.debug, MODEL_ID_KEYPHRASE)

RUN_VERSION_KEYPHRASE ?= v1-0-0
  $(call log.debug, RUN_VERSION_KEYPHRASE)

RUN_ID_KEYPHRASE ?= $(PROCESS_LABEL_KEYPHRASE)-$(TASK_KEYPHRASE)-$(MODEL_ID_KEYPHRASE)_$(RUN_VERSION_KEYPHRASE)
  $(call log.debug, RUN_ID_KEYPHRASE)

S3_PATH_KEYPHRASE ?= s3://$(S3_BUCKET_KEYPHRASE)/$(PROCESS_LABEL_KEYPHRASE)/$(RUN_ID_KEYPHRASE)
  $(call log.debug, S3_PATH_KEYPHRASE)

LOCAL_PATH_KEYPHRASE := $(call KeyphraseS3ToLocal,$(S3_PATH_KEYPHRASE))
  $(call log.debug, LOCAL_PATH_KEYPHRASE)

LOCAL_KEYPHRASE_SYNC_STAMP_FILE := $(LOCAL_PATH_KEYPHRASE).last_synced
  $(call log.debug, LOCAL_KEYPHRASE_SYNC_STAMP_FILE)

LOCAL_KEYPHRASE_LOG_DIR := $(LOCAL_PATH_KEYPHRASE)/logs
  $(call log.debug, LOCAL_KEYPHRASE_LOG_DIR)

PYTHON_KEYPHRASE ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
  $(call log.debug, PYTHON_KEYPHRASE)


###############################################################################
# Input and output files
###############################################################################

KEYPHRASE_INPUT_S3 ?= s3://115-canonical-processed-final/langident/langident-lid-ensemble_multilingual_v2-0-2__AGGREGATED.jsonl.gz
  $(call log.debug, KEYPHRASE_INPUT_S3)

KEYPHRASE_FILTERED_S3 ?= $(S3_PATH_KEYPHRASE)/01-filter/filtered.jsonl.gz
  $(call log.debug, KEYPHRASE_FILTERED_S3)

KEYPHRASE_CLASSIFIED_S3 ?= $(S3_PATH_KEYPHRASE)/02-classified/classified.jsonl.gz
  $(call log.debug, KEYPHRASE_CLASSIFIED_S3)

KEYPHRASE_SAMPLE_S3 ?= $(S3_PATH_KEYPHRASE)/02-classified/sample_non_ads.jsonl.gz
  $(call log.debug, KEYPHRASE_SAMPLE_S3)

KEYPHRASE_PER_LANGUAGE_S3_PREFIX ?= $(S3_PATH_KEYPHRASE)/03-per-language
  $(call log.debug, KEYPHRASE_PER_LANGUAGE_S3_PREFIX)

KEYPHRASE_KEYWORDS_S3_PREFIX ?= $(S3_PATH_KEYPHRASE)/04-keywords
  $(call log.debug, KEYPHRASE_KEYWORDS_S3_PREFIX)

KEYPHRASE_PER_LANGUAGE_SUMMARY_NAME ?= languages_summary.json
  $(call log.debug, KEYPHRASE_PER_LANGUAGE_SUMMARY_NAME)

KEYPHRASE_DEEPSEEK_SUMMARY_NAME ?= deepseek_summary.json
  $(call log.debug, KEYPHRASE_DEEPSEEK_SUMMARY_NAME)

LOCAL_KEYPHRASE_FILTERED_FILE := $(call KeyphraseS3ToLocal,$(KEYPHRASE_FILTERED_S3))
LOCAL_KEYPHRASE_CLASSIFIED_FILE := $(call KeyphraseS3ToLocal,$(KEYPHRASE_CLASSIFIED_S3))
LOCAL_KEYPHRASE_SAMPLE_FILE := $(call KeyphraseS3ToLocal,$(KEYPHRASE_SAMPLE_S3))
LOCAL_KEYPHRASE_PER_LANGUAGE_SUMMARY := $(call KeyphraseS3ToLocal,$(KEYPHRASE_PER_LANGUAGE_S3_PREFIX)/$(KEYPHRASE_PER_LANGUAGE_SUMMARY_NAME))
LOCAL_KEYPHRASE_DEEPSEEK_SUMMARY := $(call KeyphraseS3ToLocal,$(KEYPHRASE_KEYWORDS_S3_PREFIX)/$(KEYPHRASE_DEEPSEEK_SUMMARY_NAME))


###############################################################################
# Processing options
###############################################################################

KEYPHRASE_LOG_LEVEL ?= $(LOGGING_LEVEL)
  $(call log.debug, KEYPHRASE_LOG_LEVEL)

KEYPHRASE_FILTER_TP_OPTION ?= --tp article
  $(call log.debug, KEYPHRASE_FILTER_TP_OPTION)

KEYPHRASE_FILTER_MIN_OCRQA_OPTION ?= --min-ocrqa 0.7
  $(call log.debug, KEYPHRASE_FILTER_MIN_OCRQA_OPTION)

KEYPHRASE_FILTER_MIN_LEN_OPTION ?= --min-len 550
  $(call log.debug, KEYPHRASE_FILTER_MIN_LEN_OPTION)

KEYPHRASE_COMPILER_S3_PREFIX ?= s3://122-rebuilt-final/
  $(call log.debug, KEYPHRASE_COMPILER_S3_PREFIX)

KEYPHRASE_TARGET_NON_AD_PER_LANGUAGE ?= 1000
  $(call log.debug, KEYPHRASE_TARGET_NON_AD_PER_LANGUAGE)

KEYPHRASE_BATCH_SIZE_PER_LANGUAGE ?=
  $(call log.debug, KEYPHRASE_BATCH_SIZE_PER_LANGUAGE)

KEYPHRASE_LANGUAGES ?=
  $(call log.debug, KEYPHRASE_LANGUAGES)

KEYPHRASE_RANDOM_SEED ?= 42
  $(call log.debug, KEYPHRASE_RANDOM_SEED)

KEYPHRASE_CLASSIFIER_BATCH_SIZE ?= 64
  $(call log.debug, KEYPHRASE_CLASSIFIER_BATCH_SIZE)

KEYPHRASE_CLASSIFY_REMAINING_OPTION ?=
  $(call log.debug, KEYPHRASE_CLASSIFY_REMAINING_OPTION)

KEYPHRASE_SPLIT_GZIP_OPTION ?= --gzip-output
  $(call log.debug, KEYPHRASE_SPLIT_GZIP_OPTION)

KEYPHRASE_PROVIDERS_TITLE_PATH ?= ./data/providers-title.json
  $(call log.debug, KEYPHRASE_PROVIDERS_TITLE_PATH)

KEYPHRASE_DEEPSEEK_BASE_URL ?= https://api.deepseek.com
  $(call log.debug, KEYPHRASE_DEEPSEEK_BASE_URL)

KEYPHRASE_DEEPSEEK_MODEL ?= deepseek-chat
  $(call log.debug, KEYPHRASE_DEEPSEEK_MODEL)

KEYPHRASE_KEYWORDS_FIELD ?= keywords
  $(call log.debug, KEYPHRASE_KEYWORDS_FIELD)

KEYPHRASE_DEEPSEEK_MAX_RECORDS_OPTION ?=
  $(call log.debug, KEYPHRASE_DEEPSEEK_MAX_RECORDS_OPTION)

KEYPHRASE_SAMPLE_UPLOAD_OPTION ?= --force-overwrite
  $(call log.debug, KEYPHRASE_SAMPLE_UPLOAD_OPTION)


###############################################################################
# Schema validation
###############################################################################

KEYPHRASE_VALIDATE ?= 1
  $(call log.debug, KEYPHRASE_VALIDATE)

KEYPHRASE_SCHEMA_DIR ?= schemas/keyphrase
  $(call log.debug, KEYPHRASE_SCHEMA_DIR)

KEYPHRASE_FILTERED_SCHEMA ?= $(KEYPHRASE_SCHEMA_DIR)/filtered-record.schema.json
KEYPHRASE_CLASSIFIED_SCHEMA ?= $(KEYPHRASE_SCHEMA_DIR)/classified-record.schema.json
KEYPHRASE_PER_LANGUAGE_SCHEMA ?= $(KEYPHRASE_SCHEMA_DIR)/per-language-record.schema.json
KEYPHRASE_KEYWORD_SCHEMA ?= $(KEYPHRASE_SCHEMA_DIR)/keyword-record.schema.json
KEYPHRASE_LANGUAGES_SUMMARY_SCHEMA ?= $(KEYPHRASE_SCHEMA_DIR)/languages-summary.schema.json
KEYPHRASE_DEEPSEEK_SUMMARY_SCHEMA ?= $(KEYPHRASE_SCHEMA_DIR)/deepseek-summary.schema.json

ifeq ($(KEYPHRASE_VALIDATE),1)
KEYPHRASE_VALIDATE_FILTERED_CMD = $(PYTHON_KEYPHRASE) lib/validate_json_schema.py --schema $(KEYPHRASE_FILTERED_SCHEMA) --input $(KEYPHRASE_FILTERED_S3) --file-format jsonl --log-level $(KEYPHRASE_LOG_LEVEL)
KEYPHRASE_VALIDATE_CLASSIFIED_CMD = $(PYTHON_KEYPHRASE) lib/validate_json_schema.py --schema $(KEYPHRASE_CLASSIFIED_SCHEMA) --input $(KEYPHRASE_CLASSIFIED_S3) --file-format jsonl --log-level $(KEYPHRASE_LOG_LEVEL)
KEYPHRASE_VALIDATE_SAMPLE_CMD = $(PYTHON_KEYPHRASE) lib/validate_json_schema.py --schema $(KEYPHRASE_PER_LANGUAGE_SCHEMA) --input $(LOCAL_KEYPHRASE_SAMPLE_FILE) --file-format jsonl --log-level $(KEYPHRASE_LOG_LEVEL)
KEYPHRASE_VALIDATE_PER_LANGUAGE_CMD = $(PYTHON_KEYPHRASE) lib/validate_json_schema.py --schema $(KEYPHRASE_PER_LANGUAGE_SCHEMA) --input-prefix $(KEYPHRASE_PER_LANGUAGE_S3_PREFIX) --file-format jsonl --log-level $(KEYPHRASE_LOG_LEVEL)
KEYPHRASE_VALIDATE_LANGUAGE_SUMMARY_CMD = $(PYTHON_KEYPHRASE) lib/validate_json_schema.py --schema $(KEYPHRASE_LANGUAGES_SUMMARY_SCHEMA) --input $(KEYPHRASE_PER_LANGUAGE_S3_PREFIX)/$(KEYPHRASE_PER_LANGUAGE_SUMMARY_NAME) --file-format json --log-level $(KEYPHRASE_LOG_LEVEL)
KEYPHRASE_VALIDATE_KEYWORDS_CMD = $(PYTHON_KEYPHRASE) lib/validate_json_schema.py --schema $(KEYPHRASE_KEYWORD_SCHEMA) --input-prefix $(KEYPHRASE_KEYWORDS_S3_PREFIX) --file-format jsonl --log-level $(KEYPHRASE_LOG_LEVEL)
KEYPHRASE_VALIDATE_DEEPSEEK_SUMMARY_CMD = $(PYTHON_KEYPHRASE) lib/validate_json_schema.py --schema $(KEYPHRASE_DEEPSEEK_SUMMARY_SCHEMA) --input $(KEYPHRASE_KEYWORDS_S3_PREFIX)/$(KEYPHRASE_DEEPSEEK_SUMMARY_NAME) --file-format json --log-level $(KEYPHRASE_LOG_LEVEL)
else
KEYPHRASE_VALIDATE_FILTERED_CMD = true
KEYPHRASE_VALIDATE_CLASSIFIED_CMD = true
KEYPHRASE_VALIDATE_SAMPLE_CMD = true
KEYPHRASE_VALIDATE_PER_LANGUAGE_CMD = true
KEYPHRASE_VALIDATE_LANGUAGE_SUMMARY_CMD = true
KEYPHRASE_VALIDATE_KEYWORDS_CMD = true
KEYPHRASE_VALIDATE_DEEPSEEK_SUMMARY_CMD = true
endif


###############################################################################
# Help and config
###############################################################################

help::
	@echo ""
	@echo "KEYPHRASE PIPELINE:"
	@echo "  keyphrase-all              # Run filter, ad sampling, language split, and DeepSeek keywords"
	@echo "  keyphrase-step-1-filter    # Step 1: filter aggregated JSONL.GZ"
	@echo "  keyphrase-step-2-sample    # Step 2: sample/classify non-ad records"
	@echo "  keyphrase-step-3-split     # Step 3: split sampled non-ads by language"
	@echo "  keyphrase-step-4-keywords  # Step 4: generate DeepSeek keywords"
	@echo "  keyphrase-sync-output      # Sync keyphrase S3 outputs into local stamps"
	@echo "  keyphrase-config           # Print keyphrase paths and main options"
	@echo ""
	@echo "KEYPHRASE MAIN VARIABLES:"
	@echo "  KEYPHRASE_INPUT_S3=$(KEYPHRASE_INPUT_S3)"
	@echo "  S3_PATH_KEYPHRASE=$(S3_PATH_KEYPHRASE)"
	@echo "  KEYPHRASE_TARGET_NON_AD_PER_LANGUAGE=$(KEYPHRASE_TARGET_NON_AD_PER_LANGUAGE)"
	@echo "  KEYPHRASE_VALIDATE=$(KEYPHRASE_VALIDATE)"

keyphrase-config:
	@echo "KEYPHRASE_INPUT_S3=$(KEYPHRASE_INPUT_S3)"
	@echo "S3_PATH_KEYPHRASE=$(S3_PATH_KEYPHRASE)"
	@echo "KEYPHRASE_FILTERED_S3=$(KEYPHRASE_FILTERED_S3)"
	@echo "KEYPHRASE_CLASSIFIED_S3=$(KEYPHRASE_CLASSIFIED_S3)"
	@echo "KEYPHRASE_SAMPLE_S3=$(KEYPHRASE_SAMPLE_S3)"
	@echo "KEYPHRASE_PER_LANGUAGE_S3_PREFIX=$(KEYPHRASE_PER_LANGUAGE_S3_PREFIX)"
	@echo "KEYPHRASE_KEYWORDS_S3_PREFIX=$(KEYPHRASE_KEYWORDS_S3_PREFIX)"
	@echo "KEYPHRASE_COMPILER_S3_PREFIX=$(KEYPHRASE_COMPILER_S3_PREFIX)"
	@echo "KEYPHRASE_PROVIDERS_TITLE_PATH=$(KEYPHRASE_PROVIDERS_TITLE_PATH)"
	@echo "KEYPHRASE_DEEPSEEK_BASE_URL=$(KEYPHRASE_DEEPSEEK_BASE_URL)"
	@echo "KEYPHRASE_DEEPSEEK_MODEL=$(KEYPHRASE_DEEPSEEK_MODEL)"
	@echo "PYTHON_KEYPHRASE=$(PYTHON_KEYPHRASE)"
	@echo "LOCAL_PATH_KEYPHRASE=$(LOCAL_PATH_KEYPHRASE)"

.PHONY: keyphrase-config


###############################################################################
# Sync targets
###############################################################################

keyphrase-sync-output: $(LOCAL_KEYPHRASE_SYNC_STAMP_FILE)

$(LOCAL_KEYPHRASE_SYNC_STAMP_FILE):
	$(MAKE_SILENCE_RECIPE)mkdir -p $(LOCAL_PATH_KEYPHRASE) $(LOCAL_KEYPHRASE_LOG_DIR) && \
	$(PYTHON_KEYPHRASE) -m impresso_cookbook.s3_to_local_stamps \
		$(S3_PATH_KEYPHRASE) \
		--local-dir $(BUILD_DIR) \
		--file-extensions jsonl.gz jsonl json \
		--stamp-mode per-file \
		--remove-dangling-stamps \
		--logfile $(LOCAL_KEYPHRASE_LOG_DIR)/sync-output.log.gz \
		--log-level $(KEYPHRASE_LOG_LEVEL) && \
	touch $@

keyphrase-clean-sync:
	$(MAKE_SILENCE_RECIPE)rm -rvf $(LOCAL_PATH_KEYPHRASE) $(LOCAL_KEYPHRASE_SYNC_STAMP_FILE) || true

.PHONY: keyphrase-sync-output keyphrase-clean-sync


###############################################################################
# Pipeline entry points
###############################################################################

keyphrase-all: keyphrase-step-4-keywords

keyphrase-filter keyphrase-step-1-filter: $(LOCAL_KEYPHRASE_FILTERED_FILE)
keyphrase-sample keyphrase-step-2-sample: $(LOCAL_KEYPHRASE_SAMPLE_FILE)
keyphrase-split keyphrase-step-3-split: $(LOCAL_KEYPHRASE_PER_LANGUAGE_SUMMARY)
keyphrase-keywords keyphrase-step-4-keywords: $(LOCAL_KEYPHRASE_DEEPSEEK_SUMMARY)

.PHONY: keyphrase-all keyphrase-filter keyphrase-sample keyphrase-split keyphrase-keywords
.PHONY: keyphrase-step-1-filter keyphrase-step-2-sample keyphrase-step-3-split keyphrase-step-4-keywords


###############################################################################
# Step 1: filter aggregated input
###############################################################################

$(LOCAL_KEYPHRASE_FILTERED_FILE):
	$(MAKE_SILENCE_RECIPE)mkdir -p $(@D) $(LOCAL_KEYPHRASE_LOG_DIR) && \
	$(PYTHON_KEYPHRASE) lib/filter_jsonl_gz_s3.py \
		--input-s3 $(KEYPHRASE_INPUT_S3) \
		--output-s3 $(KEYPHRASE_FILTERED_S3) \
		$(KEYPHRASE_FILTER_TP_OPTION) \
		$(KEYPHRASE_FILTER_MIN_OCRQA_OPTION) \
		$(KEYPHRASE_FILTER_MIN_LEN_OPTION) \
		--log-file $(LOCAL_KEYPHRASE_LOG_DIR)/01-filter.log.gz \
		--log-level $(KEYPHRASE_LOG_LEVEL) && \
	$(KEYPHRASE_VALIDATE_FILTERED_CMD) && \
	$(PYTHON_KEYPHRASE) -m impresso_cookbook.s3_to_local_stamps \
		$(call KeyphraseS3Parent,$(KEYPHRASE_FILTERED_S3)) \
		--local-dir $(BUILD_DIR) \
		--file-extensions jsonl.gz \
		--stamp-mode per-file \
		--remove-dangling-stamps \
		--logfile $(LOCAL_KEYPHRASE_LOG_DIR)/01-filter-sync.log.gz \
		--log-level $(KEYPHRASE_LOG_LEVEL) && \
	test -e $@


###############################################################################
# Step 2: sample/classify non-ads
###############################################################################

$(LOCAL_KEYPHRASE_SAMPLE_FILE): $(LOCAL_KEYPHRASE_FILTERED_FILE)
	$(MAKE_SILENCE_RECIPE)mkdir -p $(@D) $(dir $(LOCAL_KEYPHRASE_CLASSIFIED_FILE)) $(LOCAL_KEYPHRASE_LOG_DIR) && \
	$(PYTHON_KEYPHRASE) lib/sample_classify_ads_s3.py \
		--input-s3 $(KEYPHRASE_FILTERED_S3) \
		--output-s3 $(KEYPHRASE_CLASSIFIED_S3) \
		--compiler-s3-prefix $(KEYPHRASE_COMPILER_S3_PREFIX) \
		--target-non-ad-per-language $(KEYPHRASE_TARGET_NON_AD_PER_LANGUAGE) \
		$(if $(strip $(KEYPHRASE_BATCH_SIZE_PER_LANGUAGE)),--batch-size-per-language $(KEYPHRASE_BATCH_SIZE_PER_LANGUAGE),) \
		$(if $(strip $(KEYPHRASE_LANGUAGES)),--languages $(KEYPHRASE_LANGUAGES),) \
		--random-seed $(KEYPHRASE_RANDOM_SEED) \
		--classifier-batch-size $(KEYPHRASE_CLASSIFIER_BATCH_SIZE) \
		$(KEYPHRASE_CLASSIFY_REMAINING_OPTION) \
		--download-local $@ \
		--log-file $(LOCAL_KEYPHRASE_LOG_DIR)/02-sample-classify.log.gz \
		--log-level $(KEYPHRASE_LOG_LEVEL) && \
	$(KEYPHRASE_VALIDATE_CLASSIFIED_CMD) && \
	$(KEYPHRASE_VALIDATE_SAMPLE_CMD) && \
	$(PYTHON_KEYPHRASE) -m impresso_cookbook.local_to_s3 \
		$(KEYPHRASE_SAMPLE_UPLOAD_OPTION) \
		$@ $(KEYPHRASE_SAMPLE_S3) && \
	$(PYTHON_KEYPHRASE) -m impresso_cookbook.s3_to_local_stamps \
		$(call KeyphraseS3Parent,$(KEYPHRASE_SAMPLE_S3)) \
		--local-dir $(BUILD_DIR) \
		--file-extensions jsonl.gz \
		--stamp-mode per-file \
		--remove-dangling-stamps \
		--logfile $(LOCAL_KEYPHRASE_LOG_DIR)/02-sample-sync.log.gz \
		--log-level $(KEYPHRASE_LOG_LEVEL) && \
	test -s $@


###############################################################################
# Step 3: split sampled non-ads by language
###############################################################################

$(LOCAL_KEYPHRASE_PER_LANGUAGE_SUMMARY): $(LOCAL_KEYPHRASE_SAMPLE_FILE)
	$(MAKE_SILENCE_RECIPE)mkdir -p $(@D) $(LOCAL_KEYPHRASE_LOG_DIR) && \
	$(PYTHON_KEYPHRASE) lib/split_by_language_jsonl_s3.py \
		--input-path $(KEYPHRASE_SAMPLE_S3) \
		--output-prefix $(KEYPHRASE_PER_LANGUAGE_S3_PREFIX) \
		--summary-name $(KEYPHRASE_PER_LANGUAGE_SUMMARY_NAME) \
		$(KEYPHRASE_SPLIT_GZIP_OPTION) \
		--log-file $(LOCAL_KEYPHRASE_LOG_DIR)/03-split-by-language.log.gz \
		--log-level $(KEYPHRASE_LOG_LEVEL) && \
	$(KEYPHRASE_VALIDATE_PER_LANGUAGE_CMD) && \
	$(KEYPHRASE_VALIDATE_LANGUAGE_SUMMARY_CMD) && \
	$(PYTHON_KEYPHRASE) -m impresso_cookbook.s3_to_local_stamps \
		$(KEYPHRASE_PER_LANGUAGE_S3_PREFIX) \
		--local-dir $(BUILD_DIR) \
		--file-extensions jsonl.gz jsonl json \
		--stamp-mode per-file \
		--remove-dangling-stamps \
		--logfile $(LOCAL_KEYPHRASE_LOG_DIR)/03-split-sync.log.gz \
		--log-level $(KEYPHRASE_LOG_LEVEL) && \
	test -e $@


###############################################################################
# Step 4: generate DeepSeek keywords
###############################################################################

$(LOCAL_KEYPHRASE_DEEPSEEK_SUMMARY): $(LOCAL_KEYPHRASE_PER_LANGUAGE_SUMMARY)
	$(MAKE_SILENCE_RECIPE)mkdir -p $(@D) $(LOCAL_KEYPHRASE_LOG_DIR) && \
	$(PYTHON_KEYPHRASE) lib/generate_keywords_deepseek_s3.py \
		--input-prefix $(KEYPHRASE_PER_LANGUAGE_S3_PREFIX) \
		--output-prefix $(KEYPHRASE_KEYWORDS_S3_PREFIX) \
		--providers-title-path $(KEYPHRASE_PROVIDERS_TITLE_PATH) \
		--base-url $(KEYPHRASE_DEEPSEEK_BASE_URL) \
		--model $(KEYPHRASE_DEEPSEEK_MODEL) \
		--keywords-field $(KEYPHRASE_KEYWORDS_FIELD) \
		--deepseek-summary-name $(KEYPHRASE_DEEPSEEK_SUMMARY_NAME) \
		$(KEYPHRASE_DEEPSEEK_MAX_RECORDS_OPTION) \
		--log-file $(LOCAL_KEYPHRASE_LOG_DIR)/04-generate-keywords.log.gz \
		--log-level $(KEYPHRASE_LOG_LEVEL) && \
	$(KEYPHRASE_VALIDATE_KEYWORDS_CMD) && \
	$(KEYPHRASE_VALIDATE_DEEPSEEK_SUMMARY_CMD) && \
	$(PYTHON_KEYPHRASE) -m impresso_cookbook.s3_to_local_stamps \
		$(KEYPHRASE_KEYWORDS_S3_PREFIX) \
		--local-dir $(BUILD_DIR) \
		--file-extensions jsonl json \
		--stamp-mode per-file \
		--remove-dangling-stamps \
		--logfile $(LOCAL_KEYPHRASE_LOG_DIR)/04-keywords-sync.log.gz \
		--log-level $(KEYPHRASE_LOG_LEVEL) && \
	test -e $@

$(call log.debug, END INCLUDE: cookbook-repo-addons/keyphrase.mk)
