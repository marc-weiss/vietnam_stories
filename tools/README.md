# Draft Tools

`vietnam_stories_draft.py` generates a local static draft site.

Raw source import:

```bash
python3 tools/vietnam_stories_draft.py \
  --source-root original_website/stories/vietnam \
  --output-dir build/draft_site \
  --emit-csv build/draft_site/threads.csv
```

CSV import:

```bash
python3 tools/vietnam_stories_draft.py \
  --csv-input build/draft_site/threads.csv \
  --output-dir build/draft_site_from_csv
```

Behavior is controlled by [draft_site_config.json](/Users/robreuss_start/Documents/Development/vietnam_stories_weblab/draft_site_config.json). Initial topic curation lives in [draft_topic_curation.json](/Users/robreuss_start/Documents/Development/vietnam_stories_weblab/draft_topic_curation.json).

Email redaction review CSV:

```bash
python3 tools/email_redaction_audit.py \
  --source-root original_website/stories/vietnam \
  --output-csv build/email_redaction_audit.csv
```

This audit is intentionally conservative. It aims for useful review candidates with low false-positive risk, not perfect capture.
