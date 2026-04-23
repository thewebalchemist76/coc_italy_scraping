# coc_italy_scraping
Python scraper for extracting company data from Italian Chamber of Commerce sources (reportaziende.it) with batching and resume support


# COc Scraping – Italian Company Registry Scraper

Python scraper for extracting structured company data from **reportaziende.it**, an Italian company registry source.

The script is designed for large-scale extraction with batching, retry logic, and resume support.

---

## Overview

This tool was built to automate the manual extraction of company data from structured registry pages and convert it into a clean dataset for analysis and outreach workflows.

It is optimized for reliability on large datasets and unstable endpoints.

---

## Features

- Batch processing of large company ID lists
- Resume support (continues from last successful index)
- JSONL structured output (1 company per line)
- Retry logic with exponential backoff
- 403 handling with cooldown + session priming
- Failed ID tracking
- Checkpoint state persistence

---

## Input format

A JSON file containing an array of objects:

```json
[
  {"id": "MI_2610233_0"},
  {"id": "MI_1234567_0"}
]
Output

Generated files inside the output folder:

*_details.jsonl → structured company data
*_state.json → resume checkpoint
*_failed_ids.txt → failed requests log
Requirements
Python 3.9+
requests

Install dependencies:

pip install requests
Usage
python scraper.py \
  --input companies.json \
  --out-base coc_dataset \
  --sleep 0.2
Optional arguments
--fresh → restart from scratch (ignore saved state)
--cookie → optional session cookie if required
--timeout → request timeout (default: 30s)
--retries → retry attempts per request (default: 3)
--cooldown-on-403 → delay when blocked (default: 30s)
Example
python scraper.py \
  --input aziende.json \
  --out-base italy_companies \
  --sleep 0.2 \
  --retries 3
Architecture notes

The script is designed around:

stateless HTTP requests with session reuse
incremental processing (safe to stop and restart)
structured logging of failures
resilience against basic blocking mechanisms
Use case

Originally built for a communication agency to:

replace manual daily research of company data
build structured datasets for outreach and analysis
scale lead research workflows
Disclaimer

This tool is intended for legitimate data extraction and research purposes only. Users are responsible for ensuring compliance with applicable terms of service and regulations.

Author

Elda Di Matteo
LinkedIn: https://www.linkedin.com/in/elda-di-matteo/
