# Green Builder Media Retrieval Bot MVP

A standalone, retrieval-based chatbot for `greenbuildermedia.com` that:

- crawls public pages from the site's sitemap
- extracts clean article text and metadata
- chunks content into retrieval passages
- embeds and stores passages in a local LanceDB index
- answers user questions using only Green Builder Media content
- returns source links for every answer
- provides an embeddable website widget

This MVP is designed to save implementation time. It does **not** require rebuilding the site or changing the CMS. It runs as a separate service that can be embedded into any page with one script tag.

## Architecture

1. `scripts/crawl_greenbuilder.py`
   - reads `robots.txt` and/or `sitemap.xml`
   - filters allowed Green Builder URLs
   - fetches and cleans article pages
   - stores normalized documents in `data/documents.jsonl`

2. `scripts/build_index.py`
   - chunks documents into passages
   - creates embeddings with OpenAI
   - stores records in LanceDB under `data/lancedb`

3. `app/main.py`
   - FastAPI backend
   - `/health` endpoint
   - `/chat` endpoint for grounded answers
   - `/widget.js` endpoint to serve the embed script

4. `widget/embed.js`
   - lightweight embeddable chat launcher
   - inject with a single `<script>` tag

## Why this design

This is the fastest path to a useful production-style bot for a publishing archive:

- no WordPress plugin dependency
- no custom training workflow
- works on thousands of pages
- easy to re-crawl nightly or after publishing batches
- citations keep it trustworthy

## Suggested hosting

- **Backend**: Render, Railway, Fly.io, or any Linux VM
- **Persistent volume**: required for `data/lancedb`
- **Secrets**: `OPENAI_API_KEY`

## Minimal deployment steps

1. Create a Python 3.11+ service.
2. Set environment variables from `.env.example`.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Crawl the site:
   ```bash
   python scripts/crawl_greenbuilder.py
   ```
5. Build the vector index:
   ```bash
   python scripts/build_index.py
   ```
6. Start the API:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```
7. Embed the widget on Green Builder Media pages:
   ```html
   <script
     src="https://YOUR-BOT-DOMAIN/widget.js"
     data-chatbot-title="Ask Green Builder"
     data-api-base="https://YOUR-BOT-DOMAIN"
   ></script>
   ```

## Grounding rules

The bot is configured to:

- answer from Green Builder content only
- prefer recent pages when the query asks for latest coverage
- cite source pages in every answer
- refuse to invent facts not supported by retrieved passages

## Recommended first crawl scope

Start with the most valuable sections first:

- `/blog`
- article pages linked from the blog archive
- topic pages only as metadata helpers
- optionally: ebooks, magazine pages, webinar pages

## Notes for your developer

### 1) Nightly refresh
Set a cron or scheduled job:

```bash
python scripts/crawl_greenbuilder.py && python scripts/build_index.py
```

### 2) CORS
Update `ALLOWED_ORIGINS` in `.env` to include `https://www.greenbuildermedia.com`.

### 3) Styling
The widget is intentionally simple. It can be restyled to match brand colors by editing `widget/embed.js`.

### 4) Scaling
For larger usage, swap LanceDB for Postgres+pgvector or a managed vector DB. The retrieval logic in `app/retrieval.py` is modular so storage can be swapped later.

## Deliverables in this package

- working crawler
- working index builder
- working FastAPI chat API
- working embeddable widget
- config templates



## Private archive support and attribution

This MVP now supports mixing **public** and **private** Green Builder documents in the same index.

Each document can include:

- `visibility`: `public` or `private`
- `attribution_label`: the branded attribution to use when private material informs an answer

Example private record:

```json
{"title":"Draft article title","url":"","published_at":"2024-11-08","category":"Building Science","text":"...","visibility":"private","attribution_label":"Green Builder Media's editorial archive"}
```

Behavior:

- Public records may appear in the response `sources` list with URLs.
- Private records are retrieved and used as background material.
- Private records are **not** returned in the `sources` list.
- When private material materially shapes the answer, the generator is instructed to use natural attribution such as:
  - `Green Builder Media's research archive suggests...`
  - `Green Builder Media's editors note...`
  - `Based on Green Builder Media's internal editorial archive...`

### Importing a HubSpot ZIP of unpublished blogs

Use the included importer:

```bash
python scripts/import_private_hubspot_zip.py "/path/to/mpower draft blogs hubspot zipped.zip"   --output ./data/private_documents.jsonl   --append-to-docs ./data/documents.jsonl
```

That script:

- reads `.html` files from blog paths inside the ZIP
- skips obvious temporary-slug pages
- extracts title, publish date where available, category, and body text
- marks every imported document as `visibility=private`
- assigns `attribution_label="Green Builder Media's editorial archive"`

### Recommended indexing workflow with private drafts

```bash
python scripts/crawl_greenbuilder.py
python scripts/import_private_hubspot_zip.py "/path/to/private-export.zip" --output ./data/private_documents.jsonl --append-to-docs ./data/documents.jsonl
python scripts/build_index.py
```

### API response additions

The `/chat` response now also includes:

- `private_archive_used`: boolean
- `attribution_note`: which private attribution label(s) were used in retrieval

This makes it easier to audit when unpublished material influenced an answer.


## Editorial governance for private archives

Private records now support three response-use modes:

- `paraphrase`: may influence the answer and can receive branded attribution such as `Green Builder Media's research archive suggests...`
- `weight_only`: may help internal retrieval/background weighting, but should not be paraphrased, quoted, or directly attributed
- `blocked`: excluded from retrieval and response generation entirely

The governance layer also flags stale or risky private material using date age, obsolete-platform signals, placeholder text, embargo language, and technology/policy references that are likely to have changed.

### Classify private documents before indexing

```bash
python scripts/import_private_hubspot_zip.py "/path/to/private-export.zip" --output ./data/private_documents.jsonl --append-to-docs ./data/documents.jsonl
python scripts/classify_private_docs.py ./data/private_documents.jsonl
python scripts/build_index.py
```

In practice, you can leave public documents alone and run governance mainly on the unpublished archive.

## Editor correction console

This MVP now includes a lightweight editor-facing correction app.

What it does:
- logs recent chatbot questions and answers
- lets editors save an override for an exact question, a recurring phrase, or a regex pattern
- applies the editor correction immediately on future matching questions
- does not require a re-crawl or reindex for simple answer fixes

### Accessing the console

Set these env vars:

```bash
ADMIN_USERNAME=editor
ADMIN_PASSWORD=strong-password-here
```

Then open:

```text
https://YOUR-BOT-DOMAIN/admin
```

Editors can review recent answers and create a correction by typing the corrected answer into the form.

### How corrections behave

- `exact`: fixes one specific question
- `contains`: fixes a family of similar questions containing a phrase
- `regex`: advanced pattern matching for repeated edge cases

When a correction is applied, the API returns it immediately and marks the response as editor-corrected.


## Green Builder editorial voice and quick-fix workflow

This package is now tuned to answer in a more Green Builder-style voice:
- direct and journalistic
- practical about tradeoffs, costs, and timing
- grounded in sustainable-building topics without sounding promotional

### Editor-only Fix this answer button in the site widget

To show the quick correction button in the widget for editors, use:

```html
<script
  src="https://YOUR-BOT-DOMAIN/widget.js"
  data-chatbot-title="Ask Green Builder"
  data-api-base="https://YOUR-BOT-DOMAIN"
  data-editor-tools="true"
></script>
```

When an editor clicks **Fix this answer**, the bot opens the admin console with the question and answer pre-filled so the editor can simply type a corrected response and save it.

### Preprocessing the uploaded HubSpot draft archive before install

You can preprocess the draft archive before deployment so install day is easier:

```bash
python scripts/import_private_hubspot_zip.py "/path/to/mpower draft blogs hubspot zipped.zip" --output ./data/private_documents.jsonl
python scripts/classify_private_docs.py ./data/private_documents.jsonl
python scripts/summarize_private_archive.py ./data/private_documents.jsonl --output ./data/private_archive_report.json
```

That produces:
- `data/private_documents.jsonl` for indexing later
- `data/private_archive_report.json` so editors can review what was imported, downgraded, or blocked before the bot goes live
