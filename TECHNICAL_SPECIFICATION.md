# Vietnam Stories Website Technical Specification

## Purpose

This project rebuilds the dialogue portion of the PBS `POV` / WebLab `Re: Vietnam: Stories Since the War` website as a modern static archive. The goal is not to recreate the original site in full. The goal is to preserve and present the dialogue/forum material in a form that is readable, contextualized, accessible, mobile-friendly, and maintainable.

The rebuilt site should function both as:

- a public-facing reading experience for general visitors
- a research-friendly archive that preserves the historical material with minimal technical friction

## Primary Source Materials

- Legacy site files in [original_website](/Users/robreuss_start/Documents/Development/vietnam_stories_weblab/original_website)
- Limited export snapshot in [limited_export](/Users/robreuss_start/Documents/Development/vietnam_stories_weblab/limited_export)
- Existing Swift extraction / generation prototype in [WeblabTransformer](/Users/robreuss_start/Documents/Development/vietnam_stories_weblab/WeblabTransformer)
- Email archive in [requirements_and_notes/emails_with_marc](/Users/robreuss_start/Documents/Development/vietnam_stories_weblab/requirements_and_notes/emails_with_marc)
- Initial Codex brief in [Codex Requirements for Vietnam Stories Website.md](/Users/robreuss_start/Documents/Development/vietnam_stories_weblab/requirements_and_notes/requirements_for_codex/Codex%20Requirements%20for%20Vietnam%20Stories%20Website/Codex%20Requirements%20for%20Vietnam%20Stories%20Website.md)

## Project Framing

The archive is valuable for at least three reasons:

- It captures an early web-based public discussion tied directly to a television broadcast.
- It documents a distinct moment in post-war reflection, reconciliation, and public memory around Vietnam.
- It shows a different online conversational culture than contemporary social media: slower, more reflective, more personal, and less performative.

## Confirmed Product Direction

The following decisions appear confirmed in the email discussion:

- The site will be static HTML.
- The site must run locally from a static tree on disk; no server-side scripting or server-side data delivery should be required.
- Client-side JavaScript is acceptable where it improves the reading experience, as long as the site remains locally runnable and fundamentally static.
- Work should proceed as draft development first; public release comes later after iteration and testing.
- Initial hosting can eventually be GitHub or GitHub Pages because it is simple, durable, and easy to publish, but that is not a current blocker.
- The rebuilt site focuses on the dialogue/forum material rather than the full original `POV` microsite.
- The site needs a substantial landing page that explains historical context before the reader enters the archive.
- The landing page should likely take the form of a `Letter from the Producer`.
- The draft site should not include original site chrome initially; that can be layered back in later if it adds value.
- The archive should preserve some original visual character in tone and seriousness, but the initial draft should prioritize clean reading pages over legacy chrome.
- Thread index ordering for the main experience should prioritize the most active threads rather than the original chronological order.
- `Activity order` is defined as:
- the special comments thread pinned first when present
- all remaining threads sorted by descending post count
- ties broken by original position within the source thread index, then by thread id
- Both activity order and original order should be supported.
- A simple configuration flag should control whether original-order navigation is exposed in the UI.
- Thread pages should allow visitors to scan long discussions quickly by truncating long posts and expanding them inline with a `Show All` control.
- Navigation within a thread should stay minimal; there should be a clear return path to the thread index rather than heavy cross-thread navigation.
- Email addresses should not be redacted by default in the draft build, because the corpus contains many malformed or inconsistent addresses and uneven redaction would be misleading.
- High-confidence email redaction can be offered as an optional mode if it can be done reliably for well-formed addresses only.
- Participant names should generally remain visible for authenticity, with the option to edit a small number of exceptional cases later if needed.
- CSV import should be part of the draft workflow so the site can be tested against normalized structured data as soon as that export is available.
- Topic-based curation should be AI-assisted, with an initial machine-assisted pass that can later be edited manually.

## Information Architecture

### 1. Landing Page

The landing page is a critical interpretive layer, not a decorative intro. It should:

- explain what the archive is
- explain why it matters historically
- explain the relationship between `POV`, the web, and the Vietnam discussion
- prepare readers for the tone and culture of the discussion
- provide curated entry points into representative threads or exchanges
- link into the forum archive itself

Likely content blocks:

- title / project introduction
- `Letter from the Producer`
- explanation of the original experiment in web-based dialogue
- short notes on how to read the archive today
- highlighted topics or exemplary exchanges
- entry link into the thread archive
- optional note on preservation / archival strategy

### 2. Main Dialogue Index

This page is the main gateway into the archive.

Requirements:

- list all discussion threads
- default sort: thread activity, with the historically significant special comments thread pinned first if appropriate
- show thread title
- show number of posts
- show a human-readable date range such as `May 7, 1997 to June 1, 1997`
- avoid visual clutter
- preserve strong readability on desktop and mobile

Potential secondary controls:

- alternate sort or alternate view for chronological/original ordering
- links to highlighted topics from the landing page

### 3. Thread Page

Each thread page should:

- display posts in chronological order within the thread
- provide a clear link back to the main thread index
- support direct permalinks to individual posts
- show author name as preserved in source material
- preserve author identity text as stored in source material by default
- show a preview for long posts and allow inline expansion without a full page reload

Low-noise navigation is preferred. The page should not feel like a modern social platform; it should feel like a readable archival conversation.

### 4. Topic Curation Layer

Draft development should include an initial AI-assisted topic layer. The first pass does not need to be final-quality scholarship; it needs to be useful enough to support iterative testing and editorial review.

Requirements:

- store the curated topics in a format that can be edited without changing parser code
- associate topics with threads and/or posts using an initial AI-assisted pass
- allow later editorial refinement

### 5. Research Layer

These items remain useful but are secondary to the draft reading experience:

- downloadable CSV of structured content
- downloadable static site bundle
- richer semantic topic index generated from the corpus

## Content And Data Requirements

### Source Content

The current extraction work suggests the content model includes:

- thread index id
- thread title
- post id
- post title
- post body
- post creation date
- poster name string
- poster email string
- source directory / source path

### Known Content Characteristics

- Participants often identified themselves manually in free-form ways.
- Names are inconsistent across posts.
- Email addresses can appear both in signatures and inside post bodies.
- Some posts contain formatting issues such as ambiguous line breaks or poetry-like text that may require careful rendering.
- The corpus appears large enough to benefit from structured exports and optional topic-based discovery.

Approximate scale mentioned in the email archive:

- 4,167 unique extracted name strings
- 1,050 lowercase-deduplicated name strings
- an estimated true participant count of roughly 500 to 800 people

## Privacy And Editorial Policy

### Confirmed Baseline

- Preserve participant names by default.
- Preserve the historical tone and textual oddities where possible.
- Do not apply blanket email redaction in the draft build.
- If redaction is attempted later, it should be limited to high-confidence cases so the transformation remains consistent and auditable.

### Exceptional Handling

The system should make it feasible to edit a small number of posts later if necessary, especially for:

- threatening content
- doxxing-like content
- other clear privacy or safety concerns

This implies the build pipeline should support deterministic regeneration from structured source data plus a small editorial override layer.

## Visual And UX Direction

The rebuilt site should preserve cues from the original while clearly being a modern edition.

Visual cues to retain:

- dark / black background as a nod to the original
- `Forum` framing in the right-column / contextual navigation spirit
- archival tone rather than consumer-web polish

Modernizations required:

- improved typography for long-form reading
- higher contrast and more consistent spacing
- accessible semantics and keyboard usability
- responsive layout for mobile devices
- simplified navigation
- better handling of long posts and long thread lists

## Accessibility Requirements

The site should materially improve on the historical version.

Requirements:

- semantic HTML structure
- readable heading hierarchy
- sufficient color contrast
- mobile-responsive layouts
- focus-visible states for keyboard users
- descriptive link text where practical
- reduced reliance on purely visual navigation cues

Because this is static HTML, accessibility should be designed into the generated markup rather than added later with server-side or data-delivery dependencies. Small client-side enhancements are acceptable.

## Hosting And Preservation Strategy

### Draft Delivery

- Generate a local static tree for draft review and testing.
- Do not block draft work on domain or hosting decisions.
- Keep the generated output usable by double-clicking into `index.html` or serving it from a simple local file server.

### Longer-Term Strategy

The email discussion points toward a later hybrid preservation approach:

- public canonical edition hosted on GitHub-based infrastructure
- later outreach to institutions or archives once the public version exists

Likely archive targets discussed:

- Library of Congress
- American Archive of Public Broadcasting
- Internet Archive
- Vietnam Center and Sam Johnson Vietnam Archive

## Build Pipeline Requirements

### Current State

There is an existing Swift prototype in [ContentView.swift](/Users/robreuss_start/Documents/Development/vietnam_stories_weblab/WeblabTransformer/WeblabTransformer/ContentView.swift) that:

- reads thread indexes and post files
- builds structured thread/post objects
- exports CSV
- contains a prototype static HTML exporter

### Build System Expectations

The production pipeline should:

- ingest source files deterministically
- support both raw-source import and CSV import
- normalize structure into an internal data model
- optionally apply high-confidence email redaction
- support editorial fixes and exceptions
- generate the static site
- generate CSV exports for testing and review
- generate initial topic curation output

### Language Choice

Either of these paths is viable:

- continue in Swift if the existing extractor is close to usable and command-line generation can be cleanly separated from the app UI
- migrate to Python if a wider range of developers needs to maintain the build pipeline and iterate quickly

Current recommendation:

- treat Python as the likely better draft and long-term build language unless there is a strong reason to keep the existing Swift work
- keep the Swift prototype as a reference until feature parity is reached

## Implementation Phases

### Phase 1: Discovery And Specification

- synthesize email discussion
- inspect source material and extraction status
- define MVP site map and content policy

### Phase 2: Data Pipeline

- finalize source ingestion
- produce normalized structured export
- support CSV import for draft testing
- optionally implement high-confidence email redaction
- define editorial override mechanism

### Phase 3: Static Site MVP

- landing page
- thread index
- optional original-order index, controlled by config
- thread pages
- permalinks
- inline post expansion
- responsive styling
- draft topic curation page

### Phase 4: Research And Preservation Enhancements

- downloadable CSV
- alternate archival ordering
- semantic topic index
- archival outreach package

## Open Questions

- What exact domain and redirect strategy will be used for launch?
- Which additional legacy source directories beyond `discuss` and `discuss2` should be included in the first draft import pass?

## Working Recommendation

Build the next draft iteration as a static local archive with these page types:

- interpretive landing page
- activity-sorted thread index
- optional original-order thread index
- individual thread pages with inline expansion and permalinks
- AI-assisted topic curation page

Keep the draft focused. Preserve authenticity, avoid uneven transformations, make the archive easy to read, and keep hosting, chrome restoration, and institutional handoff as later layers rather than blocking draft development.
