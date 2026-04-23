#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import math
import re
import signal
import socket
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib import error, request


POST_DATE_FORMAT = "%a %b %d %H:%M:%S %Z %Y"
THREAD_BATCH_SIZE = 25
EMAIL_REGEX = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
TOKEN_REGEX = re.compile(r"[A-Za-z0-9']+")
SENTENCE_SPLIT_REGEX = re.compile(r"(?<=[.!?])\s+|\n{2,}")
DIRECT_ADDRESS_REGEX = re.compile(r'^\s*"?([A-Z][A-Za-z0-9"\'&.-]*(?:\s+[A-Z][A-Za-z0-9"\'&.-]*){0,3})\s*,')
CAPITALIZED_SEQUENCE_REGEX = re.compile(
    r"\b(?:[A-Z][A-Za-z0-9'&.-]*|[A-Z]{2,})(?:\s+(?:[A-Z][A-Za-z0-9'&.-]*|[A-Z]{2,}|of|the|and|for|to)){0,4}\b"
)
QUOTED_TITLE_REGEX = re.compile(r'"([^"\n]{3,80})"')
ACRONYM_REGEX = re.compile(r"\b[A-Z]{2,6}\b")
WORLD_WAR_REGEX = re.compile(r"\b(?:World\s+War\s+[IVX0-9]+|Vietnam\s+War|Korean\s+War)\b", re.IGNORECASE)
HONORIFIC_PREFIX_REGEX = re.compile(r"^(Dr|Mr|Mrs|Ms|Rev|Gen|General|Capt|Captain|Lt|Colonel|Col|Sgt|Sergeant)\.?\s+", re.IGNORECASE)
STOPWORDS = {
    "a",
    "about",
    "after",
    "all",
    "also",
    "am",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "but",
    "by",
    "can",
    "could",
    "comments",
    "did",
    "didn",
    "didnt",
    "do",
    "does",
    "don",
    "dont",
    "for",
    "from",
    "get",
    "got",
    "had",
    "has",
    "have",
    "he",
    "her",
    "here",
    "him",
    "his",
    "how",
    "i",
    "iam",
    "im",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "i'm",
    "just",
    "like",
    "me",
    "more",
    "my",
    "no",
    "not",
    "note",
    "notes",
    "of",
    "on",
    "one",
    "or",
    "our",
    "out",
    "please",
    "post",
    "posts",
    "re",
    "really",
    "reply",
    "replies",
    "she",
    "so",
    "some",
    "site",
    "such",
    "than",
    "that",
    "thats",
    "the",
    "their",
    "them",
    "there",
    "they",
    "this",
    "thread",
    "threads",
    "thoughts",
    "to",
    "too",
    "us",
    "very",
    "was",
    "wasn",
    "we",
    "were",
    "weren",
    "what",
    "when",
    "which",
    "who",
    "why",
    "will",
    "with",
    "won",
    "wont",
    "would",
    "you",
    "your",
}
MENTION_SINGLE_WORD_BLOCKLIST = {
    "A",
    "An",
    "And",
    "April",
    "Are",
    "As",
    "August",
    "But",
    "Comments",
    "Correction",
    "December",
    "February",
    "For",
    "Friday",
    "From",
    "Hello",
    "He",
    "I",
    "January",
    "July",
    "June",
    "March",
    "May",
    "Monday",
    "My",
    "November",
    "October",
    "Others",
    "PBS",
    "Please",
    "POV",
    "PTSD",
    "Question",
    "Response",
    "Saturday",
    "September",
    "She",
    "Sunday",
    "Thanks",
    "The",
    "Then",
    "There",
    "Thursday",
    "Tuesday",
    "Vietnam",
    "Wednesday",
    "What",
    "When",
    "Where",
    "Why",
    "You",
}
MENTION_CONNECTORS = {"of", "the", "and", "for", "to"}
ENTITY_HINT_WORDS = {
    "Army",
    "Battalion",
    "Book",
    "Books",
    "Brigade",
    "Cavalry",
    "CNN",
    "Company",
    "Corps",
    "Division",
    "Film",
    "Infantry",
    "Marines",
    "Marine",
    "Movie",
    "Navy",
    "PBS",
    "Platoon",
    "POV",
    "Regiment",
    "Show",
    "States",
    "Union",
    "War",
}
GENERIC_ENTITY_WORDS = {
    "american",
    "christian",
    "family",
    "freedom",
    "government",
    "life",
    "nam",
    "north",
    "organization",
    "republic",
    "site",
    "soldier",
    "soldiers",
    "south",
    "states",
    "veteran",
    "veterans",
    "vet",
    "vets",
    "viet",
    "vietnam",
    "vietnamese",
}
GENERIC_MENTION_PHRASES = {
    "american soldier",
    "american soldiers",
    "christian organization",
    "south vietnamese",
    "viet nam vet",
    "viet nam vets",
    "vietnam veterans",
}
ALLOWED_ACRONYM_MENTIONS = {
    "ABC",
    "ARVN",
    "CBS",
    "CIA",
    "CNN",
    "MIA",
    "NBC",
    "NVA",
    "PBS",
    "POV",
    "POW",
    "USAF",
    "USMC",
    "VA",
    "VC",
}
MENTION_REVIEW_PROMPT = """You review candidate entity mentions from a Vietnam War discussion post.

Return JSON only in the form {"mentions":["..."]}.

Rules:
- Keep only distinct, widely known real-world entities explicitly referenced in the text.
- Allowed categories: public figures, organizations, military units, wars, historical events, TV shows, films, books.
- Use the common canonical name in mixed case unless the entity is normally written as an acronym.
- Collapse duplicate variants into one canonical mention.
- Exclude forum participants, usernames, vague groups, generic concepts, diagnoses, abstractions, and uncertain items.
- If confidence is not high, omit the item.
- If nothing qualifies, return {"mentions":[]}.
"""


@dataclass
class Post:
    post_id: int
    created_at: datetime
    created_at_display: str
    post_title: str
    post_body: str
    email_string: str
    name_string: str
    directory: str
    thread_id: int


@dataclass
class Thread:
    index_id: int
    thread_title: str
    directory: str
    posts: list[Post]
    original_position: int
    thread_key: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a local static draft site for Vietnam Stories.")
    parser.add_argument("--source-root", type=Path, help="Legacy forum root, e.g. original_website/stories/vietnam")
    parser.add_argument("--csv-input", type=Path, help="Structured CSV input matching threads.csv export")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory to write the static site into")
    parser.add_argument("--emit-csv", type=Path, help="Optional path to emit normalized CSV")
    parser.add_argument("--emit-mentions-audit", type=Path, help="Optional path to emit mentions review audit CSV")
    parser.add_argument("--config", type=Path, default=Path("draft_site_config.json"))
    return parser.parse_args()


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_whitespace(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
    normalized = re.sub(r"\n[ \t]*\n(?:[ \t]*\n)+", "\n\n", normalized)
    return normalized.strip()


def decode_entities(text: str) -> str:
    previous = text
    current = html.unescape(text)
    while current != previous:
        previous = current
        current = html.unescape(current)
    return current


def clean_text(text: str) -> str:
    return normalize_whitespace(decode_entities(text))


def escape_text(text: str) -> str:
    return html.escape(text, quote=False)


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_REGEX.findall(text)]


def keyword_tokens(text: str) -> list[str]:
    return [
        token
        for token in tokenize(text)
        if len(token) > 2 and any(character.isalpha() for character in token) and token not in STOPWORDS
    ]


def parse_legacy_date(raw: str) -> datetime:
    cleaned = " ".join(raw.split())
    parts = cleaned.split()
    if len(parts) >= 6 and len(parts[2]) == 1:
        parts[2] = parts[2].zfill(2)
        cleaned = " ".join(parts)
    try:
        return datetime.strptime(cleaned, POST_DATE_FORMAT)
    except ValueError:
        tokens = cleaned.split()
        if len(tokens) >= 6:
            without_zone = " ".join(tokens[:-2] + tokens[-1:])
            return datetime.strptime(without_zone, "%a %b %d %H:%M:%S %Y")
        raise


def strip_tags(text: str) -> str:
    text = text.replace("<p>", "\n\n").replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    text = re.sub(r"<[^>]+>", "", text)
    return clean_text(text)


def split_sentences(text: str) -> list[str]:
    cleaned = clean_text(text)
    sentences = []
    for raw in SENTENCE_SPLIT_REGEX.split(cleaned):
        sentence = " ".join(raw.split())
        if sentence:
            sentences.append(sentence)
    return sentences


def truncate_words(text: str, word_limit: int) -> str:
    words = text.split()
    if len(words) <= word_limit:
        return text
    return " ".join(words[:word_limit]).rstrip(",;:") + "…"


def human_join(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])}, and {items[-1]}"


def contains_any(tokens: set[str], candidates: set[str]) -> bool:
    return bool(tokens & candidates)


def preferred_author_name(name: str) -> str | None:
    cleaned = clean_text(name)
    if not cleaned:
        return None
    if cleaned.lower() in {"on file", "unknown", "anonymous"}:
        return None
    return cleaned


def with_author_name(summary: str, name: str) -> str:
    author = preferred_author_name(name)
    if not author or not summary:
        return summary
    if summary[0].isupper():
        return f"{author} {summary[0].lower()}{summary[1:]}"
    return f"{author} {summary}"


def reply_clause(summary: str) -> str:
    replacements = [
        ("argues that", "arguing that"),
        ("defends", "defending"),
        ("offers", "offering"),
        ("pushes back against", "pushing back against"),
        ("describes", "describing"),
        ("reflects on", "reflecting on"),
        ("looks for", "looking for"),
        ("debates", "debating"),
        ("connects", "connecting"),
        ("comments on", "commenting on"),
        ("raises", "raising"),
        ("discusses", "discussing"),
    ]
    for source, replacement in replacements:
        if summary.startswith(source):
            return summary.replace(source, replacement, 1)
    return summary


def cleaned_direct_address(text: str, author_name: str) -> str | None:
    match = DIRECT_ADDRESS_REGEX.match(text)
    if not match:
        return None
    candidate = clean_text(match.group(1)).strip('" ')
    candidate = re.sub(r"^(To|Dear)\s+", "", candidate)
    if not candidate or candidate in MENTION_SINGLE_WORD_BLOCKLIST:
        return None
    author = preferred_author_name(author_name)
    if author and candidate.lower() == author.lower():
        return None
    return candidate


def first_nonempty_line(text: str) -> str:
    for line in decode_entities(text).splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def normalize_mention(candidate: str) -> str:
    return " ".join(candidate.replace("&amp;", "&").split()).strip(" \t\n\r.,;:()[]{}\"'")


def strip_honorific(candidate: str) -> str:
    return HONORIFIC_PREFIX_REGEX.sub("", candidate).strip()


def normalized_name_tokens(name: str) -> list[str]:
    return [token.lower() for token in TOKEN_REGEX.findall(clean_text(name))]


def build_participant_forms(threads: list[Thread]) -> list[tuple[str, ...]]:
    forms: set[tuple[str, ...]] = set()
    for thread in threads:
        for post in thread.posts:
            tokens = normalized_name_tokens(post.name_string)
            if tokens:
                forms.add(tuple(tokens))
    return sorted(forms)


def matches_participant(candidate: str, participant_forms: list[tuple[str, ...]]) -> bool:
    candidate_tokens = tuple(token.lower() for token in TOKEN_REGEX.findall(candidate))
    if not candidate_tokens:
        return False
    for form in participant_forms:
        if len(candidate_tokens) > len(form):
            continue
        for start in range(len(form) - len(candidate_tokens) + 1):
            if form[start : start + len(candidate_tokens)] == candidate_tokens:
                return True
    return False


def is_mixed_case_entity(candidate: str) -> bool:
    letters = [character for character in candidate if character.isalpha()]
    if not letters:
        return False
    return any(character.islower() for character in letters) and any(character.isupper() for character in letters)


def looks_like_mention(candidate: str, author_name: str, participant_forms: list[tuple[str, ...]]) -> bool:
    candidate = normalize_mention(candidate)
    if len(candidate) < 2:
        return False
    author = preferred_author_name(author_name)
    if author and candidate.lower() == author.lower():
        return False
    if matches_participant(candidate, participant_forms):
        return False
    words = candidate.split()
    if words[0].lower() in MENTION_CONNECTORS or words[-1].lower() in MENTION_CONNECTORS:
        return False
    if candidate.lower() in GENERIC_MENTION_PHRASES:
        return False
    if len(words) == 1 and candidate in MENTION_SINGLE_WORD_BLOCKLIST:
        return False
    if len(words) == 1 and candidate.upper() != candidate and len(candidate) < 5:
        return False
    if all(word.lower() in {"of", "the", "and", "for", "to"} for word in words):
        return False
    if candidate.upper() == candidate and candidate not in ALLOWED_ACRONYM_MENTIONS:
        return False
    if candidate.upper() != candidate and not is_mixed_case_entity(candidate):
        return False
    return any(character.isalpha() for character in candidate)


def extract_mention_candidates(post: Post) -> list[str]:
    text = decode_entities("\n".join([post.post_title, post.post_body]))
    candidates: list[str] = []

    for match in WORLD_WAR_REGEX.finditer(text):
        candidates.append(normalize_mention(match.group(0)))

    for match in QUOTED_TITLE_REGEX.finditer(text):
        candidate = normalize_mention(match.group(1))
        if 2 <= len(candidate.split()) <= 6:
            candidates.append(candidate)

    for match in ACRONYM_REGEX.finditer(text):
        candidate = normalize_mention(match.group(0))
        if candidate in ALLOWED_ACRONYM_MENTIONS:
            candidates.append(candidate)

    for match in CAPITALIZED_SEQUENCE_REGEX.finditer(text):
        candidate = normalize_mention(match.group(0))
        words = candidate.split()
        significant_words = [word for word in words if word.lower() not in MENTION_CONNECTORS]
        if len(significant_words) < 2 and not any(word in ENTITY_HINT_WORDS for word in words):
            continue
        candidates.append(candidate)

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped[:18]


def review_mentions(candidates: list[str], participant_forms: list[tuple[str, ...]]) -> list[str]:
    normalized = [normalize_mention(candidate) for candidate in candidates if candidate]
    normalized = [candidate for candidate in normalized if looks_like_mention(candidate, "", participant_forms)]

    by_key: dict[str, str] = {}
    full_name_by_last: dict[str, str] = {}
    for candidate in normalized:
        stripped = strip_honorific(candidate)
        words = stripped.split()
        if len(words) >= 2 and is_mixed_case_entity(stripped):
            full_name_by_last[words[-1].lower()] = stripped

    reviewed: list[str] = []
    seen: set[str] = set()
    for candidate in normalized:
        stripped = strip_honorific(candidate)
        words = stripped.split()
        if len(words) == 2 and words[0].endswith(".") and words[1][0].isupper():
            stripped = full_name_by_last.get(words[-1].lower(), stripped)
        elif len(words) == 1 and words[0][0].isupper():
            stripped = full_name_by_last.get(words[0].lower(), stripped)

        key = stripped.lower()
        existing = by_key.get(key)
        preferred = stripped
        if existing:
            if is_mixed_case_entity(existing):
                preferred = existing
            elif is_mixed_case_entity(stripped):
                preferred = stripped
            else:
                preferred = existing
            by_key[key] = preferred
            continue
        by_key[key] = preferred

    for candidate in by_key.values():
        if candidate.lower() in seen:
            continue
        seen.add(candidate.lower())
        reviewed.append(candidate)
    return reviewed[:6]


def mentions_review_cache_key(post: Post, model: str, candidates: list[str]) -> str:
    payload = json.dumps(
        {
            "model": model,
            "post_title": post.post_title,
            "post_body": post.post_body,
            "candidates": candidates,
        },
        ensure_ascii=True,
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_mentions_review_cache(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_mentions_review_cache(path: Path, cache: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def cache_entry_mentions(entry: object) -> tuple[list[str], list[str], str]:
    if isinstance(entry, dict):
        llm_mentions = [str(item) for item in entry.get("llm_mentions", []) if isinstance(item, str)]
        final_mentions = [str(item) for item in entry.get("final_mentions", []) if isinstance(item, str)]
        review_mode = str(entry.get("review_mode", "llm_cache"))
        return llm_mentions, final_mentions, review_mode
    if isinstance(entry, list):
        legacy_mentions = [str(item) for item in entry if isinstance(item, str)]
        return legacy_mentions, legacy_mentions, "llm_cache_legacy"
    return [], [], "llm_cache_invalid"


def ollama_review_mentions(
    post: Post,
    candidates: list[str],
    settings: dict,
    participant_forms: list[tuple[str, ...]],
) -> tuple[list[str], list[str], str]:
    endpoint = settings["endpoint"]
    payload = {
        "model": settings["model"],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0},
        "messages": [
            {"role": "system", "content": MENTION_REVIEW_PROMPT},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "post_title": post.post_title,
                        "post_body": post.post_body,
                        "candidates": candidates,
                    },
                    ensure_ascii=False,
                ),
            },
        ],
    }
    request_body = json.dumps(payload).encode("utf-8")
    req = request.Request(endpoint, data=request_body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=settings.get("timeout_seconds", 90)) as response:
            raw = json.loads(response.read().decode("utf-8"))
    except (error.URLError, TimeoutError, socket.timeout, json.JSONDecodeError):
        fallback = review_mentions(candidates, participant_forms)
        return [], fallback, "llm_error_fallback"

    content = raw.get("message", {}).get("content", "")
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        fallback = review_mentions(candidates, participant_forms)
        return [], fallback, "llm_parse_fallback"

    mentions = parsed.get("mentions", [])
    if not isinstance(mentions, list):
        fallback = review_mentions(candidates, participant_forms)
        return [], fallback, "llm_schema_fallback"
    llm_mentions = [str(item) for item in mentions if isinstance(item, str)]
    return llm_mentions, review_mentions(llm_mentions, participant_forms), "llm_live"


def extract_mentions(post: Post, participant_forms: list[tuple[str, ...]]) -> list[str]:
    text = decode_entities(post.post_body)
    candidates: list[str] = []

    for match in WORLD_WAR_REGEX.finditer(text):
        candidate = normalize_mention(match.group(0))
        if looks_like_mention(candidate, post.name_string, participant_forms):
            candidates.append(candidate)

    for match in QUOTED_TITLE_REGEX.finditer(text):
        candidate = normalize_mention(match.group(1))
        words = candidate.split()
        if 2 <= len(words) <= 6 and all(
            word in MENTION_CONNECTORS or word[:1].isupper() for word in words
        ) and looks_like_mention(candidate, post.name_string, participant_forms):
            candidates.append(candidate)

    for match in ACRONYM_REGEX.finditer(text):
        candidate = normalize_mention(match.group(0))
        if looks_like_mention(candidate, post.name_string, participant_forms) and candidate not in {"I", "PS"}:
            candidates.append(candidate)

    for match in CAPITALIZED_SEQUENCE_REGEX.finditer(text):
        candidate = normalize_mention(match.group(0))
        words = candidate.split()
        significant_words = [word for word in words if word.lower() not in MENTION_CONNECTORS]
        if len(significant_words) < 2 and not any(word in ENTITY_HINT_WORDS for word in words):
            continue
        if any(word in MENTION_SINGLE_WORD_BLOCKLIST for word in words):
            continue
        if all(word.lower() in GENERIC_ENTITY_WORDS for word in significant_words):
            continue
        if looks_like_mention(candidate, post.name_string, participant_forms):
            candidates.append(candidate)
    return review_mentions(candidates, participant_forms)


def specific_elements(primary_tokens: set[str], body_tokens: set[str]) -> list[str]:
    combined = primary_tokens | body_tokens
    details: list[str] = []

    def maybe_add(label: str, candidates: set[str], minimum_hits: int = 1) -> None:
        if label not in details and len(combined & candidates) >= minimum_hits:
            details.append(label)

    maybe_add("South Vietnamese civilians", {"south", "vietnamese"})
    maybe_add("former ARVN soldiers", {"arvn"})
    maybe_add("family life", {"family", "son", "daughter", "child", "children", "mother", "father", "dad", "parents"})
    maybe_add("haunting memories", {"memory", "memories", "haunt", "haunting", "nightmare", "nightmares", "ptsd"})
    maybe_add("lost friends", {"friend", "friends"})
    maybe_add("combat bonds", {"combat", "brothers", "brother", "trust", "love"})
    maybe_add("Veterans Day", {"veterans", "day"}, minimum_hits=2)
    maybe_add("freedom of expression", {"freedom", "expression", "rights", "right"})
    maybe_add("the site's purpose", {"site", "website", "page", "discussion"})
    maybe_add("Christian conversion", {"christian", "conversion"})
    maybe_add("deliverance language", {"deliverance", "demons", "prophet"})
    maybe_add("the Wall", {"wall", "memorial"})
    maybe_add("names of the dead", {"names", "remembrance", "honor", "honour"})
    maybe_add("anti-war protest", {"protest", "protests", "protestor", "protestors"})
    maybe_add("government power", {"government", "politics", "political", "country"})
    maybe_add("military duty", {"duty", "oath", "service", "served", "charge"})
    maybe_add("healing after the war", {"heal", "healing", "peace", "forgive", "forgiveness"})
    maybe_add("PBS", {"pbs"})
    return details[:2]


def append_specifics(summary: str, primary_tokens: set[str], body_tokens: set[str], post_id: int) -> str:
    details = specific_elements(primary_tokens, body_tokens)
    if not details:
        return summary
    templates = [
        "through {details}.",
        "especially around {details}.",
        "in its focus on {details}.",
        "as it turns to {details}.",
    ]
    template = templates[post_id % len(templates)]
    return summary.rstrip(".") + " " + template.format(details=human_join(details))


def build_post_mentions_map(
    threads: list[Thread], config: dict
) -> tuple[dict[tuple[str, int], list[str]], list[dict[str, str]]]:
    mention_map: dict[tuple[str, int], list[str]] = {}
    audit_rows: list[dict[str, str]] = []
    participant_forms = build_participant_forms(threads)
    review_settings = config.get("mentions_llm_review", {})
    review_enabled = bool(review_settings.get("enabled"))
    max_new_reviews = review_settings.get("max_new_reviews_per_run")
    cache_path = Path(review_settings.get("cache_path", "build/mentions_llm_review_cache.json"))
    review_cache = load_mentions_review_cache(cache_path) if review_enabled else {}
    cache_dirty = False
    new_reviews = 0
    for thread in threads:
        for post in thread.posts:
            key = (thread.thread_key, post.post_id)
            raw_candidates: list[str] = []
            llm_mentions: list[str] = []
            final_mentions: list[str] = []
            review_mode = "heuristic_no_candidates"
            if review_enabled:
                raw_candidates = extract_mention_candidates(post)
                if raw_candidates:
                    cache_key = mentions_review_cache_key(post, review_settings["model"], raw_candidates)
                    cached_entry = review_cache.get(cache_key)
                    if cached_entry is None:
                        if max_new_reviews is None or new_reviews < int(max_new_reviews):
                            llm_mentions, final_mentions, review_mode = ollama_review_mentions(
                                post, raw_candidates, review_settings, participant_forms
                            )
                            review_cache[cache_key] = {
                                "llm_mentions": llm_mentions,
                                "final_mentions": final_mentions,
                                "review_mode": review_mode,
                            }
                            cache_dirty = True
                            new_reviews += 1
                        else:
                            final_mentions = extract_mentions(post, participant_forms)
                            review_mode = "heuristic_review_cap"
                    else:
                        llm_mentions, final_mentions, review_mode = cache_entry_mentions(cached_entry)
                    mention_map[key] = final_mentions
                else:
                    final_mentions = extract_mentions(post, participant_forms)
                    mention_map[key] = final_mentions
            else:
                raw_candidates = extract_mention_candidates(post)
                final_mentions = extract_mentions(post, participant_forms)
                mention_map[key] = final_mentions
                if raw_candidates:
                    review_mode = "heuristic_only"
            if raw_candidates or final_mentions or llm_mentions:
                audit_rows.append(
                    {
                        "directory": thread.directory,
                        "thread_key": thread.thread_key,
                        "thread_index_id": str(thread.index_id),
                        "thread_title": thread.thread_title,
                        "post_id": str(post.post_id),
                        "post_created_at": post.created_at.isoformat(),
                        "post_title": post.post_title,
                        "post_author": post.name_string,
                        "review_mode": review_mode,
                        "raw_candidates": " | ".join(raw_candidates),
                        "llm_mentions": " | ".join(llm_mentions),
                        "final_mentions": " | ".join(final_mentions),
                    }
                )
    if review_enabled and cache_dirty:
        save_mentions_review_cache(cache_path, review_cache)
    return mention_map, audit_rows


def theme_match_score(post: Post, keywords: list[str]) -> int:
    title = post.post_title.lower()
    body = post.post_body.lower()
    combined = "\n".join([title, body])
    matched_keywords = 0
    score = 0
    for keyword in keywords:
        title_hits = title.count(keyword)
        body_hits = body.count(keyword)
        if not title_hits and not body_hits:
            continue
        matched_keywords += 1
        score += title_hits * 6
        score += min(body_hits, 3) * 2
        if " " in keyword:
            score += 2
    if matched_keywords >= 2:
        score += matched_keywords * 2
    if any(keyword in combined for keyword in keywords[:1]):
        score += 1
    return score


def descriptive_summary(
    thread: Thread,
    post: Post,
    lead_sentence: str,
    lead_tokens: set[str],
    context_token_set: set[str],
    title_tokens: set[str],
) -> str:
    primary_tokens = set(lead_tokens) | set(title_tokens)
    body_tokens = set(keyword_tokens(post.post_body))
    combined = primary_tokens | body_tokens
    lead_lower = lead_sentence.lower()
    summary = "discusses one aspect of how the war continues to be remembered and debated"
    reply_markers = {"response", "reply", "correction", "reconsider", "agree", "disagree", "apology"}
    reply_target = cleaned_direct_address(first_nonempty_line(post.post_body), post.name_string)
    is_reply = contains_any(primary_tokens | body_tokens, reply_markers) or reply_target is not None

    if contains_any(combined, {"south", "vietnamese", "arvn"}) and contains_any(
        combined, {"ignore", "ignored", "overlooked", "overlook"}
    ):
        summary = "argues that South Vietnamese soldiers and civilians are being overlooked in accounts of the war"
    elif contains_any(combined, {"freedom", "expression", "rights", "right", "dissent"}) and contains_any(
        combined, {"discussion", "site", "page", "website"}
    ):
        summary = "defends open expression and argues over how broad the discussion should be"
    elif contains_any(combined, {"christian", "religion", "religious", "deliverance", "demons", "prophet"}):
        if contains_any(body_tokens | primary_tokens, {"waste", "belong", "line", "wrong", "remove", "crud", "narrow", "mission"}):
            summary = "pushes back against religiously framed messages appearing in the discussion"
        elif contains_any(primary_tokens | body_tokens, {"help", "support", "free"}):
            summary = "offers religiously framed help and support"
    elif contains_any(combined, {"thank", "thanks", "grateful", "appreciate", "hope", "keep"}) and contains_any(
        combined, {"site", "page", "website", "pbs"}
    ):
        summary = "offers appreciation for the site and its role as a place to speak and remember"
    elif contains_any(combined, {"memory", "memories", "remember", "haunt", "haunting", "ptsd", "nightmare", "nightmares"}):
        if contains_any(combined, {"son", "daughter", "children", "child", "family", "father", "mother", "dad"}):
            summary = "connects painful war memories to family life and later relationships"
        else:
            summary = "describes how memories of the war continue to surface and shape everyday life"
    elif contains_any(combined, {"friends", "friend", "combat", "brothers", "brother", "miss", "love", "trust"}) and contains_any(
        combined, {"veteran", "veterans", "soldier", "soldiers"}
    ):
        summary = "reflects on lost friends and the bonds formed in combat"
    elif contains_any(combined, {"wall", "memorial", "names", "remembrance", "remember", "honor", "honour"}):
        summary = "reflects on remembrance, names, and the need to honor those who died"
    elif contains_any(combined, {"heal", "healing", "peace", "understand", "understanding", "forgive", "forgiveness"}):
        summary = "looks for understanding, healing, and a way to live with the past"
    elif contains_any(combined, {"protest", "protests", "protestor", "protestors", "politics", "political", "government", "country"}):
        summary = "debates the politics of the war and the conflicts it still stirs"
    elif contains_any(combined, {"service", "served", "duty", "oath", "charge", "veteran", "veterans", "soldier", "soldiers"}):
        summary = "reflects on service, duty, and what the war demanded of those who served"
    elif contains_any(combined, {"children", "child", "son", "daughter", "parents", "parent", "mother", "father", "dad", "family"}):
        summary = "connects the war's effects to family relationships across generations"
    elif contains_any(combined, {"question", "questions", "ask", "asks", "wonder", "why", "how"}):
        summary = "raises questions about the war and how it should be understood"
    elif contains_any(combined, {"site", "page", "website", "pbs"}):
        summary = "comments on the site and what kind of dialogue it should make possible"
    elif "?" in lead_sentence:
        summary = "raises questions about the war and its aftermath"
    elif re.match(r"^(i|we|my|our)\b", lead_lower):
        summary = "discusses a personal response to the war and its aftermath"

    if is_reply and reply_target:
        summary = f"replies to {reply_target} by {reply_clause(summary)}"
    elif is_reply:
        summary = f"replies by {reply_clause(summary)}"

    summary = summary[0].upper() + summary[1:] + "."
    return with_author_name(append_specifics(summary, primary_tokens, body_tokens, post.post_id), post.name_string)


def discover_forums(source_root: Path) -> list[str]:
    names = []
    for thread_index in sorted(source_root.glob("*/thread.index")):
        names.append(thread_index.parent.name)
    return names


def format_date(date: datetime) -> str:
    return f"{date.strftime('%B')} {date.day}, {date.year}"


def format_date_with_ordinal(date: datetime) -> str:
    day = date.day
    if 10 <= day % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{date.strftime('%B')} {day}{suffix}, {date.year}"


def format_full_timestamp(date: datetime) -> str:
    return date.strftime(POST_DATE_FORMAT)


def original_web_css() -> str:
    return """
body.original-web-body {
  background: #000000;
  margin: 0;
}
.original-web-page {
  padding-top: 0;
}
.original-web-content {
  font-size: 16px;
  line-height: 1.35;
  color: #ffffff;
  font-family: "Times New Roman", Times, serif;
}
.original-web-thread-content {
  margin-top: 1.35em;
}
.original-web-jump,
.original-web-batch-nav {
  font-size: 16px;
  line-height: 1.35;
}
.original-web-batch-nav {
  margin: 10px 0 22px;
  text-align: center;
}
.original-web-jump {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  margin: 16px 0 8px;
}
.original-web-content a {
  color: inherit;
  text-decoration: underline;
}
.original-web-topics a {
  color: #86aecd;
}
.original-web-topics a:visited {
  color: #3b8f8a;
}
.original-web-content h1 {
  font-size: 28px;
  font-weight: 700;
  margin: 0 0 14px;
  color: #ffffff;
}
.original-web-topics {
  margin: 0;
}
.original-web-topic-row {
  margin: 0 0 6px;
}
.original-web-topic-number,
.original-web-topic-count {
  color: #ffffff;
}
.original-web-post {
  margin: 0 0 52px;
}
.original-web-post-title {
  color: #cc0000;
  font-size: 22px;
  font-weight: 700;
  text-transform: uppercase;
  margin: 0 0 8px;
}
.original-web-post-date {
  margin: 0 0 14px;
  font-family: "Courier New", Courier, monospace;
  color: #ffffff;
}
.original-web-post-body {
  white-space: pre-wrap;
  font-size: 1.2rem;
  line-height: 1.4;
  color: #f0f0f0;
  font-weight: 400;
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
  max-width: 500pt;
}
.original-web-signature {
  margin-top: 12px;
  white-space: pre-wrap;
  font-family: "Courier New", Courier, monospace;
  color: #ffffff;
}
.original-web-post-spacer {
  height: 2.5em;
}
"""


def render_original_web_page(title: str, body: str, *, root_prefix: str) -> str:
    return render_page(
        title,
        f"""<main class="page original-web-page">
  <header class="masthead">
    {render_top_nav(
        current_utility="",
        home_href=f"{root_prefix}index.html",
        producer_href=f"{root_prefix}producer_letter.html",
        credits_href=f"{root_prefix}credits.html",
        downloads_href=f"{root_prefix}downloads.html",
    )}
    {render_site_titlebar(f"{root_prefix}index.html")}
  </header>
  {render_explore_nav(
      current_primary="original_web",
      index_href=f"{root_prefix}active_threads.html",
      original_href=f"{root_prefix}index_original.html",
      original_web_href=f"{root_prefix}original_web_design.html",
      theme_href=f"{root_prefix}topics.html",
      original_enabled=True,
      original_web_enabled=True,
      theme_enabled=True,
  )}
  <section class="original-web-content">
    {body}
  </section>
</main>""",
        extra_css=original_web_css(),
        body_class="original-web-body",
    )


def original_web_thread_page_name(thread_key: str, batch_index: int) -> str:
    return f"{thread_key}.html" if batch_index == 0 else f"{thread_key}__{batch_index + 1}.html"


def original_web_batch_label(start_index: int, batch_posts: list[Post]) -> str:
    return f"{start_index + 1} - {start_index + len(batch_posts)}"


def modern_thread_page_name(thread_key: str, batch_index: int) -> str:
    return original_web_thread_page_name(thread_key, batch_index)


def thread_batch_count(thread: Thread) -> int:
    return max(1, math.ceil(len(thread.posts) / THREAD_BATCH_SIZE))


def thread_batch_index_for_post(thread: Thread, post_id: int) -> int:
    sorted_posts = sorted(thread.posts, key=lambda item: item.post_id)
    for index, post in enumerate(sorted_posts):
        if post.post_id == post_id:
            return index // THREAD_BATCH_SIZE
    return 0


def render_original_web_batch_nav(
    *,
    thread: Thread,
    total_posts: int,
    batch_index: int,
    batch_count: int,
    root_prefix: str,
) -> str:
    items = []
    previous_count = min(THREAD_BATCH_SIZE, batch_index * THREAD_BATCH_SIZE)
    remaining_after_current = total_posts - ((batch_index + 1) * THREAD_BATCH_SIZE)
    next_count = min(THREAD_BATCH_SIZE, max(0, remaining_after_current))
    if previous_count > 0:
        previous_href = original_web_thread_page_name(thread.thread_key, batch_index - 1)
        items.append(f'<a href="{previous_href}">Previous {previous_count}</a>')
    for current_index in range(batch_count):
        start = current_index * THREAD_BATCH_SIZE
        end = min(total_posts, start + THREAD_BATCH_SIZE)
        label = f"{start + 1} - {end}"
        href = original_web_thread_page_name(thread.thread_key, current_index)
        if current_index == batch_index:
            items.append(f"<strong>{label}</strong>")
        else:
            items.append(f'<a href="{href}">{label}</a>')
    if next_count > 0:
        next_href = original_web_thread_page_name(thread.thread_key, batch_index + 1)
        items.append(f'<a href="{next_href}">Next {next_count}</a>')
    return '<div class="original-web-batch-nav">' + " | ".join(items) + "</div>"


def render_original_web_thread_batch_page(
    thread: Thread,
    *,
    batch_index: int,
    batch_posts: list[Post],
    batch_count: int,
) -> str:
    posts_markup = []
    for post in batch_posts:
        signature_parts = []
        if post.email_string.strip():
            signature_parts.append(post.email_string.strip())
        if post.name_string.strip():
            signature_parts.append(f"({post.name_string.strip()})")
        signature_text = "--" if not signature_parts else "-- " + " ".join(signature_parts)
        posts_markup.append(
            f"""<article class="original-web-post">
<h2 class="original-web-post-title" id="post-{post.post_id}">{escape_text(post.post_title.upper())}</h2>
<div class="original-web-post-date">{escape_text(post.created_at_display or format_full_timestamp(post.created_at))}</div>
<div class="original-web-post-body">{escape_text(post.post_body)}</div>
<div class="original-web-signature">{escape_text(signature_text)}</div>
<div class="original-web-post-spacer"></div>
</article>"""
        )
    nav = render_original_web_batch_nav(
        thread=thread,
        total_posts=len(thread.posts),
        batch_index=batch_index,
        batch_count=batch_count,
        root_prefix="../",
    )
    body = f"""
<div class="original-web-thread-content">
<div id="top"></div>
<h1>{escape_text(thread.thread_title)}</h1>
<div class="original-web-jump"><a href="../original_web_design.html">Back to Topics</a><a href="#bottom">Go to bottom</a></div>
{nav}
{''.join(posts_markup)}
<div id="bottom"></div>
<div class="original-web-jump"><a href="../original_web_design.html">Back to Topics</a><a href="#top">Go to top</a></div>
{nav}
</div>
"""
    return render_original_web_page(thread.thread_title, body, root_prefix="../")


def render_original_web_topics_page(ordered_threads: list[Thread]) -> str:
    rows = []
    for index, thread in enumerate(ordered_threads, start=1):
        rows.append(
            f'<div class="original-web-topic-row"><span class="original-web-topic-number">{index}. </span>'
            f'<a href="threads_original_web/{original_web_thread_page_name(thread.thread_key, 0)}">{escape_text(thread.thread_title)}</a> '
            f'<span class="original-web-topic-count">({len(thread.posts)} posts)</span></div>'
        )
    body = f"""
<h1>Topics</h1>
<section class="original-web-topics">
{''.join(rows)}
</section>
"""
    return render_original_web_page("Original Web Design", body, root_prefix="")


def format_date_range(posts: list[Post]) -> str:
    if not posts:
        return "Unknown date range"
    start = min(post.created_at for post in posts)
    end = max(post.created_at for post in posts)
    return f"{format_date(start)} to {format_date(end)}"


class ReadTimeoutError(RuntimeError):
    pass


def _alarm_handler(signum, frame):
    raise ReadTimeoutError()


def safe_read_text(path: Path, encoding: str = "latin-1", timeout_seconds: int = 2) -> str | None:
    previous_handler = signal.getsignal(signal.SIGALRM)
    try:
        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.alarm(timeout_seconds)
        return path.read_text(encoding=encoding, errors="ignore")
    except (ReadTimeoutError, TimeoutError, OSError):
        print(f"Skipping unreadable file after timeout: {path}")
        return None
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def parse_post_file(path: Path, thread_id: int, directory_name: str) -> Post | None:
    content = safe_read_text(path, encoding="latin-1")
    if content is None:
        return None

    title_match = re.search(r'<font color="#FF0000">(.*?)</font><br>', content, re.IGNORECASE | re.DOTALL)
    date_match = re.search(r"<tt>(.*?)</tt>", content, re.IGNORECASE | re.DOTALL)
    body_match = re.search(r"</tt><p>(.*?)<p><code>--", content, re.IGNORECASE | re.DOTALL)
    author_match = re.search(r"<p><code>--\s*(.*?)\((.*?)\)\s*</code>", content, re.IGNORECASE | re.DOTALL)

    if not title_match or not date_match or not body_match or not author_match:
        return None

    post_title = strip_tags(title_match.group(1))
    created_at_display = " ".join(date_match.group(1).split())
    created_at = parse_legacy_date(created_at_display)
    post_body = strip_tags(body_match.group(1))
    email_string = clean_text(author_match.group(1))
    name_string = clean_text(author_match.group(2))
    post_id = int(path.name.split(".")[-1])

    return Post(
        post_id=post_id,
        created_at=created_at,
        created_at_display=created_at_display,
        post_title=post_title,
        post_body=post_body,
        email_string=email_string,
        name_string=name_string,
        directory=directory_name,
        thread_id=thread_id,
    )


def load_threads_from_source(source_root: Path, included_forums: Iterable[str]) -> list[Thread]:
    threads: list[Thread] = []
    for forum_name in included_forums:
        forum_path = source_root / forum_name
        if not forum_path.is_dir():
            continue
        thread_index_path = forum_path / "thread.index"
        if not thread_index_path.exists():
            continue
        thread_index_text = safe_read_text(thread_index_path, encoding="latin-1")
        if thread_index_text is None:
            continue
        raw_lines = thread_index_text.splitlines()
        original_position = 0
        for raw_line in raw_lines:
            parts = raw_line.split("\t", 1)
            if len(parts) != 2:
                continue
            thread_id_text, title = parts[0].strip(), parts[1].strip()
            if not thread_id_text.isdigit() or not title:
                continue
            thread_id = int(thread_id_text)
            thread_dir = forum_path / str(thread_id)
            posts: list[Post] = []
            if thread_dir.is_dir():
                post_paths = [path for path in thread_dir.glob("post.*") if path.name.split(".")[-1].isdigit()]
                for post_path in sorted(post_paths, key=lambda item: int(item.name.split(".")[-1])):
                    post = parse_post_file(post_path, thread_id=thread_id, directory_name=forum_name)
                    if post is not None:
                        posts.append(post)
            threads.append(
                Thread(
                    index_id=thread_id,
                    thread_title=title,
                    directory=forum_name,
                    posts=posts,
                    original_position=original_position,
                    thread_key="",
                )
            )
            original_position += 1
    return assign_thread_keys(threads)


def load_threads_from_csv(csv_input: Path) -> list[Thread]:
    by_thread: dict[str, Thread] = {}
    with csv_input.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row_index, row in enumerate(reader):
            thread_id = int(row["thread_index_id"])
            directory_name = row.get("directory", "") or ""
            thread_key = row.get("thread_key", "").strip() or f"{directory_name}_{thread_id}"
            key = thread_key
            if key not in by_thread:
                by_thread[key] = Thread(
                    index_id=thread_id,
                    thread_title=row["thread_title"],
                    directory=directory_name,
                    posts=[],
                    original_position=row_index,
                    thread_key=thread_key,
                )
            post_id_text = (row.get("post_id", "") or "").strip()
            if not post_id_text:
                continue
            created_at = datetime.fromisoformat(row["post_created_at"].replace("Z", "+00:00"))
            by_thread[key].posts.append(
                Post(
                    post_id=int(post_id_text),
                    created_at=created_at,
                    created_at_display=(row.get("post_created_at_display", "") or "").strip()
                    or (row.get("post_created_at", "") or "").strip()
                    or created_at.isoformat(),
                    post_title=clean_text(row["post_title"]),
                    post_body=clean_text(row["post_body"]),
                    email_string=clean_text(row.get("post_email", "")),
                    name_string=clean_text(row.get("post_name", "")),
                    directory=directory_name,
                    thread_id=thread_id,
                )
            )
    return list(by_thread.values())


def maybe_redact_emails(text: str, mode: str) -> str:
    if mode != "high_confidence":
        return text
    return EMAIL_REGEX.sub("{email redacted}", text)


def prepare_threads(threads: list[Thread], email_redaction_mode: str) -> list[Thread]:
    prepared: list[Thread] = []
    for thread in threads:
        posts = []
        for post in sorted(thread.posts, key=lambda item: item.post_id):
            posts.append(
                Post(
                    post_id=post.post_id,
                    created_at=post.created_at,
                    created_at_display=post.created_at_display,
                    post_title=post.post_title,
                    post_body=maybe_redact_emails(post.post_body, email_redaction_mode),
                    email_string=maybe_redact_emails(post.email_string, email_redaction_mode),
                    name_string=post.name_string,
                    directory=post.directory,
                    thread_id=post.thread_id,
                )
            )
        prepared.append(
            Thread(
                    index_id=thread.index_id,
                    thread_title=thread.thread_title,
                    directory=thread.directory,
                    posts=posts,
                    original_position=thread.original_position,
                    thread_key=thread.thread_key,
                )
        )
    return prepared


def assign_thread_keys(threads: list[Thread]) -> list[Thread]:
    counts: dict[tuple[str, int], int] = {}
    for thread in threads:
        key = (thread.directory, thread.index_id)
        counts[key] = counts.get(key, 0) + 1

    seen: dict[tuple[str, int], int] = {}
    assigned: list[Thread] = []
    for thread in threads:
        key = (thread.directory, thread.index_id)
        base_key = f"{thread.directory}_{thread.index_id}"
        occurrence = seen.get(key, 0) + 1
        seen[key] = occurrence
        thread_key = base_key if counts[key] == 1 else f"{base_key}_{occurrence}"
        assigned.append(
            Thread(
                index_id=thread.index_id,
                thread_title=thread.thread_title,
                directory=thread.directory,
                posts=thread.posts,
                original_position=thread.original_position,
                thread_key=thread_key,
            )
        )
    return assigned


def activity_order(threads: list[Thread]) -> list[Thread]:
    if not threads:
        return []
    pinned = None
    for thread in threads:
        if thread.index_id == 1:
            pinned = thread
            break
    rest = [thread for thread in threads if pinned is None or thread.index_id != pinned.index_id]
    rest.sort(key=lambda thread: (-len(thread.posts), thread.original_position, thread.index_id))
    return [pinned] + rest if pinned else rest


def original_order(threads: list[Thread]) -> list[Thread]:
    return sorted(threads, key=lambda thread: (thread.directory, thread.original_position, thread.index_id))


def preview_post(text: str, word_limit: int) -> tuple[str, bool]:
    words = text.split()
    if len(words) <= word_limit:
        return text, False
    preview = " ".join(words[:word_limit]).strip()
    return preview + " ...", True


def summarize_post(thread: Thread, post: Post, sorted_posts: list[Post], post_index: int) -> str:
    title_tokens = set(keyword_tokens(post.post_title))
    previous_post = sorted_posts[post_index - 1] if post_index > 0 else None
    next_post = sorted_posts[post_index + 1] if post_index + 1 < len(sorted_posts) else None

    context_fragments = [thread.thread_title]
    if previous_post is not None:
        context_fragments.append(previous_post.post_title)
    if next_post is not None:
        context_fragments.append(next_post.post_title)
    if previous_post is None and next_post is None:
        for sibling in sorted_posts[:3]:
            if sibling.post_id == post.post_id:
                continue
            context_fragments.append(sibling.post_title)

    context_counts = Counter(keyword_tokens(" ".join(context_fragments)))
    context_terms = [token for token, _count in context_counts.most_common(6)]
    context_token_set = set(context_terms)

    sentences = split_sentences(post.post_body)
    candidate_sentences = [sentence for sentence in sentences if len(sentence.split()) >= 5]
    if candidate_sentences:
        sentences = candidate_sentences
    elif not sentences and post.post_title:
        sentences = [post.post_title]
    if not sentences:
        fallback = thread.thread_title or "Untitled post"
        return truncate_words(fallback, 20)

    ranked_sentences = []
    for position, sentence in enumerate(sentences):
        tokens = set(keyword_tokens(sentence))
        if not tokens:
            continue
        word_count = len(sentence.split())
        score = 0
        if position == 0:
            score += 4
        if 7 <= word_count <= 32:
            score += 3
        elif word_count < 4:
            score -= 2
        score += min(len(tokens & title_tokens) * 3, 9)
        score += min(len(tokens & context_token_set) * 2, 6)
        if "@" in sentence:
            score -= 2
        if sentence.lower().startswith(("dear ", "hello ", "hi ")):
            score -= 1
        ranked_sentences.append((score, position, sentence, tokens))

    if not ranked_sentences:
        return descriptive_summary(thread, post, sentences[0], set(keyword_tokens(sentences[0])), context_token_set, title_tokens)

    ranked_sentences.sort(key=lambda item: (-item[0], item[1]))
    lead_sentence = ranked_sentences[0][2]
    lead_tokens = ranked_sentences[0][3]
    return descriptive_summary(thread, post, lead_sentence, lead_tokens, context_token_set, title_tokens)


def build_post_summary_map(threads: list[Thread]) -> dict[tuple[str, int], str]:
    summary_map: dict[tuple[str, int], str] = {}
    for thread in threads:
        sorted_posts = sorted(thread.posts, key=lambda item: item.post_id)
        for index, post in enumerate(sorted_posts):
            summary_map[(thread.thread_key, post.post_id)] = summarize_post(thread, post, sorted_posts, index)
    return summary_map


def base_css() -> str:
    return """
:root {
  --bg: #111111;
  --surface: #1a1a1a;
  --surface-alt: #202020;
  --text: #f2efe8;
  --muted: #c2b7a3;
  --rule: #5e5546;
  --accent: #d0b16c;
  --link: #8db7d9;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: linear-gradient(180deg, #0b0b0b 0%, #171717 100%);
  color: var(--text);
  font-family: "Avenir Next", "Helvetica Neue", Arial, sans-serif;
  line-height: 1.6;
}
a {
  color: inherit;
  text-decoration: underline;
  text-decoration-thickness: 1px;
  text-underline-offset: 0.14em;
  text-decoration-skip-ink: none;
}
.page {
  width: min(920px, calc(100vw - 32px));
  margin: 0 auto;
  padding: 32px 0 64px;
}
.masthead {
  border-bottom: 1px solid var(--rule);
  margin-bottom: 24px;
  padding-bottom: 16px;
}
.top-nav {
  display: flex;
  justify-content: flex-start;
  align-items: flex-start;
  gap: 20px;
  flex-wrap: wrap;
  margin-bottom: 18px;
}
.top-nav-group {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
}
.site-titlebar {
  display: flex;
  align-items: flex-end;
  gap: 18px;
  flex-wrap: wrap;
  margin-bottom: 18px;
}
.site-titlebar-link {
  color: inherit;
  text-decoration: none;
}
.site-titlebar-mark {
  color: #2a6a99;
  font-size: clamp(3.5rem, 8vw, 6.75rem);
  font-weight: 700;
  letter-spacing: -0.07em;
  line-height: 0.84;
  text-transform: uppercase;
}
.site-titlebar-copy {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding-bottom: 0.38rem;
  text-transform: uppercase;
}
.site-titlebar-series {
  color: #d8d3c8;
  font-size: clamp(1.5rem, 3.4vw, 3rem);
  font-weight: 500;
  letter-spacing: 0.04em;
  line-height: 0.95;
}
.site-titlebar-subtitle {
  color: var(--muted);
  font-size: clamp(0.95rem, 2.1vw, 1.7rem);
  letter-spacing: 0.07em;
  line-height: 0.95;
}
.eyebrow {
  color: var(--accent);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  font-size: 0.78rem;
}
h1, h2, h3 {
  font-weight: normal;
  line-height: 1.1;
}
.thread-heading {
  display: grid;
  grid-template-columns: 1fr;
  gap: 8px;
  align-items: start;
}
.thread-heading h3 {
  margin: 0;
  justify-self: start;
  text-align: left;
}
.thread-heading-by {
  color: var(--muted);
  justify-self: start;
  text-align: left;
  font-size: 0.92rem;
  line-height: 1.2;
}
.lede, .muted {
  color: var(--muted);
}
.nav {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  margin-top: 16px;
}
.thread-batch-nav {
  text-align: center;
  margin: 0 0 20px;
}
.thread-batch-nav.bottom {
  margin: 20px 0 0;
}
.explore-nav {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
  margin-top: 18px;
  text-align: center;
}
.explore-nav-prefix {
  color: var(--muted);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  font-size: 0.78rem;
}
.explore-nav-choices {
  display: flex;
  justify-content: center;
  gap: 16px;
  flex-wrap: wrap;
}
.page-intro {
  margin-top: 20px;
  margin-bottom: 24px;
}
.rich-copy {
  display: grid;
  gap: 18px;
}
.rich-copy p {
  margin: 0;
}
.rich-copy p.lead-paragraph {
  font-size: 1.18rem;
  line-height: 1.7;
  color: #d8d3c8;
}
.callout-links {
  display: flex;
  gap: 18px;
  flex-wrap: wrap;
  margin-top: 8px;
}
.signature {
  margin-top: 10px;
  color: #d8d3c8;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}
.credits-block + .credits-block {
  margin-top: 28px;
}
.credits-block h2 {
  margin: 0 0 12px;
}
.credits-list {
  margin: 0;
  padding: 0;
  list-style: none;
  display: grid;
  gap: 8px;
}
.credits-pair {
  display: grid;
  grid-template-columns: minmax(180px, 240px) minmax(0, 1fr);
  gap: 16px;
}
.thread-list {
  display: grid;
  gap: 24px;
}
.thread-card, .topic-card, .post-card {
  background: rgba(255, 255, 255, 0.06);
  border: 1px solid rgba(255, 255, 255, 0.08);
  padding: 16px;
}
.topic-match-list {
  display: grid;
  gap: 10px;
  margin-top: 16px;
}
.topic-match-row {
  border-top: 1px solid rgba(255, 255, 255, 0.08);
  padding-top: 10px;
}
.topic-subtopic-title {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: baseline;
  flex-wrap: wrap;
}
.topic-subtopic-summary {
  color: var(--muted);
  margin-top: 6px;
}
.topic-thread-group {
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid rgba(255, 255, 255, 0.08);
}
.topic-badge {
  display: inline-block;
  margin-bottom: 8px;
  color: var(--accent);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  font-size: 0.78rem;
}
.post-card-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 340px;
  gap: 20px;
}
.post-side {
  border-left: 1px solid rgba(255, 255, 255, 0.08);
  padding-left: 20px;
}
.post-side-section + .post-side-section {
  margin-top: 18px;
  padding-top: 18px;
  border-top: 1px solid rgba(255, 255, 255, 0.08);
}
.post-side-title {
  margin: 0 0 10px;
  color: var(--muted);
  font-size: 1rem;
  font-weight: 600;
  letter-spacing: 0.05em;
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: baseline;
}
.post-side-label {
  text-transform: uppercase;
}
.post-side-see-all {
  margin-top: 10px;
  text-align: right;
  font-size: 0.78rem;
}
.post-summary {
  color: var(--muted);
  font-size: 0.88rem;
  font-weight: 400;
  line-height: 1.5;
}
.post-side-inline {
  color: var(--muted);
  font-size: 0.86rem;
  line-height: 1.5;
}
.post-side-list {
  margin: 0;
  padding: 0;
  list-style: none;
  display: grid;
  gap: 8px;
  font-size: 0.88rem;
  font-weight: 400;
  color: var(--muted);
}
.post-side-list li {
  margin: 0;
}
.post-side-list a {
  color: var(--muted);
}
.post-side-list li.current-theme {
  color: var(--text);
  font-weight: 400;
}
.post-side-list li.current-theme::before {
  content: "› ";
  color: var(--text);
}
.post-side-empty {
  color: var(--muted);
  font-size: 0.95rem;
}
.post-actions {
  margin-top: 2px;
}
.post-actions a {
  color: var(--muted);
  font-size: 0.82rem;
  font-weight: 400;
}
.post-card:nth-child(even) {
  background: rgba(255, 255, 255, 0.05);
}
.post-meta {
  color: var(--muted);
  font-size: 0.92rem;
  margin-top: 8px;
}
.post-body {
  white-space: pre-wrap;
  margin-top: 12px;
}
button.toggle {
  margin-top: 12px;
  background: transparent;
  color: var(--accent);
  border: 1px solid var(--rule);
  padding: 8px 10px;
  cursor: pointer;
}
ul.flat {
  padding-left: 18px;
}
@media (max-width: 920px) {
  .top-nav {
    flex-direction: column;
    align-items: stretch;
  }
  .site-titlebar {
    gap: 12px;
  }
  .site-titlebar-copy {
    padding-bottom: 0;
  }
  .explore-nav {
    flex-direction: column;
    gap: 10px;
  }
  .explore-nav-choices {
    justify-content: center;
  }
  .credits-pair {
    grid-template-columns: 1fr;
    gap: 4px;
  }
  .post-card-layout {
    grid-template-columns: 1fr;
  }
  .post-side {
    border-left: 0;
    border-top: 1px solid rgba(255, 255, 255, 0.08);
    padding-left: 0;
    padding-top: 16px;
  }
}
"""


def base_script() -> str:
    return """
document.addEventListener("click", (event) => {
  const button = event.target.closest("[data-expand-target]");
  if (!button) return;
  const targetId = button.getAttribute("data-expand-target");
  const preview = document.getElementById(targetId + "-preview");
  const full = document.getElementById(targetId + "-full");
  if (!preview || !full) return;
  const isHidden = full.hidden;
  full.hidden = !isHidden;
  preview.hidden = isHidden;
  button.textContent = isHidden ? "Show Less" : "Show All";
});
"""


def render_page(
    title: str,
    body: str,
    *,
    extra_css: str = "",
    extra_script: str = "",
    body_class: str = "",
) -> str:
    body_attr = f' class="{body_class}"' if body_class else ""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title, quote=False)}</title>
  <style>{base_css()}{extra_css}</style>
</head>
<body{body_attr}>
{body}
<script>{base_script()}{extra_script}</script>
</body>
</html>
"""


def render_top_nav(
    *,
    current_utility: str,
    home_href: str,
    producer_href: str,
    credits_href: str,
    downloads_href: str,
) -> str:
    utility = []
    for key, label, href in (
        ("home", "Home", home_href),
        ("producer", "Producer Letter", producer_href),
        ("credits", "Credits", credits_href),
        ("downloads", "Downloads", downloads_href),
    ):
        if key == current_utility:
            utility.append(f"<strong>{label}</strong>")
        else:
            utility.append(f'<a href="{href}">{label}</a>')
    return '<div class="top-nav"><nav class="top-nav-group">' + "".join(utility) + "</nav></div>"


def render_explore_nav(
    *,
    current_primary: str,
    index_href: str,
    original_href: str,
    original_web_href: str,
    theme_href: str,
    original_enabled: bool,
    original_web_enabled: bool,
    theme_enabled: bool,
) -> str:
    items = []
    for key, label, href, enabled in (
        ("original", "Posted Order", original_href, original_enabled),
        ("rank", "Most Active", index_href, True),
        ("original_web", "Original Web Design", original_web_href, original_web_enabled),
        ("theme", "Topics/Themes", theme_href, theme_enabled),
    ):
        if key == "theme":
            continue
        if not enabled:
            continue
        if key == current_primary:
            items.append(f"<strong>{label}</strong>")
        else:
            items.append(f'<a href="{href}">{label}</a>')
    return '<nav class="explore-nav"><span class="explore-nav-prefix">Explore by: </span><span class="explore-nav-choices">' + "".join(items) + "</span></nav>"


def render_site_titlebar(home_href: str) -> str:
    return f"""
<a class="site-titlebar-link" href="{home_href}" aria-label="Go to home page">
  <div class="site-titlebar">
    <div class="site-titlebar-mark">DIALOGUE</div>
    <div class="site-titlebar-copy">
      <div class="site-titlebar-series">RE: VIETNAM</div>
      <div class="site-titlebar-subtitle">STORIES SINCE THE WAR</div>
    </div>
  </div>
</a>
"""


def render_rich_copy(paragraphs: list[str]) -> str:
    return '<section class="rich-copy">' + "".join(f"<p>{escape_text(paragraph)}</p>" for paragraph in paragraphs) + "</section>"


PRODUCER_LETTER_PARAGRAPHS = [
    "The beginning of 1996 was an exciting time for me. As executive producer of POV I had been experimenting for a couple of years with using the internet to tie into the films we were showing, but it had all been pretty primitive. In 1995, after the introduction of the first web browsers, we created the first website for a public TV series (and probably one of the first websites for anything on television). But the site was just background information on each film, static webpages that people would read once but have little reason to come back to.",
    "An experiment in the fall of 1995 changed how I thought about what was possible to do on the internet. When we aired \"Leona's Sister Gerri,\" a powerful film about Gerri Santoro, a woman who had died in 1964 as a result of a botched illegal abortion, we invited viewers to tell us their story of a personal experience with an unwanted pregnancy. Within 24 hours of the broadcast, we had received more than 1100 stories, not talking points in the debate about abortion, but a cross-section of first-person accounts: the choices each person faced, the values that shaped their decisions, and the consequences of those decisions.",
    "It was a revelation and an inspiration. Why not take advantage of web technology to invite people to share their stories and engage each other in discussion in response to POV programs? Instead of the \"one to many\" experience of broadcast and traditional media, content that's created or published by a single source and then consumed by everyone out there, what would happen if we created a \"many to many\" experience where people could read what others had written, and then add their own thoughts to the mix?",
    "So at the beginning of 1996 we set out to do some experiments, creating interactive websites tied into three of the films being broadcast that season. One of the films that seemed particularly suited for an interactive website was Frieda Lee Mock's extraordinary film about Maya Lin, the designer of the Vietnam Veterans Memorial (\"The Wall\") in Washington, DC. The film tells the story of how Lin's submission to a design competition was chosen, initial opposition to it, how she defended her vision, and the enormous power of the Wall for everyone who visits, regardless of their background or political views.",
    "I had come of age during the height of the Vietnam War, and realized that the experiences of that era profoundly shaped so many things about me: my work choices, my personal relationships, my values. I realized that that was probably true for most people who lived through that time, whether they fought in the war, protested against it, lost a loved one or a friend, came to the US as refugees after the war, or any number of other ways that the war touched their lives and shaped their values.",
    "Just as the Vietnam Veterans Memorial was a place for reflection and healing, I wanted to try to create a website that would offer reflection and healing. The site had two main components: stories and dialogues.",
    "The stories were reflections on the experiences of people who had lived through the Vietnam era, not so much about what happened to them then, as how those experiences shaped who they became. We began with stories we gathered in advance of the launch of the website on the day of the broadcast, and invited people coming to the site to add their own stories, some of which were highlighted, and all of which were added to a searchable archive.",
    "The big experiment was creating a space where people could \"talk\" with each other, what I hoped would become a dialogue across differences. We created the first couple of topics, and set it up so anyone coming to the website could add new topics. Within each topic, people could post and respond to posts from others.",
    "It was asynchronous, meaning someone could post one day, then come back to see, and add to, what others had posted since the last time they'd been on the site. The dialogue area took on a life of its own, continuing for almost a year, ultimately with 225320 topics and tk posts.",
    "For me personally, it was a glimpse of the potential of the internet to build community, bridge differences, deepen understanding, and widen perspectives. I was so inspired by the site that I transitioned out of my role at POV and started Web Lab, a new non-profit organization, to explore and expand the potential of the web.",
]


def render_credits_content() -> str:
    leadership = [
        ("This site was developed by", "P.O.V. Interactive, in cooperation with PBS Online, under the direction of Marc N. Weiss."),
        ("Designed by", "Alison Cornyn, Sue Johnson and Chris Vail of Picture Projects."),
    ]
    contributors = [
        ("Consultant Extraordinaire", "Fred Branfman"),
        ("Project Coordinator", "Jill Soley"),
        ("Coro Fellow", "Anim Steel"),
        ("Oral History Consultant", "Bret Eynon"),
        ("Dayton Story Circle* Convenor", "Marilyn Shannon"),
    ]
    special_thanks = [
        "PBS Online",
        "John Hollar and Cindy Johanson for unflagging support",
        "Dave Johnston, Mike Cramer, Kevin Dando and Molly Breeden",
        "P.O.V. Interactive",
        "Robin Stober",
        "WNET/13, New York, especially to Ward Chamberlin and Margot Cozell",
        "Bill Labeur and CPSI, Colorado Springs, CO",
        "Frieda Lee Mock, Producer/Director, Maya Lin: A Strong, Clear Vision",
        "Thanks to the following people who volunteered their time to make this site possible: Bronwyn Jones, Beth Friedman, Dina Luciano, Lisa Hamilton, Nicole Kovach, David Marcinkowski, Rajul Mehta, Wendy Sanders, and Tania Van Bergen.",
        "We are very grateful to the hundreds of people who have contributed their wisdom, energy and enthusiasm to this project. You will find their stories in the archives and a selection of them in the stories section.",
        'If you have any comments or questions about the site we would love to hear from you. Please include the words "Vietnam website" in the subject line. stories@weblab.org',
        "* A number of stories appearing on this site were shared at story circles held under the auspices of the Dayton Stories Project (DSP) in Dayton, Ohio. A collaborative arts project of CITYFOLK, The Human Race Theatre Company and other local and out-of-town partners, DSP included community sharing events at which participants told their stories to each other; a Dayton Stories component at the National Folk Festival in Dayton; an original play based on the stories; and an archive of audio tapes and transcripts of stories told during the project.",
        "Central to the project was the belief that stories have the power to affirm individual lives, promote understanding of diverse cultures and create communities where people know and cherish one another.",
        "At the invitation of P.O.V. Interactive, a story circle was held for members of the Vietnam Veterans of America, Miami Valley Chapter 97, and another for individuals who had opposed the war. For more information about the Dayton Stories Project, contact project director Marilyn Shannon at DTSHANNON@AOL.com",
    ]
    leadership_markup = "".join(
        f'<div class="credits-pair"><div>{escape_text(label)}</div><div>{escape_text(value)}</div></div>'
        for label, value in leadership
    )
    contributor_markup = "".join(
        f'<div class="credits-pair"><div>{escape_text(role)}</div><div>{escape_text(name)}</div></div>'
        for role, name in contributors
    )
    thanks_markup = "".join(f"<li>{escape_text(item)}</li>" for item in special_thanks)
    return f"""
<section class="credits-block rich-copy">
  {leadership_markup}
</section>
<section class="credits-block">
  <h2>Contributors</h2>
  <div class="rich-copy">
    {contributor_markup}
  </div>
</section>
<section class="credits-block">
  <h2>Special Thanks</h2>
  <ul class="credits-list">
    {thanks_markup}
  </ul>
</section>
"""


def render_producer_letter_content() -> str:
    paragraphs = []
    for index, paragraph in enumerate(PRODUCER_LETTER_PARAGRAPHS):
        class_name = ' class="lead-paragraph"' if index == 0 else ""
        paragraphs.append(f"<p{class_name}>{escape_text(paragraph)}</p>")
    return '<section class="rich-copy">' + "".join(paragraphs) + '<p class="signature">Marc</p>' + "</section>"


def render_home_content() -> str:
    intro = [
        "This site is a contemporary recovery of an early online community that formed around Re: Vietnam: Stories Since the War, a PBS / POV experiment in public dialogue. It brings back a historically important conversation in a form that people can read, browse, and study now, while still preserving the seriousness, vulnerability, and texture of the original exchange.",
        "The original project combined a television broadcast, a stories archive, and a discussion space at a moment when the web itself was still new. People returned over time to post, respond, reflect, argue, and remember together, creating an unusually sustained public conversation about Vietnam, memory, loss, responsibility, and healing.",
        "What survives here is not just content but a record of how an online community once tried to listen across differences. The archive matters because it preserves that communal process as well as the individual testimonies within it.",
    ]
    second_paragraph = (
        'We have updated the site to utilize the latest in web technologies to improve accessibility across a variety '
        'of devices, but you can see the original web design, which captures the unique look of the early web, '
        '<a href="original_web_design.html">on this page</a>.'
    )
    return (
        '<section class="rich-copy">'
        + f"<p>{escape_text(intro[0])}</p>"
        + f"<p>{second_paragraph}</p>"
        + "".join(f"<p>{escape_text(paragraph)}</p>" for paragraph in intro[1:])
        + "</section>"
        + '<div class="callout-links"><a href="producer_letter.html">Read the producer letter</a><a href="credits.html">View credits</a></div>'
    )


def render_info_page(config: dict, *, page_title: str, eyebrow: str, current_utility: str, content_html: str) -> str:
    body = f"""
<main class="page">
  <header class="masthead">
    {render_top_nav(
        current_utility=current_utility,
        home_href="index.html",
        producer_href="producer_letter.html",
        credits_href="credits.html",
        downloads_href="downloads.html",
    )}
    {render_site_titlebar("index.html")}
  </header>
  {render_explore_nav(
      current_primary="",
      index_href="active_threads.html",
      original_href="index_original.html",
      original_web_href="original_web_design.html",
      theme_href="topics.html",
      original_enabled=bool(config.get("enable_original_order_view", False)),
      original_web_enabled=True,
      theme_enabled=bool(config.get("output_topic_page", False)),
  )}
  <section class="page-intro">
    <div class="eyebrow">{escape_text(eyebrow)}</div>
    <h1>{escape_text(page_title)}</h1>
  </section>
  {content_html}
 </main>
"""
    return render_page(page_title, body)


def render_archive_page(
    *,
    config: dict,
    page_title: str,
    eyebrow: str,
    lede: str,
    current_primary: str,
    index_href: str,
    original_href: str,
    original_web_href: str,
    theme_href: str,
    home_href: str,
    producer_href: str,
    credits_href: str,
    downloads_href: str,
    original_enabled: bool,
    original_web_enabled: bool,
    theme_enabled: bool,
    body_html: str,
) -> str:
    lede_markup = f'<p class="lede">{escape_text(lede)}</p>' if lede else ""
    eyebrow_markup = f'<div class="eyebrow">{escape_text(eyebrow)}</div>' if eyebrow else ""
    body = f"""
<main class="page">
  <header class="masthead">
    {render_top_nav(
        current_utility="",
        home_href=home_href,
        producer_href=producer_href,
        credits_href=credits_href,
        downloads_href=downloads_href,
    )}
    {render_site_titlebar(home_href)}
  </header>
  {render_explore_nav(
      current_primary=current_primary,
      index_href=index_href,
      original_href=original_href,
      original_web_href=original_web_href,
      theme_href=theme_href,
      original_enabled=original_enabled,
      original_web_enabled=original_web_enabled,
      theme_enabled=theme_enabled,
  )}
  <section class="page-intro">
    {eyebrow_markup}
    <h1>{escape_text(page_title)}</h1>
    {lede_markup}
  </section>
  {body_html}
</main>
"""
    return render_page(page_title, body)


def build_post_theme_map(matched_topics: list[dict]) -> dict[tuple[str, int], list[dict]]:
    post_theme_map: dict[tuple[str, int], list[dict]] = {}
    for topic in matched_topics:
        for subtopic in topic["subtopics"]:
            theme_entry = {
                "title": subtopic["title"],
                "filename": topic_set_filename(topic["slug"], subtopic["slug"]),
                "keywords": subtopic["keywords"],
            }
            for match in subtopic["matches"]:
                thread = match["thread"]
                for post in match["posts"]:
                    key = (thread.thread_key, post.post_id)
                    entries = post_theme_map.setdefault(key, [])
                    if any(existing["filename"] == theme_entry["filename"] for existing in entries):
                        continue
                    entries.append(
                        {
                            **theme_entry,
                            "score": theme_match_score(post, [keyword.lower() for keyword in subtopic["keywords"]]),
                        }
                    )
    return post_theme_map


def render_post_card(
    thread: Thread,
    post: Post,
    word_limit: int,
    post_theme_map: dict[tuple[str, int], list[dict]],
    post_summary_map: dict[tuple[str, int], str],
    post_mentions_map: dict[tuple[str, int], list[str]],
    topic_base_path: str,
    current_theme_filename: str | None = None,
    action_link: str | None = None,
    action_label: str | None = None,
    themes_index_path: str | None = None,
) -> str:
    preview, truncated = preview_post(post.post_body, word_limit)
    post_key = f"post-{thread.directory}-{thread.index_id}-{post.post_id}"
    toggle = ""
    preview_markup = f'<div id="{post_key}-preview" class="post-body">{escape_text(preview)}</div>'
    full_markup = ""
    if truncated:
        full_markup = f'<div id="{post_key}-full" class="post-body" hidden>{escape_text(post.post_body)}</div>'
        toggle = f'<button class="toggle" data-expand-target="{post_key}">Show All</button>'

    summary_text = post_summary_map.get((thread.thread_key, post.post_id), "")
    mentions = post_mentions_map.get((thread.thread_key, post.post_id), [])
    themes = post_theme_map.get((thread.thread_key, post.post_id), [])
    if themes:
        ordered_themes = sorted(list(themes), key=lambda theme: (-theme.get("score", 0), theme["title"].lower()))
        if len(ordered_themes) > 5:
            confidence_floor = max(6, ordered_themes[4].get("score", 0))
            filtered_themes = [theme for theme in ordered_themes if theme.get("score", 0) >= confidence_floor][:5]
        else:
            filtered_themes = ordered_themes
        if current_theme_filename and not any(theme["filename"] == current_theme_filename for theme in filtered_themes):
            current_theme = next((theme for theme in ordered_themes if theme["filename"] == current_theme_filename), None)
            if current_theme is not None:
                filtered_themes = [current_theme] + filtered_themes[:4]
        ordered_themes = filtered_themes
        if current_theme_filename:
            ordered_themes.sort(
                key=lambda theme: (theme["filename"] != current_theme_filename, -theme.get("score", 0), theme["title"].lower())
            )
        rendered_items = []
        for theme in ordered_themes:
            if theme["filename"] == current_theme_filename:
                rendered_items.append(f'<li class="current-theme">{escape_text(theme["title"])}</li>')
            else:
                rendered_items.append(
                    f'<li><a href="{topic_base_path}{theme["filename"]}">{escape_text(theme["title"])}</a></li>'
                )
        theme_markup = '<ul class="post-side-list">' + "".join(rendered_items) + "</ul>"
    else:
        theme_markup = '<div class="post-side-empty">No related themes identified.</div>'

    side_title = '<span class="post-side-label">Related Themes</span>'
    see_all_markup = ""
    if themes_index_path:
        see_all_markup = f'<div class="post-side-see-all">(<a href="{themes_index_path}">See all</a>)</div>'

    actions = ""
    if action_label:
        actions = f'<div class="post-actions">{action_label}</div>'

    mention_markup = ", ".join(escape_text(mention) for mention in mentions) if mentions else "No notable mentions identified."

    return f"""<article class="post-card" id="post-{post.post_id}">
<div class="post-main">
<div class="thread-heading">
  <h3>{escape_text(post.post_title)}</h3>
  <div class="thread-heading-by">{escape_text(format_date_with_ordinal(post.created_at))} by {escape_text(post.name_string)}</div>
</div>
{actions}
{preview_markup}
{full_markup}
{toggle}
</div>
</article>"""


def render_modern_thread_batch_nav(
    *,
    thread: Thread,
    batch_index: int,
    css_class: str = "",
) -> str:
    total_posts = len(thread.posts)
    batch_count = thread_batch_count(thread)
    items = []
    previous_count = min(THREAD_BATCH_SIZE, batch_index * THREAD_BATCH_SIZE)
    remaining_after_current = total_posts - ((batch_index + 1) * THREAD_BATCH_SIZE)
    next_count = min(THREAD_BATCH_SIZE, max(0, remaining_after_current))
    if previous_count > 0:
        previous_href = modern_thread_page_name(thread.thread_key, batch_index - 1)
        items.append(f'<a href="{previous_href}">Previous {previous_count}</a>')
    for current_index in range(batch_count):
        start = current_index * THREAD_BATCH_SIZE
        end = min(total_posts, start + THREAD_BATCH_SIZE)
        label = f"{start + 1} - {end}"
        href = modern_thread_page_name(thread.thread_key, current_index)
        if current_index == batch_index:
            items.append(f"<strong>{label}</strong>")
        else:
            items.append(f'<a href="{href}">{label}</a>')
    if next_count > 0:
        next_href = modern_thread_page_name(thread.thread_key, batch_index + 1)
        items.append(f'<a href="{next_href}">Next {next_count}</a>')
    classes = "thread-batch-nav" if not css_class else f"thread-batch-nav {css_class}"
    return f'<div class="{classes}">' + " | ".join(items) + "</div>"


def render_home_page(config: dict, ordered_threads: list[Thread], original_enabled: bool) -> str:
    items = []
    for thread in ordered_threads:
        range_text = format_date_range(thread.posts)
        items.append(
            f"""<article class="thread-card">
<h2><a href="threads/{modern_thread_page_name(thread.thread_key, 0)}">{escape_text(thread.thread_title)}</a></h2>
<div class="post-meta">{len(thread.posts)} posts · {escape_text(range_text)}</div>
</article>"""
        )
    return render_archive_page(
        config=config,
        page_title="Most Active",
        eyebrow="",
        lede="This view presents our online community by thread activity, surfacing the conversations that drew the most participation while still preserving the shape of the forum as people actually used it.",
        current_primary="rank",
        index_href="active_threads.html",
        original_href="index_original.html",
        original_web_href="original_web_design.html",
        theme_href="topics.html",
        home_href="index.html",
        producer_href="producer_letter.html",
        credits_href="credits.html",
        downloads_href="downloads.html",
        original_enabled=original_enabled,
        original_web_enabled=True,
        theme_enabled=bool(config.get("output_topic_page", False)),
        body_html='<section class="thread-list">' + ''.join(items) + "</section>",
    )


def render_original_page(config: dict, ordered_threads: list[Thread]) -> str:
    items = []
    for thread in ordered_threads:
        items.append(
            f"""<article class="thread-card">
<h2><a href="threads/{modern_thread_page_name(thread.thread_key, 0)}">{escape_text(thread.thread_title)}</a></h2>
<div class="post-meta">{len(thread.posts)} posts · {escape_text(format_date_range(thread.posts))}</div>
</article>"""
        )
    return render_archive_page(
        config=config,
        page_title="Posted Order",
        eyebrow="",
        lede="This view follows our online community in its original sequence, keeping the conversations in the order they appeared so the dialogue can be read as it unfolded over time.",
        current_primary="original",
        index_href="active_threads.html",
        original_href="index_original.html",
        original_web_href="original_web_design.html",
        theme_href="topics.html",
        home_href="index.html",
        producer_href="producer_letter.html",
        credits_href="credits.html",
        downloads_href="downloads.html",
        original_enabled=True,
        original_web_enabled=True,
        theme_enabled=bool(config.get("output_topic_page", False)),
        body_html='<section class="thread-list">' + ''.join(items) + "</section>",
    )


def render_thread_page(
    config: dict,
    thread: Thread,
    post_theme_map: dict[tuple[str, int], list[dict]],
    post_summary_map: dict[tuple[str, int], str],
    post_mentions_map: dict[tuple[str, int], list[str]],
    *,
    batch_index: int,
) -> str:
    posts_html = []
    word_limit = max(1, int(round(float(config["preview_word_limit"]) * 1.5)))
    sorted_posts = sorted(thread.posts, key=lambda item: item.post_id)
    start = batch_index * THREAD_BATCH_SIZE
    batch_posts = sorted_posts[start : start + THREAD_BATCH_SIZE]
    for post in batch_posts:
        posts_html.append(
            render_post_card(
                thread=thread,
                post=post,
                word_limit=word_limit,
                post_theme_map=post_theme_map,
                post_summary_map=post_summary_map,
                post_mentions_map=post_mentions_map,
                topic_base_path="../topic_sets/",
                themes_index_path="../topics.html",
            )
        )
    top_batch_nav = render_modern_thread_batch_nav(thread=thread, batch_index=batch_index)
    bottom_batch_nav = render_modern_thread_batch_nav(thread=thread, batch_index=batch_index, css_class="bottom")
    return render_archive_page(
        config=config,
        page_title=thread.thread_title,
        eyebrow="Topic",
        lede="",
        current_primary="",
        index_href="../active_threads.html",
        original_href="../index_original.html",
        original_web_href="../original_web_design.html",
        theme_href="../topics.html",
        home_href="../index.html",
        producer_href="../producer_letter.html",
        credits_href="../credits.html",
        downloads_href="../downloads.html",
        original_enabled=bool(config.get("enable_original_order_view", False)),
        original_web_enabled=True,
        theme_enabled=bool(config.get("output_topic_page", False)),
        body_html=top_batch_nav + '<section class="thread-list">' + ''.join(posts_html) + "</section>" + bottom_batch_nav,
    )


def topic_set_filename(topic_slug: str, subtopic_slug: str) -> str:
    return f"{topic_slug}__{subtopic_slug}.html"


def match_topics(threads: list[Thread], topic_definitions: list[dict]) -> list[dict]:
    results = []
    for topic in topic_definitions:
        subtopic_matches = []
        total_threads = set()
        total_posts = 0
        for subtopic in topic.get("subtopics", []):
            keywords = [keyword.lower() for keyword in subtopic["keywords"]]
            matched_threads = []
            for thread in threads:
                matched_posts = []
                for post in thread.posts:
                    haystack = "\n".join([post.post_title.lower(), post.post_body.lower()])
                    if any(keyword in haystack for keyword in keywords):
                        matched_posts.append(post)
                if matched_posts:
                    matched_threads.append({"thread": thread, "posts": matched_posts})
                    total_threads.add(thread.thread_key)
                    total_posts += len(matched_posts)
            subtopic_matches.append(
                {
                    **subtopic,
                    "matches": matched_threads,
                    "thread_count": len(matched_threads),
                    "post_count": sum(len(match["posts"]) for match in matched_threads),
                }
            )
        results.append({**topic, "subtopics": subtopic_matches, "thread_count": len(total_threads), "post_count": total_posts})
    return results


def render_topic_set_page(
    config: dict,
    topic: dict,
    subtopic: dict,
    post_theme_map: dict[tuple[str, int], list[dict]],
    post_summary_map: dict[tuple[str, int], str],
    post_mentions_map: dict[tuple[str, int], list[str]],
) -> str:
    thread_sections = []
    word_limit = max(1, int(round(float(config["preview_word_limit"]) * 1.5)))
    current_theme_filename = topic_set_filename(topic["slug"], subtopic["slug"])
    for match in subtopic["matches"]:
        thread = match["thread"]
        posts_html = []
        for post in match["posts"]:
            full_thread_link = (
                f'../threads/{modern_thread_page_name(thread.thread_key, thread_batch_index_for_post(thread, post.post_id))}'
                f'#post-{post.post_id}'
            )
            action_markup = (
                f'From the "<a href="{full_thread_link}">{escape_text(thread.thread_title)}</a>" thread.'
            )
            posts_html.append(
                render_post_card(
                    thread=thread,
                    post=post,
                    word_limit=word_limit,
                    post_theme_map=post_theme_map,
                    post_summary_map=post_summary_map,
                    post_mentions_map=post_mentions_map,
                    topic_base_path="../topic_sets/",
                    current_theme_filename=current_theme_filename,
                    action_label=action_markup,
                    themes_index_path="../topics.html",
                )
            )
        thread_sections.append(
            f"""<section class="topic-thread-group">
<h2>{escape_text(thread.thread_title)}</h2>
<div class="post-meta">{len(match['posts'])} matched posts · {escape_text(format_date_range(thread.posts))}</div>
{''.join(posts_html)}
</section>"""
        )
    page_title = f"{topic['title']}: {subtopic['title']}"
    durable_badge = '<div class="topic-badge">Durable Topic</div>' if topic.get("durable") else ""
    return render_archive_page(
        config=config,
        page_title=page_title,
        eyebrow="Explore Archive",
        lede=subtopic["summary"],
        current_primary="theme",
        index_href="../active_threads.html",
        original_href="../index_original.html",
        original_web_href="../original_web_design.html",
        theme_href="../topics.html",
        home_href="../index.html",
        producer_href="../producer_letter.html",
        credits_href="../credits.html",
        downloads_href="../downloads.html",
        original_enabled=bool(config.get("enable_original_order_view", False)),
        original_web_enabled=True,
        theme_enabled=True,
        body_html='<section class="thread-list">' + durable_badge + "".join(thread_sections) + "</section>",
    )


def render_topics_page(config: dict, matched: list[dict]) -> str:
    cards = []
    for topic in matched:
        durable_badge = '<div class="topic-badge">Durable Topic</div>' if topic.get("durable") else ""
        links = "".join(
            f"""<div class="topic-match-row">
<div class="topic-subtopic-title">
<a href="topic_sets/{topic_set_filename(topic['slug'], subtopic['slug'])}">{escape_text(subtopic['title'])}</a>
<span class="post-meta">{subtopic['post_count']} posts</span>
</div>
<div class="topic-subtopic-summary">{escape_text(subtopic['summary'])}</div>
</div>"""
            for subtopic in topic["subtopics"]
        )
        cards.append(
            f"""<article class="topic-card">
{durable_badge}
<h2>{escape_text(topic['title'])}</h2>
<p class="lede">{escape_text(topic['summary'])}</p>
<div class="post-meta">{topic['post_count']} matched posts in draft pass</div>
<div class="topic-match-list">{links}</div>
</article>"""
        )
    return render_archive_page(
        config=config,
        page_title="Theme Explorer",
        eyebrow="Explore Archive",
        lede="The themes and topics gathered here combine generative AI analysis of the archive with historical framing to create a meaning map of community participation. Individual messages are enriched with summaries, mentions of notable people and organizations, and links to related themes, not to rewrite what participants said, but to provide interpretive scaffolding and navigation so readers can explore the archive according to their interests.",
        current_primary="theme",
        index_href="active_threads.html",
        original_href="index_original.html",
        original_web_href="original_web_design.html",
        theme_href="topics.html",
        home_href="index.html",
        producer_href="producer_letter.html",
        credits_href="credits.html",
        downloads_href="downloads.html",
        original_enabled=bool(config.get("enable_original_order_view", False)),
        original_web_enabled=True,
        theme_enabled=True,
        body_html='<section class="thread-list">' + ''.join(cards) + "</section>",
    )


def emit_csv(threads: list[Thread], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "post_created_at",
                "post_created_at_display",
                "thread_title",
                "post_title",
                "post_email",
                "post_name",
                "post_body",
                "post_id",
                "thread_index_id",
                "directory",
                "thread_key",
            ]
        )
        for thread in threads:
            if not thread.posts:
                writer.writerow(
                    [
                        "",
                        thread.thread_title,
                        "",
                        "",
                        "",
                        "",
                        "",
                        thread.index_id,
                        thread.directory,
                        thread.thread_key,
                    ]
                )
                continue
            for post in sorted(thread.posts, key=lambda item: item.post_id):
                writer.writerow(
                    [
                        post.created_at.isoformat(),
                        post.created_at_display,
                        thread.thread_title,
                        post.post_title,
                        post.email_string,
                        post.name_string,
                        post.post_body,
                        post.post_id,
                        thread.index_id,
                        thread.directory,
                        thread.thread_key,
                    ]
                )


def emit_jsonc_archive(threads: list[Thread], path: Path) -> None:
    payload = {
        "format": "vietnam-stories-archive",
        "version": 1,
        "site_title": "Re: Vietnam: Stories Since the War",
        "exported_at": datetime.now().astimezone().isoformat(),
        "thread_count": len(threads),
        "post_count": sum(len(thread.posts) for thread in threads),
        "threads": [
            {
                "thread_key": thread.thread_key,
                "thread_index_id": thread.index_id,
                "thread_title": thread.thread_title,
                "directory": thread.directory,
                "original_position": thread.original_position,
                "post_count": len(thread.posts),
                "posts": [
                    {
                        "post_id": post.post_id,
                        "created_at": post.created_at.isoformat(),
                        "created_at_display": post.created_at_display,
                        "post_title": post.post_title,
                        "post_body": post.post_body,
                        "author": {
                            "display_name": post.name_string,
                            "email": post.email_string,
                        },
                    }
                    for post in sorted(thread.posts, key=lambda item: item.post_id)
                ],
            }
            for thread in threads
        ],
    }
    json_body = json.dumps(payload, ensure_ascii=False, indent=2)
    commented = (
        "{\n"
        '  // Normalized export of the original Vietnam Stories forum corpus.\n'
        '  // This file uses JSONC (JSON with comments), a common industry format for human-editable JSON.\n'
        '  // Remove comment lines if you need strict RFC 8259 JSON.\n'
        + json_body[1:]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(commented, encoding="utf-8")


def format_file_size(size: int) -> str:
    units = ["bytes", "KB", "MB", "GB"]
    value = float(size)
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    return f"{value:.1f} {units[unit_index]}"


def create_zip_archive(zip_path: Path, base_dir: Path, *, exclude_paths: set[Path] | None = None) -> None:
    exclude_paths = exclude_paths or set()
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as handle:
        for file_path in sorted(base_dir.rglob("*")):
            if file_path.is_dir():
                continue
            if any(file_path == excluded for excluded in exclude_paths):
                continue
            handle.write(file_path, file_path.relative_to(base_dir))


def render_downloads_content(
    *,
    data_href: str,
    data_size_text: str,
    site_href: str,
    site_size_text: str,
) -> str:
    return f"""
<section class="rich-copy">
  <p><a href="{data_href}">Download the original-data JSONC archive</a> ({escape_text(data_size_text)}). This zip contains a commented JSONC export of the normalized thread and post data, including thread metadata, preserved display timestamps, post text, and author fields for archival or research use.</p>
  <p><a href="{site_href}">Download the static HTML site archive</a> ({escape_text(site_size_text)}). This zip contains the full static website, including the modern archive views, the original web design views, downloads page, and supporting downloadable assets.</p>
</section>
"""


def emit_mentions_audit(rows: list[dict[str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "directory",
                "thread_key",
                "thread_index_id",
                "thread_title",
                "post_id",
                "post_created_at",
                "post_title",
                "post_author",
                "review_mode",
                "raw_candidates",
                "llm_mentions",
                "final_mentions",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def write_site(config: dict, threads: list[Thread], output_dir: Path) -> list[dict[str, str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    downloads_dir = output_dir / "downloads"
    downloads_dir.mkdir(exist_ok=True)
    thread_dir = output_dir / "threads"
    thread_dir.mkdir(exist_ok=True)
    original_web_thread_dir = output_dir / "threads_original_web"
    original_web_thread_dir.mkdir(exist_ok=True)
    topic_set_dir = output_dir / "topic_sets"
    topic_set_dir.mkdir(exist_ok=True)
    legacy_home_path = output_dir / "home.html"
    if legacy_home_path.exists():
        legacy_home_path.unlink()

    activity_threads = activity_order(threads)
    original_threads = original_order(threads)
    original_enabled = bool(config.get("enable_original_order_view", False))

    (output_dir / "index.html").write_text(
        render_info_page(
            config,
            page_title="Recovering the Early History of Television on the Web",
            eyebrow="Project Context",
            current_utility="home",
            content_html=render_home_content(),
        ),
        encoding="utf-8",
    )
    (output_dir / "active_threads.html").write_text(
        render_home_page(config, activity_threads, original_enabled), encoding="utf-8"
    )
    (output_dir / "producer_letter.html").write_text(
        render_info_page(
            config,
            page_title="A Letter from Producer Marc Weiss",
            eyebrow="Project Context",
            current_utility="producer",
            content_html=render_producer_letter_content(),
        ),
        encoding="utf-8",
    )
    (output_dir / "credits.html").write_text(
        render_info_page(
            config,
            page_title="Credits",
            eyebrow="Project Credits",
            current_utility="credits",
            content_html=render_credits_content(),
        ),
        encoding="utf-8",
    )
    if original_enabled:
        (output_dir / "index_original.html").write_text(
            render_original_page(config, original_threads), encoding="utf-8"
        )
    (output_dir / "original_web_design.html").write_text(
        render_original_web_topics_page(original_threads), encoding="utf-8"
    )
    if config.get("output_topic_page", False):
        topic_path = Path(config["topic_curation_path"])
        topic_defs = json.loads(topic_path.read_text(encoding="utf-8"))
        matched = match_topics(activity_threads, topic_defs)
        post_theme_map = build_post_theme_map(matched)
        post_summary_map = build_post_summary_map(threads)
        post_mentions_map, mentions_audit_rows = build_post_mentions_map(threads, config)
        (output_dir / "topics.html").write_text(
            render_topics_page(config, matched), encoding="utf-8"
        )
        for topic in matched:
            for subtopic in topic["subtopics"]:
                filename = topic_set_filename(topic["slug"], subtopic["slug"])
                (topic_set_dir / filename).write_text(
                    render_topic_set_page(
                        config,
                        topic,
                        subtopic,
                        post_theme_map,
                        post_summary_map,
                        post_mentions_map,
                    ),
                    encoding="utf-8",
                )
    else:
        post_theme_map = {}
        post_summary_map = build_post_summary_map(threads)
        post_mentions_map, mentions_audit_rows = build_post_mentions_map(threads, config)
    for thread in threads:
        batch_count = thread_batch_count(thread)
        for batch_index in range(batch_count):
            filename = modern_thread_page_name(thread.thread_key, batch_index)
            (thread_dir / filename).write_text(
                render_thread_page(
                    config,
                    thread,
                    post_theme_map,
                    post_summary_map,
                    post_mentions_map,
                    batch_index=batch_index,
                ),
                encoding="utf-8",
            )
    for thread in original_threads:
        sorted_posts = sorted(thread.posts, key=lambda item: item.post_id)
        batch_count = max(1, math.ceil(len(sorted_posts) / THREAD_BATCH_SIZE))
        if not sorted_posts:
            filename = original_web_thread_page_name(thread.thread_key, 0)
            (original_web_thread_dir / filename).write_text(
                render_original_web_thread_batch_page(
                    thread,
                    batch_index=0,
                    batch_posts=[],
                    batch_count=batch_count,
                ),
                encoding="utf-8",
            )
            continue
        for batch_index in range(batch_count):
            start = batch_index * THREAD_BATCH_SIZE
            batch_posts = sorted_posts[start : start + THREAD_BATCH_SIZE]
            filename = original_web_thread_page_name(thread.thread_key, batch_index)
            (original_web_thread_dir / filename).write_text(
                render_original_web_thread_batch_page(
                    thread,
                    batch_index=batch_index,
                    batch_posts=batch_posts,
                    batch_count=batch_count,
                ),
                encoding="utf-8",
            )

    data_jsonc_path = downloads_dir / "vietnam_stories_original_data.jsonc"
    data_zip_path = downloads_dir / "vietnam_stories_original_data_jsonc.zip"
    static_zip_path = downloads_dir / "vietnam_stories_static_html_site.zip"

    emit_jsonc_archive(threads, data_jsonc_path)
    with zipfile.ZipFile(data_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as handle:
        handle.write(data_jsonc_path, data_jsonc_path.name)

    placeholder_downloads = render_info_page(
        config,
        page_title="Downloads",
        eyebrow="Downloads",
        current_utility="downloads",
        content_html=render_downloads_content(
            data_href=f"downloads/{data_zip_path.name}",
            data_size_text=format_file_size(data_zip_path.stat().st_size),
            site_href=f"downloads/{static_zip_path.name}",
            site_size_text="Calculating…",
        ),
    )
    (output_dir / "downloads.html").write_text(placeholder_downloads, encoding="utf-8")

    create_zip_archive(static_zip_path, output_dir, exclude_paths={static_zip_path})
    static_size_text = format_file_size(static_zip_path.stat().st_size)
    final_downloads = render_info_page(
        config,
        page_title="Downloads",
        eyebrow="Downloads",
        current_utility="downloads",
        content_html=render_downloads_content(
            data_href=f"downloads/{data_zip_path.name}",
            data_size_text=format_file_size(data_zip_path.stat().st_size),
            site_href=f"downloads/{static_zip_path.name}",
            site_size_text=static_size_text,
        ),
    )
    (output_dir / "downloads.html").write_text(final_downloads, encoding="utf-8")
    create_zip_archive(static_zip_path, output_dir, exclude_paths={static_zip_path})
    return mentions_audit_rows


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if bool(args.source_root) == bool(args.csv_input):
        raise SystemExit("Provide exactly one of --source-root or --csv-input.")

    if args.source_root:
        included_forums = config["included_forums"] or discover_forums(args.source_root)
        threads = load_threads_from_source(args.source_root, included_forums)
    else:
        threads = load_threads_from_csv(args.csv_input)

    threads = prepare_threads(threads, config.get("email_redaction_mode", "none"))
    mentions_audit_rows = write_site(config, threads, args.output_dir)

    if args.emit_csv:
        emit_csv(threads, args.emit_csv)
    if args.emit_mentions_audit:
        emit_mentions_audit(mentions_audit_rows, args.emit_mentions_audit)

    print(f"Wrote draft site to {args.output_dir}")
    print(f"Loaded {len(threads)} threads and {sum(len(thread.posts) for thread in threads)} posts")


if __name__ == "__main__":
    main()
