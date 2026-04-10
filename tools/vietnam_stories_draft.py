#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import signal
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


POST_DATE_FORMAT = "%a %b %d %H:%M:%S %Z %Y"
EMAIL_REGEX = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
TOKEN_REGEX = re.compile(r"[A-Za-z0-9']+")
SENTENCE_SPLIT_REGEX = re.compile(r"(?<=[.!?])\s+|\n{2,}")
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


@dataclass
class Post:
    post_id: int
    created_at: datetime
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


def append_specifics(summary: str, primary_tokens: set[str], body_tokens: set[str]) -> str:
    details = specific_elements(primary_tokens, body_tokens)
    if not details:
        return summary
    return summary.rstrip(".") + f", touching on {human_join(details)}."


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
    combined = primary_tokens | body_tokens | set(context_token_set) | set(keyword_tokens(thread.thread_title))
    lead_lower = lead_sentence.lower()
    summary = "Discusses one aspect of how the war continues to be remembered and debated."

    if contains_any(combined, {"south", "vietnamese", "arvn"}) and contains_any(
        combined, {"ignore", "ignored", "overlooked", "overlook"}
    ):
        summary = "Argues that South Vietnamese soldiers and civilians are being overlooked in accounts of the war."
    elif contains_any(combined, {"freedom", "expression", "rights", "right", "dissent"}) and contains_any(
        combined, {"discussion", "site", "page", "website"}
    ):
        summary = "Defends open expression and argues over how broad the discussion should be."
    elif contains_any(combined, {"christian", "religion", "religious", "deliverance", "demons", "prophet"}):
        if contains_any(body_tokens | primary_tokens, {"waste", "belong", "line", "wrong", "remove", "crud", "narrow", "mission"}):
            summary = "Pushes back against religiously framed messages appearing in the discussion."
        elif contains_any(primary_tokens | body_tokens, {"help", "support", "free"}):
            summary = "Offers religiously framed help and support."
    elif contains_any(combined, {"thank", "thanks", "grateful", "appreciate", "hope", "keep"}) and contains_any(
        combined, {"site", "page", "website", "pbs"}
    ):
        summary = "Offers appreciation for the site and its role as a place to speak and remember."
    elif contains_any(combined, {"memory", "memories", "remember", "haunt", "haunting", "ptsd", "nightmare", "nightmares"}):
        if contains_any(combined, {"son", "daughter", "children", "child", "family", "father", "mother", "dad"}):
            summary = "Connects painful war memories to family life and later relationships."
        else:
            summary = "Describes how memories of the war continue to surface and shape everyday life."
    elif contains_any(combined, {"friends", "friend", "combat", "brothers", "brother", "miss", "love", "trust"}) and contains_any(
        combined, {"veteran", "veterans", "soldier", "soldiers"}
    ):
        summary = "Reflects on lost friends and the bonds formed in combat."
    elif contains_any(combined, {"wall", "memorial", "names", "remembrance", "remember", "honor", "honour"}):
        summary = "Reflects on remembrance, names, and the need to honor those who died."
    elif contains_any(combined, {"heal", "healing", "peace", "understand", "understanding", "forgive", "forgiveness"}):
        summary = "Looks for understanding, healing, and a way to live with the past."
    elif contains_any(combined, {"protest", "protests", "protestor", "protestors", "politics", "political", "government", "country"}):
        summary = "Debates the politics of the war and the conflicts it still stirs."
    elif contains_any(combined, {"service", "served", "duty", "oath", "charge", "veteran", "veterans", "soldier", "soldiers"}):
        summary = "Reflects on service, duty, and what the war demanded of those who served."
    elif contains_any(combined, {"children", "child", "son", "daughter", "parents", "parent", "mother", "father", "dad", "family"}):
        summary = "Connects the war's effects to family relationships across generations."
    elif contains_any(combined, {"question", "questions", "ask", "asks", "wonder", "why", "how"}):
        summary = "Raises questions about the war and how it should be understood."
    elif contains_any(combined, {"site", "page", "website", "pbs"}):
        summary = "Comments on the site and what kind of dialogue it should make possible."
    elif "?" in lead_sentence:
        summary = "Raises questions about the war and its aftermath."
    elif re.match(r"^(i|we|my|our)\b", lead_lower):
        summary = "Discusses a personal response to the war and its aftermath."

    return with_author_name(append_specifics(summary, primary_tokens, body_tokens), post.name_string)


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
    created_at = parse_legacy_date(date_match.group(1))
    post_body = strip_tags(body_match.group(1))
    email_string = clean_text(author_match.group(1))
    name_string = clean_text(author_match.group(2))
    post_id = int(path.name.split(".")[-1])

    return Post(
        post_id=post_id,
        created_at=created_at,
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
  font-size: 0.82rem;
}
.post-summary {
  color: var(--muted);
  font-size: 0.88rem;
  font-weight: 400;
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


def render_page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title, quote=False)}</title>
  <style>{base_css()}</style>
</head>
<body>
{body}
<script>{base_script()}</script>
</body>
</html>
"""


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
    if themes_index_path:
        side_title = (
            f'<span class="post-side-label">Related Themes</span>'
            f'<span class="post-side-see-all">(<a href="{themes_index_path}">See all</a>)</span>'
        )

    actions = ""
    if action_label:
        actions = f'<div class="post-actions">{action_label}</div>'

    return f"""<article class="post-card" id="post-{post.post_id}">
<div class="post-card-layout">
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
<aside class="post-side">
<section class="post-side-section">
<p class="post-side-title"><span class="post-side-label">AI Summary</span></p>
<div class="post-summary">{escape_text(summary_text)}</div>
</section>
<section class="post-side-section">
<p class="post-side-title">{side_title}</p>
{theme_markup}
</section>
</aside>
</div>
</article>"""


def render_home_page(config: dict, ordered_threads: list[Thread], original_enabled: bool) -> str:
    items = []
    for thread in ordered_threads:
        range_text = format_date_range(thread.posts)
        items.append(
            f"""<article class="thread-card">
<h2><a href="threads/{thread.thread_key}.html">{escape_text(thread.thread_title)}</a></h2>
<div class="post-meta">{len(thread.posts)} posts · {escape_text(range_text)} · source {escape_text(thread.directory)}</div>
</article>"""
        )
    original_link = '<a href="index_original.html">Original Order</a>' if original_enabled else ""
    topic_link = '<a href="topics.html">Topic Explorer</a>' if config.get("output_topic_page", False) else ""
    body = f"""
<main class="page">
  <header class="masthead">
    <div class="eyebrow">Vietnam Stories</div>
    <h1>{escape_text(config['site_title'])}</h1>
    <p class="lede">{escape_text(config['site_subtitle'])}</p>
    <p class="muted">Activity order ranks the special comments thread first when present, then sorts all remaining threads by post count.</p>
    <nav class="nav">
      <strong>Activity Order</strong>
      {original_link}
      {topic_link}
    </nav>
  </header>
  <section class="thread-list">
    {''.join(items)}
  </section>
</main>
"""
    return render_page(config["site_title"], body)


def render_original_page(config: dict, ordered_threads: list[Thread]) -> str:
    topic_link = '<a href="topics.html">Topic Explorer</a>' if config.get("output_topic_page", False) else ""
    items = []
    for thread in ordered_threads:
        items.append(
            f"""<article class="thread-card">
<h2><a href="threads/{thread.thread_key}.html">{escape_text(thread.thread_title)}</a></h2>
<div class="post-meta">Original order · {escape_text(thread.directory)} · {len(thread.posts)} posts · {escape_text(format_date_range(thread.posts))}</div>
</article>"""
        )
    body = f"""
<main class="page">
  <header class="masthead">
    <div class="eyebrow">Vietnam Stories</div>
    <h1>Original Order</h1>
    <nav class="nav">
      <a href="index.html">Activity Order</a>
      <strong>Original Order</strong>
      {topic_link}
    </nav>
  </header>
  <section class="thread-list">
    {''.join(items)}
  </section>
</main>
"""
    return render_page("Original Order", body)


def render_thread_page(
    config: dict,
    thread: Thread,
    post_theme_map: dict[tuple[str, int], list[dict]],
    post_summary_map: dict[tuple[str, int], str],
) -> str:
    topic_link = '<a href="../topics.html">Topic Explorer</a>' if config.get("output_topic_page", False) else ""
    posts_html = []
    word_limit = max(1, int(round(float(config["preview_word_limit"]) * 1.5)))
    for post in sorted(thread.posts, key=lambda item: item.post_id):
        posts_html.append(
            render_post_card(
                thread=thread,
                post=post,
                word_limit=word_limit,
                post_theme_map=post_theme_map,
                post_summary_map=post_summary_map,
                topic_base_path="../topic_sets/",
                themes_index_path="../topics.html",
            )
        )
    body = f"""
<main class="page">
  <header class="masthead">
    <div class="eyebrow">Thread</div>
    <h1>{escape_text(thread.thread_title)}</h1>
    <div class="post-meta">{len(thread.posts)} posts · source {escape_text(thread.directory)}</div>
    <nav class="nav">
      <a href="../index.html">Activity Order</a>
      {topic_link}
    </nav>
  </header>
  <section class="thread-list">
    {''.join(posts_html)}
  </section>
</main>
"""
    return render_page(thread.thread_title, body)


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
) -> str:
    thread_sections = []
    word_limit = max(1, int(round(float(config["preview_word_limit"]) * 1.5)))
    current_theme_filename = topic_set_filename(topic["slug"], subtopic["slug"])
    for match in subtopic["matches"]:
        thread = match["thread"]
        posts_html = []
        for post in match["posts"]:
            full_thread_link = f'../threads/{thread.thread_key}.html#post-{post.post_id}'
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
    body = f"""
<main class="page">
  <header class="masthead">
    <div class="eyebrow">Topic Explorer</div>
    {durable_badge}
    <h1>{escape_text(page_title)}</h1>
    <p class="lede">{escape_text(subtopic['summary'])}</p>
    <div class="post-meta">{subtopic['thread_count']} matched threads · {subtopic['post_count']} matched posts</div>
    <nav class="nav">
      <a href="../topics.html">Topic Explorer</a>
      <a href="../index.html">Activity Order</a>
    </nav>
  </header>
  <section class="thread-list">
    {''.join(thread_sections)}
  </section>
</main>
"""
    return render_page(page_title, body)


def render_topics_page(config: dict, matched: list[dict]) -> str:
    original_link = '<a href="index_original.html">Original Order</a>' if config.get("enable_original_order_view", False) else ""
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
    body = f"""
<main class="page">
  <header class="masthead">
    <div class="eyebrow">Topic Explorer</div>
    <h1>Topic Explorer</h1>
    <p class="lede">This page is intentionally provisional. The topic list is editable and the matches are heuristic.</p>
    <nav class="nav">
      <a href="index.html">Activity Order</a>
      {original_link}
      <strong>Topic Explorer</strong>
    </nav>
  </header>
  <section class="thread-list">
    {''.join(cards)}
  </section>
</main>
"""
    return render_page("Topic Explorer", body)


def emit_csv(threads: list[Thread], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "post_created_at",
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


def write_site(config: dict, threads: list[Thread], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    thread_dir = output_dir / "threads"
    thread_dir.mkdir(exist_ok=True)
    topic_set_dir = output_dir / "topic_sets"
    topic_set_dir.mkdir(exist_ok=True)

    activity_threads = activity_order(threads)
    original_threads = original_order(threads)
    original_enabled = bool(config.get("enable_original_order_view", False))

    (output_dir / "index.html").write_text(render_home_page(config, activity_threads, original_enabled), encoding="utf-8")
    if original_enabled:
        (output_dir / "index_original.html").write_text(
            render_original_page(config, original_threads), encoding="utf-8"
        )
    if config.get("output_topic_page", False):
        topic_path = Path(config["topic_curation_path"])
        topic_defs = json.loads(topic_path.read_text(encoding="utf-8"))
        matched = match_topics(activity_threads, topic_defs)
        post_theme_map = build_post_theme_map(matched)
        post_summary_map = build_post_summary_map(threads)
        (output_dir / "topics.html").write_text(
            render_topics_page(config, matched), encoding="utf-8"
        )
        for topic in matched:
            for subtopic in topic["subtopics"]:
                filename = topic_set_filename(topic["slug"], subtopic["slug"])
                (topic_set_dir / filename).write_text(
                    render_topic_set_page(config, topic, subtopic, post_theme_map, post_summary_map), encoding="utf-8"
                )
    else:
        post_theme_map = {}
        post_summary_map = build_post_summary_map(threads)
    for thread in threads:
        filename = f"{thread.thread_key}.html"
        (thread_dir / filename).write_text(
            render_thread_page(config, thread, post_theme_map, post_summary_map), encoding="utf-8"
        )


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
    write_site(config, threads, args.output_dir)

    if args.emit_csv:
        emit_csv(threads, args.emit_csv)

    print(f"Wrote draft site to {args.output_dir}")
    print(f"Loaded {len(threads)} threads and {sum(len(thread.posts) for thread in threads)} posts")


if __name__ == "__main__":
    main()
