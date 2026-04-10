#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import re
import subprocess
from pathlib import Path

from vietnam_stories_draft import clean_text, discover_forums, strip_tags


VALID_EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
HEURISTIC_EMAIL = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$", re.IGNORECASE)
SPACED_EMAIL = re.compile(
    r"\b[A-Z0-9._%+-]+(?:\s+@\s*|\s*@\s+)[A-Z0-9-]+(?:\s*\.\s*[A-Z0-9-]+)+\b",
    re.IGNORECASE,
)
BROKEN_DOMAIN = re.compile(
    r"\b[A-Z0-9._%+-]+\s*@\s*[A-Z0-9-]+(?:\s+\.\s*[A-Z0-9-]+)+\b",
    re.IGNORECASE,
)
READ_TIMEOUT_SECONDS = 0.2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an email redaction review CSV.")
    parser.add_argument("--source-root", type=Path, required=True, help="Legacy forum root, e.g. original_website/stories/vietnam")
    parser.add_argument("--output-csv", type=Path, required=True, help="Destination CSV path")
    return parser.parse_args()


def normalize_candidate(text: str) -> str:
    return re.sub(r"\s+", "", text.strip()).rstrip(".,;:!?)]}")


def snippet(text: str, match_text: str, width: int = 80) -> str:
    index = text.find(match_text)
    if index == -1:
        return text[: width * 2]
    start = max(0, index - width)
    end = min(len(text), index + len(match_text) + width)
    return text[start:end].replace("\n", " ")


def iter_candidates(text: str) -> list[tuple[str, str, float, str]]:
    seen = set()
    results: list[tuple[str, str, float, str]] = []

    for match in VALID_EMAIL.finditer(text):
        original = match.group(0)
        if original in seen:
            continue
        seen.add(original)
        results.append((original, "{email redacted}", 0.99, "valid_email"))

    for regex, confidence, label in [
        (SPACED_EMAIL, 0.9, "spaced_email"),
        (BROKEN_DOMAIN, 0.78, "broken_domain_spacing"),
    ]:
        for match in regex.finditer(text):
            original = match.group(0)
            normalized = normalize_candidate(original)
            if original in seen or normalized in seen:
                continue
            if not HEURISTIC_EMAIL.fullmatch(normalized):
                continue
            seen.add(original)
            seen.add(normalized)
            results.append((original, "{email redacted}", confidence, label))

    return results


def read_text_with_timeout(path: Path, timeout_seconds: float = READ_TIMEOUT_SECONDS) -> str | None:
    try:
        result = subprocess.run(
            ["/bin/cat", str(path)],
            check=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return None
    return result.stdout.decode("latin-1", errors="ignore")


def parse_post_content(content: str) -> tuple[str, str, str, str] | None:
    title_match = re.search(r'<font color="#FF0000">(.*?)</font><br>', content, re.IGNORECASE | re.DOTALL)
    body_match = re.search(r"</tt><p>(.*?)<p><code>--", content, re.IGNORECASE | re.DOTALL)
    author_match = re.search(r"<p><code>--\s*(.*?)\((.*?)\)\s*</code>", content, re.IGNORECASE | re.DOTALL)
    if not title_match or not body_match or not author_match:
        return None

    post_title = strip_tags(title_match.group(1))
    post_body = strip_tags(body_match.group(1))
    email_string = clean_text(author_match.group(1))
    post_name = clean_text(author_match.group(2))
    return post_title, post_body, email_string, post_name


def main() -> None:
    args = parse_args()
    forums = discover_forums(args.source_root)

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    total_threads = 0
    total_posts = 0
    total_matches = 0
    skipped_files = 0

    with args.output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "directory",
                "thread_index_id",
                "thread_title",
                "post_id",
                "post_title",
                "post_name",
                "field",
                "matched_text",
                "replacement",
                "confidence",
                "rule",
                "context_snippet",
                "full_field_value",
            ]
        )

        for forum_name in forums:
            forum_path = args.source_root / forum_name
            thread_index_path = forum_path / "thread.index"
            thread_index_text = read_text_with_timeout(thread_index_path)
            if thread_index_text is None:
                skipped_files += 1
                continue

            for raw_line in thread_index_text.splitlines():
                parts = raw_line.split("\t", 1)
                if len(parts) != 2:
                    continue
                thread_id_text, thread_title = parts[0].strip(), parts[1].strip()
                if not thread_id_text.isdigit() or not thread_title:
                    continue

                total_threads += 1
                thread_id = int(thread_id_text)
                thread_dir = forum_path / thread_id_text
                if not thread_dir.is_dir():
                    continue

                post_paths = [path for path in thread_dir.glob("post.*") if path.name.split(".")[-1].isdigit()]
                for post_path in sorted(post_paths, key=lambda item: int(item.name.split(".")[-1])):
                    content = read_text_with_timeout(post_path)
                    if content is None:
                        skipped_files += 1
                        continue

                    parsed = parse_post_content(content)
                    if parsed is None:
                        continue

                    total_posts += 1
                    post_title, post_body, email_string, post_name = parsed
                    post_id = int(post_path.name.split(".")[-1])

                    fields = {
                        "post_email": email_string,
                        "post_body": post_body,
                    }
                    for field_name, value in fields.items():
                        for matched_text, replacement, confidence, rule in iter_candidates(value):
                            total_matches += 1
                            writer.writerow(
                                [
                                    forum_name,
                                    thread_id,
                                    thread_title,
                                    post_id,
                                    post_title,
                                    post_name,
                                    field_name,
                                    matched_text,
                                    replacement,
                                    confidence,
                                    rule,
                                    snippet(value, matched_text),
                                    value,
                                ]
                            )

            print(f"Scanned forum {forum_name}")

    print(f"Wrote {args.output_csv}")
    print(f"Scanned {total_threads} threads and {total_posts} posts")
    print(f"Detected {total_matches} candidate email strings")
    print(f"Skipped {skipped_files} unreadable files")


if __name__ == "__main__":
    main()
