#!/usr/bin/env python3
"""Check the snippets docs/ embeds: that every fence has one, and that every one resolves.

Two checks, both about the same seam between the prose and the compiled examples.

Every ```rust fence must either embed a compiled snippet (a --8<-- include) or be
explicitly exempted by an HTML comment on the line directly above it:

    <!-- inline-rust: <why this cannot come from a compiled example> -->
    ```rust

The justification is mandatory. Exemptions are for code that has no compilable home in
this repository: simplified trait sketches (the real signatures are RPITIT with long doc
comments) and walk-throughs of another crate's internals.

And every include must point at something that exists: the file, and - when the include
names one - the `--8<-- [start:section]` / `[end:section]` pair inside it. Renaming or
rewriting an example silently breaks the pages that quote it, and until this check existed
the break surfaced only in the docs build, which does not run locally. `check_paths` in
properdocs.yml catches the missing file; nothing but this catches the missing section.
"""

import re
import sys
from pathlib import Path

FENCE = re.compile(r"^```rust\b")
SNIPPET = re.compile(r"--8<--")
EXEMPT = re.compile(r"<!--\s*inline-rust:\s*\S")
# What pymdownx.snippets accepts: `--8<-- "path"` or `--8<-- "path:section"`.
INCLUDE = re.compile(r'--8<--\s+"(?P<path>[^":]+)(?::(?P<section>[^"]+))?"')


def fences(path: Path, lines: list[str]) -> list[str]:
    """Every rust fence embeds a snippet, or says why it cannot."""
    errors = []
    inside = False
    fence_line = 0
    exempt = False
    has_snippet = False
    for n, line in enumerate(lines, 1):
        if not inside and FENCE.match(line.strip()):
            inside = True
            fence_line = n
            exempt = n > 1 and bool(EXEMPT.search(lines[n - 2]))
            has_snippet = False
        elif inside:
            if SNIPPET.search(line):
                has_snippet = True
            if line.strip() == "```":
                inside = False
                if not has_snippet and not exempt:
                    errors.append(
                        f"{path}:{fence_line}: inline rust fence - embed a compiled"
                        " snippet (--8<--) or add an `<!-- inline-rust: why -->`"
                        " justification on the previous line"
                    )
    return errors


def sections(path: Path, lines: list[str], root: Path) -> list[str]:
    """Every include resolves: the file exists, and the section it names is in it."""
    errors = []
    for n, line in enumerate(lines, 1):
        match = INCLUDE.search(line)
        if not match:
            continue
        target = root / match.group("path")
        if not target.is_file():
            errors.append(f"{path}:{n}: snippet file {match.group('path')} does not exist")
            continue
        section = match.group("section")
        if section is None:
            continue
        body = target.read_text()
        for marker in ("start", "end"):
            if f"--8<-- [{marker}:{section}]" not in body:
                errors.append(
                    f"{path}:{n}: snippet section '{section}' has no [{marker}:{section}]"
                    f" marker in {match.group('path')}"
                )
    return errors


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    errors = []
    for path in sorted((root / "docs").rglob("*.md")):
        lines = path.read_text().splitlines()
        errors.extend(fences(path, lines))
        errors.extend(sections(path, lines, root))
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        print(f"{len(errors)} problem(s)", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
