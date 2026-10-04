"""pi exact/fuzzy matching and unchanged-line preservation; diffs use difflib."""

import difflib
import re
import unicodedata


def normalize_lf(text):
    return text.replace("\r\n", "\n").replace("\r", "\n")


def fuzzy(text):
    text = "\n".join(line.rstrip() for line in unicodedata.normalize("NFKC", text).split("\n"))
    for pattern, replacement in (
        (r"[\u2018\u2019\u201a\u201b]", "'"),
        (r"[\u201c\u201d\u201e\u201f]", '"'),
        (r"[\u2010-\u2015\u2212]", "-"),
        (r"[\u00a0\u2002-\u200a\u202f\u205f\u3000]", " "),
    ):
        text = re.sub(pattern, replacement, text)
    return text


def find_match(content, old):
    index = content.find(old)
    if index >= 0:
        return index, len(old), False
    normalized = fuzzy(old)
    # An all-whitespace match must never turn into an empty insertion.
    if not normalized:
        return -1, 0, False
    return fuzzy(content).find(normalized), len(normalized), True


def replace_all(content, replacements, offset=0):
    for start, length, new, _ in reversed(replacements):
        index = start - offset
        content = content[:index] + new + content[index + length :]
    return content


def preserve_lines(original, base, replacements):
    old_lines = re.findall(r"[^\n]*\n|[^\n]+", original)
    base_lines = re.findall(r"[^\n]*\n|[^\n]+", base)
    if len(old_lines) != len(base_lines):
        raise ValueError("Cannot preserve unchanged lines: normalized line counts differ")
    spans, offset = [], 0
    for line in base_lines:
        spans.append((offset, offset + len(line)))
        offset += len(line)
    groups = []
    for change in replacements:
        start, length, _, _ = change
        first = next(i for i, (a, b) in enumerate(spans) if a <= start < b)
        last = first
        while spans[last][1] < start + length:
            last += 1
        if groups and first < groups[-1][1]:
            groups[-1][1] = max(groups[-1][1], last + 1)
            groups[-1][2].append(change)
        else:
            groups.append([first, last + 1, [change]])
    result, previous = [], 0
    for first, last, changes in groups:
        result.extend(old_lines[previous:first])
        begin, end = spans[first][0], spans[last - 1][1]
        result.append(replace_all(base[begin:end], changes, begin))
        previous = last
    return "".join(result + old_lines[previous:])


def apply_edits(content, edits, path):
    edits = [(normalize_lf(e["oldText"]), normalize_lf(e["newText"])) for e in edits]
    if not edits:
        raise ValueError("edits must contain at least one replacement")
    for i, (old, _) in enumerate(edits):
        if not old:
            raise ValueError(f"edits[{i}].oldText must not be empty in {path}.")
    use_fuzzy = any(find_match(content, old)[2] for old, _ in edits)
    base = fuzzy(content) if use_fuzzy else content
    replacements = []
    for i, (old, new) in enumerate(edits):
        index, length, _ = find_match(base, old)
        if index < 0:
            raise ValueError(
                f"Could not find edits[{i}] in {path}. "
                "The oldText must match exactly including whitespace and newlines."
            )
        normalized = fuzzy(old)
        count = fuzzy(base).count(normalized) if normalized else base.count(old)
        if count > 1:
            raise ValueError(
                f"Found {count} occurrences of edits[{i}] in {path}. "
                "Each oldText must be unique. Please provide more context."
            )
        replacements.append((index, length, new, i))
    replacements.sort(key=lambda r: r[0])
    for previous, current in zip(replacements, replacements[1:]):
        if previous[0] + previous[1] > current[0]:
            raise ValueError(
                f"edits[{previous[3]}] and edits[{current[3]}] overlap in {path}. "
                "Merge them into one edit or target disjoint regions."
            )
    result = (
        preserve_lines(content, base, replacements)
        if use_fuzzy
        else replace_all(base, replacements)
    )
    if result == content:
        raise ValueError(f"No changes made to {path}. The replacements produced identical content.")
    return result


def generate_diff(path, old, new):
    # Python's sequence matcher can choose different hunks than JS diff for repeated lines.
    before, after = old.splitlines(keepends=True), new.splitlines(keepends=True)
    matcher = difflib.SequenceMatcher(a=before, b=after, autojunk=False)
    width = len(str(max(len(old.split("\n")), len(new.split("\n")))))
    display, first_changed = [], None
    for group in matcher.get_grouped_opcodes(4):
        if display or group[0][1] > 0:
            display.append(f" {'':>{width}} ...")
        for tag, a, b, c, d in group:
            if tag != "equal" and first_changed is None:
                first_changed = c + 1
            if tag in ("replace", "delete"):
                display.extend(
                    f"-{i + 1:>{width}} {before[i].rstrip(chr(10))}" for i in range(a, b)
                )
            if tag in ("replace", "insert"):
                display.extend(f"+{i + 1:>{width}} {after[i].rstrip(chr(10))}" for i in range(c, d))
            if tag == "equal":
                display.extend(
                    f" {i + 1:>{width}} {before[i].rstrip(chr(10))}" for i in range(a, b)
                )
    patch = []
    for line in difflib.unified_diff(before, after, fromfile=path, tofile=path, n=4):
        patch.append(line if line.endswith("\n") else line + "\n\\ No newline at end of file\n")
    return {"diff": "\n".join(display), "patch": "".join(patch), "firstChangedLine": first_changed}
