#!/usr/bin/env python3
import os
import re
import json
import argparse

def find_matching_brace(text: str, start_idx: int) -> int:
    """
    Given text[start_idx] == '{', find the index of the matching '}', 
    ignoring braces inside string literals (backticks, single, double).
    """
    brace_count = 1
    i = start_idx + 1
    in_backtick = in_single = in_double = False
    escape = False

    while i < len(text):
        ch = text[i]

        if in_backtick:
            if ch == '`':
                in_backtick = False

        elif in_single:
            if escape:
                escape = False
            elif ch == '\\':
                escape = True
            elif ch == "'":
                in_single = False

        elif in_double:
            if escape:
                escape = False
            elif ch == '\\':
                escape = True
            elif ch == '"':
                in_double = False

        else:
            if ch == '`':
                in_backtick = True
            elif ch == "'":
                in_single = True
            elif ch == '"':
                in_double = True
            elif ch == '{':
                brace_count += 1
            elif ch == '}':
                brace_count -= 1
                if brace_count == 0:
                    return i
        i += 1

    return -1  # no match

def extract_packs(go_text: str) -> list[str]:
    """
    Find all '&CardPack{ ... }' struct literals in go_text and return
    each literal (including its outer braces).
    """
    packs = []
    for m in re.finditer(r"&CardPack\s*{", go_text):
        start = m.end() - 1  # position of '{'
        end = find_matching_brace(go_text, start)
        if end != -1:
            packs.append(go_text[start:end+1])
    return packs

def extract_field(block: str, field_name: str) -> str:
    """
    Extract a quoted string value for `FieldName: "value"` in the block.
    Returns empty string if not found.
    """
    m = re.search(rf"{field_name}\s*:\s*\"([^\"]*)\"", block)
    return m.group(1) if m else ""

def extract_slice_literals(block: str, field_name: str) -> list[str]:
    """
    Given a struct block, locate 'FieldName: []...{ ... }' and pull
    every backtick- or double-quoted literal inside that slice.
    """
    # match e.g. Prompts: []*PromptCard{
    slice_start = re.search(
        rf"{field_name}\s*:\s*\[\s*\*?\]\s*\w+\s*\{{", block, re.DOTALL
    )
    if not slice_start:
        return []
    start = slice_start.end() - 1
    end = find_matching_brace(block, start)
    if end == -1:
        return []

    body = block[start+1:end]
    # find all backtick-literals or double-quoted ones
    raws = re.findall(r"`([^`]*)`|\"([^\"]*)\"", body)
    return [b if b else d for (b, d) in raws]

def parse_pack(block: str) -> dict:
    name = extract_field(block, "Name")
    desc = extract_field(block, "Description")
    prompts = extract_slice_literals(block, "Prompts")
    responses = extract_slice_literals(block, "Responses")
    return {
        "name": name,
        "description": desc,
        "prompts": [{"text": p} for p in prompts],
        "responses": responses
    }

def main():
    p = argparse.ArgumentParser(
        description="Convert Go CardPack literals -> JSON files"
    )
    p.add_argument("go_dir", help="Root directory of Go source (e.g. lib/cardsagainstdiscord)")
    p.add_argument("out_dir", help="Where to write .json files")
    args = p.parse_args()

    if not os.path.isdir(args.go_dir):
        raise SystemExit(f"Error: {args.go_dir} is not a directory")

    os.makedirs(args.out_dir, exist_ok=True)

    found = 0
    for root, _, files in os.walk(args.go_dir):
        for fname in files:
            if not fname.endswith(".go"):
                continue
            path = os.path.join(root, fname)
            with open(path, encoding="utf-8") as f:
                text = f.read()
            for struct in extract_packs(text):
                pack = parse_pack(struct)
                if not pack["name"]:
                    continue
                # sanitize filename
                safe = re.sub(r"\W+", "_", pack["name"].lower()).strip("_")
                outpath = os.path.join(args.out_dir, f"{safe}.json")
                with open(outpath, "w", encoding="utf-8") as outf:
                    json.dump(pack, outf, indent=2, ensure_ascii=False)
                print(f"Wrote {outpath}")
                found += 1

    if found == 0:
        print("No CardPack literals found.")
    else:
        print(f"Done. {found} packs exported.")

if __name__ == "__main__":
    main()