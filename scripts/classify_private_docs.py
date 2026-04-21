from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.governance import apply_governance


def main() -> None:
    parser = argparse.ArgumentParser(description="Classify private documents for paraphrase / weight-only / blocked use")
    parser.add_argument("input", help="Input JSONL documents file")
    parser.add_argument("--output", default=None, help="Output JSONL path. Defaults to overwriting input.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path

    docs = []
    counts = {"paraphrase": 0, "weight_only": 0, "blocked": 0, "public": 0}
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            doc = json.loads(line)
            updated = apply_governance(doc)
            counts[updated.get("surface_policy", "public")] = counts.get(updated.get("surface_policy", "public"), 0) + 1
            docs.append(updated)

    with output_path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print(f"Wrote {len(docs)} docs to {output_path}")
    print("Policy counts:", counts)


if __name__ == "__main__":
    main()
