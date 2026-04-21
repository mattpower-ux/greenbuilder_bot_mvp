from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize private archive JSONL for editor review")
    parser.add_argument('jsonl_path')
    parser.add_argument('--output', default='./data/private_archive_report.json')
    args = parser.parse_args()

    path = Path(args.jsonl_path)
    docs = []
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))

    category_counts = Counter((d.get('category') or 'Uncategorized') for d in docs)
    policy_counts = Counter((d.get('surface_policy') or 'unknown') for d in docs)
    stale_counts = Counter('stale' if d.get('stale') else 'fresh_or_unflagged' for d in docs)
    year_counts = Counter((str(d.get('published_at', ''))[:4] if d.get('published_at') else 'unknown') for d in docs)

    report = {
        'document_count': len(docs),
        'surface_policy_counts': dict(policy_counts),
        'stale_counts': dict(stale_counts),
        'top_categories': category_counts.most_common(25),
        'top_years': year_counts.most_common(25),
        'sample_titles': [d.get('title') for d in docs[:25]],
    }

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Wrote archive report to {out}')


if __name__ == '__main__':
    main()
