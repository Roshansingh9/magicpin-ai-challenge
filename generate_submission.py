"""Generate submission.jsonl from expanded dataset test pairs.

Usage:
    python generate_submission.py --expanded-dir expanded --out submission.jsonl
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from composer import compose


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_context(scope_dir: Path, key: str) -> Optional[Dict[str, Any]]:
    path = scope_dir / f"{key}.json"
    if not path.exists():
        return None
    return load_json(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expanded-dir", default="expanded", help="Path containing categories/, merchants/, customers/, triggers/, test_pairs.json")
    parser.add_argument("--out", default="submission.jsonl", help="Output JSONL file path")
    args = parser.parse_args()

    expanded = Path(args.expanded_dir)
    test_pairs = load_json(expanded / "test_pairs.json").get("pairs", [])

    categories_dir = expanded / "categories"
    merchants_dir = expanded / "merchants"
    customers_dir = expanded / "customers"
    triggers_dir = expanded / "triggers"

    lines = []
    for pair in test_pairs:
        test_id = pair.get("test_id")
        trigger_id = pair.get("trigger_id")
        merchant_id = pair.get("merchant_id")
        customer_id = pair.get("customer_id")
        if not (test_id and trigger_id and merchant_id):
            continue

        trigger = load_context(triggers_dir, str(trigger_id))
        merchant = load_context(merchants_dir, str(merchant_id))
        if not trigger or not merchant:
            continue

        category_slug = merchant.get("category_slug")
        category = load_context(categories_dir, str(category_slug))
        if not category:
            continue

        customer = None
        if customer_id:
            customer = load_context(customers_dir, str(customer_id))

        out = compose(category, merchant, trigger, customer)
        lines.append(
            {
                "test_id": test_id,
                "body": out["body"],
                "cta": out["cta"],
                "send_as": out["send_as"],
                "suppression_key": out["suppression_key"],
                "rationale": out["rationale"],
            }
        )

    out_path = Path(args.out)
    with out_path.open("w", encoding="utf-8") as f:
        for row in lines:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Wrote {len(lines)} lines to {out_path}")


if __name__ == "__main__":
    main()
