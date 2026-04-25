from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_DATA_ROOT = "data_all_books_jsonl_live"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Decode a market directory written by stream_all_books_files.py into "
            "readable JSON samples."
        )
    )
    parser.add_argument(
        "--data-root",
        default=DEFAULT_DATA_ROOT,
        help="Collector data root containing all_market_meta.json and market directories.",
    )
    parser.add_argument(
        "--market-id",
        default="",
        help="Market id to decode. Defaults to the first market directory under data root.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5,
        help="How many book rows to show.",
    )
    parser.add_argument(
        "--from-start",
        action="store_true",
        help="Show rows from the beginning instead of the end.",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Optional JSON output file path.",
    )
    return parser.parse_args()


def load_catalog(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Catalog not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    markets = payload.get("markets")
    if not isinstance(markets, list):
        raise ValueError("all_market_meta.json does not contain a valid markets list")
    market_map: dict[str, Any] = {}
    for market in markets:
        market_id = str(market.get("market") or "")
        if market_id:
            market_map[market_id] = market
    return {
        "layout_version": payload.get("layout_version"),
        "storage_format": payload.get("storage_format"),
        "markets": market_map,
    }


def choose_market(data_root: Path, requested_market_id: str) -> str:
    if requested_market_id:
        return requested_market_id
    candidates = sorted(
        path.name
        for path in data_root.iterdir()
        if path.is_dir() and path.name not in {"state", "resolved_market"}
    )
    if not candidates:
        raise FileNotFoundError(f"No market directories found under {data_root}")
    return candidates[0]


def load_books(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"books.jsonl not found: {path}")
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def expand_compact_rows(*, market_meta: dict[str, Any], books: list[dict[str, Any]]) -> list[dict[str, Any]]:
    assets = market_meta.get("assets") or []
    asset_by_index = {
        int(asset.get("asset_index")): asset
        for asset in assets
        if asset.get("asset_index") is not None
    }
    expanded: list[dict[str, Any]] = []
    for row in books:
        if "market" in row and "asset_id" in row:
            expanded.append(row)
            continue
        asset_index = row.get("i")
        try:
            asset_index = int(asset_index)
        except Exception:
            asset_index = None
        asset_meta = asset_by_index.get(asset_index) if asset_index is not None else None
        expanded.append(
            {
                "market": market_meta.get("market"),
                "asset_id": asset_meta.get("asset_id") if asset_meta else None,
                "asset_index": asset_index,
                "outcome": asset_meta.get("outcome") if asset_meta else None,
                "timestamp": row.get("t"),
                "hash": row.get("h", ""),
                "bids": [
                    {"price": level[0], "size": level[1]}
                    for level in row.get("b", [])
                    if isinstance(level, list) and len(level) >= 2
                ],
                "asks": [
                    {"price": level[0], "size": level[1]}
                    for level in row.get("a", [])
                    if isinstance(level, list) and len(level) >= 2
                ],
            }
        )
    return expanded


def build_sample(
    *,
    market_id: str,
    market_meta: dict[str, Any],
    books: list[dict[str, Any]],
    rows: int,
    from_start: bool,
) -> dict[str, Any]:
    rows = max(1, rows)
    total_rows = len(books)
    if from_start:
        sample_rows = books[:rows]
    else:
        sample_rows = books[-rows:]
    return {
        "market": market_id,
        "question": market_meta.get("question"),
        "slug": market_meta.get("slug"),
        "condition_id": market_meta.get("condition_id"),
        "gamma_market_id": market_meta.get("gamma_market_id"),
        "total_rows": total_rows,
        "sample_count": len(sample_rows),
        "rows": sample_rows,
    }


def main() -> int:
    args = parse_args()
    data_root = Path(args.data_root)
    catalog = load_catalog(data_root / "all_market_meta.json")
    market_id = choose_market(data_root, args.market_id)
    market_meta = catalog["markets"].get(market_id)
    if market_meta is None:
        raise KeyError(f"Market not found in catalog: {market_id}")

    books = load_books(data_root / market_id / "books.jsonl")
    books = expand_compact_rows(market_meta=market_meta, books=books)
    payload = build_sample(
        market_id=market_id,
        market_meta=market_meta,
        books=books,
        rows=args.rows,
        from_start=args.from_start,
    )

    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output_file:
        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
