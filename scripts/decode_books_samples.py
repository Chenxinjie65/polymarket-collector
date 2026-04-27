from __future__ import annotations

import argparse
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


DEFAULT_DATA_ROOT = "data_all_books_jsonl_live"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Decode a market stream written by stream_all_books_files.py into "
            "readable JSON samples."
        )
    )
    parser.add_argument(
        "--data-root",
        default=DEFAULT_DATA_ROOT,
        help="Collector data root containing all_market_meta.json and either sharded or legacy book files.",
    )
    parser.add_argument(
        "--market-id",
        default="",
        help="Market id to decode. Defaults to the first market found in all_market_meta.json.",
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
        "price_scale": payload.get("price_scale", 1_000_000),
        "size_scale": payload.get("size_scale", 100),
        "write_shards": payload.get("write_shards"),
        "markets": market_map,
    }


def choose_market(catalog: dict[str, Any], requested_market_id: str) -> str:
    if requested_market_id:
        return requested_market_id
    candidates = sorted(catalog["markets"])
    if not candidates:
        raise FileNotFoundError("No markets found in all_market_meta.json")
    return candidates[0]


def load_books_legacy(path: Path) -> list[dict[str, Any]]:
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


def load_books_sharded(data_root: Path, market_meta: dict[str, Any]) -> list[dict[str, Any]]:
    market_index = int(market_meta.get("market_index") or 0)
    write_shard = int(market_meta.get("write_shard") or 0)
    books_root = data_root / "books"
    if not books_root.exists():
        raise FileNotFoundError(f"Sharded books root not found: {books_root}")

    rows: list[dict[str, Any]] = []
    pattern = f"dt=*/hour=*/shard-{write_shard:04d}.jsonl"
    for path in sorted(books_root.glob(pattern)):
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                if int(row.get("m") or 0) == market_index:
                    rows.append(row)
    return rows


def format_scaled_decimal(value: Any, scale: int) -> str | None:
    if value in (None, ""):
        return None
    try:
        decimal_value = Decimal(int(value)) / Decimal(scale)
    except (InvalidOperation, TypeError, ValueError):
        return None
    text = format(decimal_value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def expand_levels(levels: Any, *, price_scale: int, size_scale: int) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for level in levels or []:
        if not isinstance(level, list) or len(level) < 2:
            continue
        price = format_scaled_decimal(level[0], price_scale)
        size = format_scaled_decimal(level[1], size_scale)
        if price is None or size is None:
            continue
        expanded.append({"price": price, "size": size})
    return expanded


def expand_compact_rows(
    *,
    market_meta: dict[str, Any],
    books: list[dict[str, Any]],
    layout_version: int,
    price_scale: int,
    size_scale: int,
) -> list[dict[str, Any]]:
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
        if layout_version >= 6:
            bids = expand_levels(row.get("b", []), price_scale=price_scale, size_scale=size_scale)
            asks = expand_levels(row.get("a", []), price_scale=price_scale, size_scale=size_scale)
        else:
            bids = [
                {"price": level[0], "size": level[1]}
                for level in row.get("b", [])
                if isinstance(level, list) and len(level) >= 2
            ]
            asks = [
                {"price": level[0], "size": level[1]}
                for level in row.get("a", [])
                if isinstance(level, list) and len(level) >= 2
            ]
        expanded.append(
            {
                "market": market_meta.get("market"),
                "asset_id": asset_meta.get("asset_id") if asset_meta else None,
                "asset_index": asset_index,
                "outcome": asset_meta.get("outcome") if asset_meta else None,
                "timestamp": str(row.get("t") or ""),
                "hash": row.get("h", ""),
                "bids": bids,
                "asks": asks,
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
    market_id = choose_market(catalog, args.market_id)
    market_meta = catalog["markets"].get(market_id)
    if market_meta is None:
        raise KeyError(f"Market not found in catalog: {market_id}")

    layout_version = int(catalog.get("layout_version") or 0)
    if layout_version >= 6:
        books = load_books_sharded(data_root, market_meta)
    else:
        books = load_books_legacy(data_root / market_id / "books.jsonl")
    books = expand_compact_rows(
        market_meta=market_meta,
        books=books,
        layout_version=layout_version,
        price_scale=int(catalog.get("price_scale") or 1_000_000),
        size_scale=int(catalog.get("size_scale") or 100),
    )
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
