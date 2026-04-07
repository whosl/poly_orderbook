#!/usr/bin/env python3
import argparse
import gzip
import json
from bisect import bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class MarketMeta:
    question: str
    slug: str
    end_time: str
    outcomes: list[str]
    token_ids: list[str]
    recv_ts: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="回放 polymarket_eth_15m_ws.py 录下的历史文件。"
    )
    parser.add_argument(
        "--base-dir",
        default="history",
        help="历史根目录，默认 ./history",
    )
    parser.add_argument(
        "--slug",
        required=True,
        help="市场 slug，例如 eth-updown-15m-1775529000",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="输出 JSON。",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("meta", help="查看市场元数据和文件统计。")
    subparsers.add_parser("start-book", help="查看订阅开始时捕获到的初始全量 orderbook。")

    snapshot_at = subparsers.add_parser(
        "snapshot-at",
        help="查看某个时间点最近一条历史 top-of-book 快照。",
    )
    snapshot_at.add_argument(
        "--at",
        required=True,
        help="ISO 时间，例如 2026-04-07T02:36:28Z",
    )

    history = subparsers.add_parser(
        "top-history",
        help="输出 top-of-book 历史，支持时间范围和条数限制。",
    )
    history.add_argument("--start", help="起始 ISO 时间。")
    history.add_argument("--end", help="结束 ISO 时间。")
    history.add_argument("--limit", type=int, default=20, help="最多输出多少条，默认 20。")

    return parser.parse_args()


def parse_iso8601(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def history_dir(base_dir: str, slug: str) -> Path:
    return Path(base_dir).expanduser().resolve() / slug


def locate_market_dir(base_dir: Path, slug: str) -> Path:
    direct = base_dir / slug
    if direct.exists():
        return direct
    matches = [path for path in base_dir.rglob(slug) if path.is_dir()]
    if matches:
        return matches[0]
    raise FileNotFoundError(slug)


def resolve_jsonl_path(path: Path) -> Path:
    if path.exists():
        return path
    gz_path = path.with_suffix(path.suffix + ".gz")
    if gz_path.exists():
        return gz_path
    raise FileNotFoundError(path)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    opener = gzip.open if path.suffix.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def load_meta(events_rows: list[dict[str, Any]], slug: str) -> MarketMeta:
    for row in events_rows:
        meta = row.get("meta")
        if meta:
            return MarketMeta(
                question=meta["question"],
                slug=slug,
                end_time=meta["end_time"],
                outcomes=list(meta["outcomes"]),
                token_ids=list(meta["token_ids"]),
                recv_ts=row["recv_ts"],
            )
    raise RuntimeError("events.jsonl 里没有 meta 记录。")


def find_initial_book(events_rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in events_rows:
        payload = row.get("payload")
        if not isinstance(payload, list):
            continue
        if payload and all(item.get("event_type") == "book" for item in payload if isinstance(item, dict)):
            return {
                "recv_ts": row["recv_ts"],
                "payload": payload,
            }
    raise RuntimeError("没有找到初始 full book 事件。")


def format_top_snapshot(snapshot: dict[str, Any]) -> str:
    market = snapshot["market"]
    up = snapshot["prices"].get("up", {})
    down = snapshot["prices"].get("down", {})
    return (
        f"{snapshot['ts']} slug={market['slug']} "
        f"up bid={up.get('best_bid')} ask={up.get('best_ask')} mid={up.get('mid')} "
        f"| down bid={down.get('best_bid')} ask={down.get('best_ask')} mid={down.get('mid')} "
        f"| lag_ms={snapshot.get('lag_ms')}"
    )


def format_book(book_row: dict[str, Any], meta: MarketMeta) -> str:
    token_to_outcome = dict(zip(meta.token_ids, meta.outcomes, strict=True))
    lines = [f"recv_ts={book_row['recv_ts']} slug={meta.slug} question={meta.question}"]
    for item in book_row["payload"]:
        outcome = token_to_outcome.get(item["asset_id"], item["asset_id"])
        lines.append(
            f"[{outcome}] last_trade={item.get('last_trade_price')} "
            f"best_bid={item['bids'][-1]['price'] if item.get('bids') else None} "
            f"best_ask={item['asks'][-1]['price'] if item.get('asks') else None}"
        )
        lines.append("bids:")
        for level in reversed(item.get("bids", [])):
            lines.append(f"  {level['price']} x {level['size']}")
        lines.append("asks:")
        for level in reversed(item.get("asks", [])):
            lines.append(f"  {level['price']} x {level['size']}")
    return "\n".join(lines)


def snapshot_at_or_before(
    snapshots: list[dict[str, Any]],
    target_time: datetime,
) -> dict[str, Any]:
    times = [parse_iso8601(snapshot["ts"]) for snapshot in snapshots]
    idx = bisect_right(times, target_time) - 1
    if idx < 0:
        raise RuntimeError("目标时间早于第一条快照。")
    return snapshots[idx]


def filter_snapshots(
    snapshots: list[dict[str, Any]],
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        ts = parse_iso8601(snapshot["ts"])
        if start and ts < start:
            continue
        if end and ts > end:
            continue
        rows.append(snapshot)
    if limit > 0:
        rows = rows[:limit]
    return rows


def main() -> None:
    args = parse_args()
    market_dir = locate_market_dir(Path(args.base_dir).expanduser().resolve(), args.slug)
    events_path = resolve_jsonl_path(market_dir / "events.jsonl")
    snapshots_path = resolve_jsonl_path(market_dir / "snapshots.jsonl")

    if not events_path.exists():
        raise SystemExit(f"找不到 {events_path}")
    if not snapshots_path.exists():
        raise SystemExit(f"找不到 {snapshots_path}")

    events_rows = load_jsonl(events_path)
    snapshots = load_jsonl(snapshots_path)
    meta = load_meta(events_rows, args.slug)

    if args.command == "meta":
        payload_rows = [row for row in events_rows if "payload" in row]
        result = {
            "slug": meta.slug,
            "question": meta.question,
            "end_time": meta.end_time,
            "outcomes": meta.outcomes,
            "token_ids": meta.token_ids,
            "recording_started_at": meta.recv_ts,
            "event_rows": len(payload_rows),
            "snapshot_rows": len(snapshots),
            "first_snapshot_ts": snapshots[0]["ts"] if snapshots else None,
            "last_snapshot_ts": snapshots[-1]["ts"] if snapshots else None,
        }
        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    if args.command == "start-book":
        book_row = find_initial_book(events_rows)
        if args.json:
            print(json.dumps(book_row, ensure_ascii=False, indent=2))
        else:
            print(format_book(book_row, meta))
        return

    if args.command == "snapshot-at":
        snapshot = snapshot_at_or_before(snapshots, parse_iso8601(args.at))
        if args.json:
            print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        else:
            print(format_top_snapshot(snapshot))
        return

    if args.command == "top-history":
        start = parse_iso8601(args.start) if args.start else None
        end = parse_iso8601(args.end) if args.end else None
        rows = filter_snapshots(snapshots, start, end, args.limit)
        if args.json:
            for row in rows:
                print(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
        else:
            for row in rows:
                print(format_top_snapshot(row))
        return

    raise SystemExit(f"未知命令: {args.command}")


if __name__ == "__main__":
    main()
