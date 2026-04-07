#!/usr/bin/env python3
import argparse
import asyncio
import gzip
import json
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed


GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_MARKET_PREFIX = "eth-updown-15m-"
REQUEST_HEADERS = {
    "User-Agent": "poly-websocket-bot/1.0",
    "Accept": "application/json",
}


@dataclass(slots=True)
class MarketInfo:
    question: str
    slug: str
    end_time: datetime
    token_ids: list[str]
    outcomes: list[str]
    raw: dict[str, Any] = field(repr=False)


@dataclass(slots=True)
class TokenState:
    outcome: str
    asset_id: str
    best_bid: float | None = None
    best_ask: float | None = None
    last_trade_price: float | None = None
    event_ts_ms: int | None = None
    source: str = "bootstrap"

    @property
    def midpoint(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return round((self.best_bid + self.best_ask) / 2, 6)


class OutputClosed(Exception):
    pass


class HistoryRecorder:
    def __init__(self, save_dir: str | None, compress: bool = True) -> None:
        self.base_dir = Path(save_dir) if save_dir else None
        self.compress = compress
        self.current_slug: str | None = None
        self.events_fp: Any | None = None
        self.snapshots_fp: Any | None = None

    @property
    def enabled(self) -> bool:
        return self.base_dir is not None

    def rotate_market(self, market: MarketInfo) -> None:
        if not self.enabled:
            return
        if self.current_slug == market.slug:
            return
        self.close()
        assert self.base_dir is not None
        self.base_dir.mkdir(parents=True, exist_ok=True)
        market_dir = self.base_dir / market.slug
        market_dir.mkdir(parents=True, exist_ok=True)
        suffix = ".jsonl.gz" if self.compress else ".jsonl"
        self.events_fp = self._open_text_writer(market_dir / f"events{suffix}")
        self.snapshots_fp = self._open_text_writer(market_dir / f"snapshots{suffix}")
        self.current_slug = market.slug

    def _open_text_writer(self, path: Path) -> Any:
        if self.compress:
            return gzip.open(path, "at", encoding="utf-8", compresslevel=6)
        return path.open("a", encoding="utf-8", buffering=1)

    def write_event(self, market: MarketInfo, raw_message: str) -> None:
        if self.events_fp is None:
            return
        self.events_fp.write(
            json.dumps(
                {
                    "recv_ts": iso_now(),
                    "slug": market.slug,
                    "payload": json.loads(raw_message),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        )

    def write_meta(self, market: MarketInfo) -> None:
        if self.events_fp is None:
            return
        self.events_fp.write(
            json.dumps(
                {
                    "recv_ts": iso_now(),
                    "slug": market.slug,
                    "meta": {
                        "question": market.question,
                        "end_time": market.end_time.isoformat().replace("+00:00", "Z"),
                        "outcomes": market.outcomes,
                        "token_ids": market.token_ids,
                    },
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        )

    def write_snapshot(self, snapshot: dict[str, Any]) -> None:
        if self.snapshots_fp is None:
            return
        self.snapshots_fp.write(json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")) + "\n")

    def close(self) -> None:
        for fp in (self.events_fp, self.snapshots_fp):
            if fp is not None:
                fp.close()
        self.events_fp = None
        self.snapshots_fp = None
        self.current_slug = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="订阅 Polymarket 15 分钟 Up/Down 市场的实时价格并持续保存历史。"
    )
    parser.add_argument(
        "--market-prefix",
        default=DEFAULT_MARKET_PREFIX,
        help="自动发现市场用的 slug 前缀，例如 eth-updown-15m- 或 btc-updown-15m-。",
    )
    parser.add_argument(
        "--label",
        help="输出和元数据里附带的流标签；不传则等于 market-prefix。",
    )
    parser.add_argument(
        "--slug",
        help="指定要订阅的 market slug；不传则自动跟踪当前最近未到期的目标市场。",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="输出 JSON Lines，方便程序消费。",
    )
    parser.add_argument(
        "--rollover-grace-seconds",
        type=int,
        default=5,
        help="市场到期后等待多少秒再切换到下一个市场。",
    )
    parser.add_argument(
        "--discover-interval-seconds",
        type=int,
        default=3,
        help="自动模式下，检查是否该切换市场的间隔秒数。",
    )
    parser.add_argument(
        "--save-dir",
        default="history",
        help="历史落盘目录，默认写到 ./history/<slug>/{events,snapshots}.jsonl.gz",
    )
    parser.add_argument(
        "--compress-history",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否将历史文件以 gzip 压缩写入，默认开启。",
    )
    parser.add_argument(
        "--no-save-history",
        action="store_true",
        help="不落盘历史，只输出实时价格。",
    )
    return parser.parse_args()


def parse_embedded_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def iso_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def format_float(value: float | None) -> str:
    return "null" if value is None else f"{value:.3f}"


def parse_market(raw_market: dict[str, Any]) -> MarketInfo:
    return MarketInfo(
        question=raw_market["question"],
        slug=raw_market["slug"],
        end_time=datetime.fromisoformat(raw_market["endDate"].replace("Z", "+00:00")),
        token_ids=list(parse_embedded_json(raw_market["clobTokenIds"])),
        outcomes=list(parse_embedded_json(raw_market["outcomes"])),
        raw=raw_market,
    )


async def fetch_markets(session: aiohttp.ClientSession) -> list[dict[str, Any]]:
    params = {
        "active": "true",
        "closed": "false",
        "limit": "1000",
        "offset": "0",
        "order": "endDate",
        "ascending": "true",
    }
    async with session.get(GAMMA_MARKETS_URL, params=params) as response:
        response.raise_for_status()
        return await response.json()


async def fetch_market_by_slug(session: aiohttp.ClientSession, slug: str) -> MarketInfo:
    params = {"slug": slug}
    async with session.get(GAMMA_MARKETS_URL, params=params) as response:
        response.raise_for_status()
        data = await response.json()

    if isinstance(data, list) and data:
        return parse_market(data[0])
    raise RuntimeError(f"找不到 slug={slug!r} 的市场。")


async def discover_market(
    session: aiohttp.ClientSession,
    market_prefix: str,
    slug: str | None = None,
) -> MarketInfo:
    now = datetime.now(UTC)
    if slug:
        return await fetch_market_by_slug(session, slug)

    raw_markets = await fetch_markets(session)
    markets = [parse_market(raw_market) for raw_market in raw_markets]

    for market in markets:
        if market.slug.startswith(market_prefix) and market.end_time > now:
            return market
    raise RuntimeError(f"没有发现当前可交易的市场: prefix={market_prefix}")


def build_states(market: MarketInfo) -> dict[str, TokenState]:
    return {
        asset_id: TokenState(outcome=outcome, asset_id=asset_id)
        for asset_id, outcome in zip(market.token_ids, market.outcomes, strict=True)
    }


def best_bid_from_book(book_side: list[dict[str, Any]]) -> float | None:
    if not book_side:
        return None
    return max(float(level["price"]) for level in book_side)


def best_ask_from_book(book_side: list[dict[str, Any]]) -> float | None:
    if not book_side:
        return None
    return min(float(level["price"]) for level in book_side)


def update_from_book(item: dict[str, Any], states: dict[str, TokenState]) -> bool:
    state = states.get(item.get("asset_id"))
    if state is None:
        return False

    changed = False
    best_bid = best_bid_from_book(item.get("bids", []))
    best_ask = best_ask_from_book(item.get("asks", []))
    last_trade_price = (
        float(item["last_trade_price"]) if item.get("last_trade_price") is not None else None
    )
    event_ts_ms = int(item["timestamp"]) if item.get("timestamp") is not None else None

    if state.best_bid != best_bid:
        state.best_bid = best_bid
        changed = True
    if state.best_ask != best_ask:
        state.best_ask = best_ask
        changed = True
    if last_trade_price is not None and state.last_trade_price != last_trade_price:
        state.last_trade_price = last_trade_price
        changed = True
    if event_ts_ms is not None:
        state.event_ts_ms = event_ts_ms
    if changed:
        state.source = item.get("event_type", "book")
    return changed


def update_from_best_bid_ask(item: dict[str, Any], states: dict[str, TokenState]) -> bool:
    state = states.get(item.get("asset_id"))
    if state is None:
        return False

    changed = False
    best_bid = float(item["best_bid"]) if item.get("best_bid") is not None else None
    best_ask = float(item["best_ask"]) if item.get("best_ask") is not None else None
    event_ts_ms = int(item["timestamp"]) if item.get("timestamp") is not None else None

    if state.best_bid != best_bid:
        state.best_bid = best_bid
        changed = True
    if state.best_ask != best_ask:
        state.best_ask = best_ask
        changed = True
    if event_ts_ms is not None:
        state.event_ts_ms = event_ts_ms
    if changed:
        state.source = item.get("event_type", "best_bid_ask")
    return changed


def update_from_price_changes(item: dict[str, Any], states: dict[str, TokenState]) -> bool:
    changed = False
    event_ts_ms = int(item["timestamp"]) if item.get("timestamp") is not None else None
    for price_change in item.get("price_changes", []):
        state = states.get(price_change.get("asset_id"))
        if state is None:
            continue
        best_bid = (
            float(price_change["best_bid"]) if price_change.get("best_bid") is not None else None
        )
        best_ask = (
            float(price_change["best_ask"]) if price_change.get("best_ask") is not None else None
        )
        local_changed = False
        if state.best_bid != best_bid:
            state.best_bid = best_bid
            local_changed = True
        if state.best_ask != best_ask:
            state.best_ask = best_ask
            local_changed = True
        if event_ts_ms is not None:
            state.event_ts_ms = event_ts_ms
        if local_changed:
            state.source = item.get("event_type", "price_change")
            changed = True
    return changed


def update_from_last_trade_price(item: dict[str, Any], states: dict[str, TokenState]) -> bool:
    state = states.get(item.get("asset_id"))
    if state is None or item.get("price") is None:
        return False

    last_trade_price = float(item["price"])
    event_ts_ms = int(item["timestamp"]) if item.get("timestamp") is not None else None
    changed = state.last_trade_price != last_trade_price
    state.last_trade_price = last_trade_price
    if event_ts_ms is not None:
        state.event_ts_ms = event_ts_ms
    if changed:
        state.source = item.get("event_type", "last_trade_price")
    return changed


def apply_payload(payload: Any, states: dict[str, TokenState]) -> bool:
    changed = False
    items = payload if isinstance(payload, list) else [payload]
    for item in items:
        if not isinstance(item, dict):
            continue
        event_type = item.get("event_type")
        if event_type == "book":
            changed = update_from_book(item, states) or changed
        elif event_type == "best_bid_ask":
            changed = update_from_best_bid_ask(item, states) or changed
        elif event_type == "price_change":
            changed = update_from_price_changes(item, states) or changed
        elif event_type == "last_trade_price":
            changed = update_from_last_trade_price(item, states) or changed
    return changed


def make_snapshot(market: MarketInfo, states: dict[str, TokenState]) -> dict[str, Any]:
    ordered = [states[token_id] for token_id in market.token_ids]
    latest_event_ts = max((state.event_ts_ms or 0) for state in ordered)
    lag_ms = None
    if latest_event_ts:
        lag_ms = int(datetime.now(UTC).timestamp() * 1000) - latest_event_ts

    outcome_map = {}
    for state in ordered:
        key = state.outcome.lower()
        outcome_map[key] = {
            "asset_id": state.asset_id,
            "best_bid": state.best_bid,
            "best_ask": state.best_ask,
            "mid": state.midpoint,
            "last_trade_price": state.last_trade_price,
            "source": state.source,
            "event_ts_ms": state.event_ts_ms,
        }

    if market.outcomes == ["Up", "Down"]:
        outcome_map["yes"] = outcome_map["up"]
        outcome_map["no"] = outcome_map["down"]

    return {
        "ts": iso_now(),
        "market": {
            "question": market.question,
            "slug": market.slug,
            "end_time": market.end_time.isoformat().replace("+00:00", "Z"),
            "outcomes": market.outcomes,
        },
        "lag_ms": lag_ms,
        "prices": outcome_map,
    }


def format_text_snapshot(snapshot: dict[str, Any]) -> str:
    up = snapshot["prices"].get("up", {})
    down = snapshot["prices"].get("down", {})
    lag = snapshot["lag_ms"]
    lag_text = "null" if lag is None else str(lag)
    label = snapshot.get("label", "stream")
    market_slug = snapshot["market"]["slug"]
    end_time = snapshot["market"]["end_time"]
    return (
        f"{snapshot['ts']} label={label} lag_ms={lag_text} slug={market_slug} end={end_time} "
        f"up(yes) bid={format_float(up.get('best_bid'))} ask={format_float(up.get('best_ask'))} "
        f"mid={format_float(up.get('mid'))} last={format_float(up.get('last_trade_price'))} "
        f"| down(no) bid={format_float(down.get('best_bid'))} ask={format_float(down.get('best_ask'))} "
        f"mid={format_float(down.get('mid'))} last={format_float(down.get('last_trade_price'))}"
    )


def write_line(line: str) -> None:
    try:
        print(line, flush=True)
    except BrokenPipeError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        os.close(devnull)
        raise OutputClosed() from None


def emit_snapshot(
    snapshot: dict[str, Any],
    json_mode: bool,
    last_emitted: str | None,
) -> str:
    line = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")) if json_mode else format_text_snapshot(snapshot)
    if line != last_emitted:
        write_line(line)
    return line


async def subscribe_market(
    websocket: Any,
    market: MarketInfo,
) -> None:
    await websocket.send(
        json.dumps(
            {
                "type": "market",
                "assets_ids": market.token_ids,
                "custom_feature_enabled": True,
            },
            separators=(",", ":"),
        )
    )


async def open_market_ws(market: MarketInfo) -> Any:
    websocket = await websockets.connect(
        WS_MARKET_URL,
        ping_interval=20,
        ping_timeout=20,
        max_queue=None,
        compression=None,
        close_timeout=2,
        user_agent_header=REQUEST_HEADERS["User-Agent"],
    )
    await subscribe_market(websocket, market)
    return websocket


async def run_stream(args: argparse.Namespace) -> None:
    timeout = aiohttp.ClientTimeout(total=10)
    recorder = HistoryRecorder(
        None if args.no_save_history else args.save_dir,
        compress=args.compress_history,
    )
    label = args.label or args.market_prefix
    async with aiohttp.ClientSession(timeout=timeout, headers=REQUEST_HEADERS) as session:
        current_market: MarketInfo | None = None
        websocket: Any | None = None
        states: dict[str, TokenState] = {}
        last_emitted: str | None = None
        next_discover_at = datetime.min.replace(tzinfo=UTC)
        last_wait_notice: str | None = None

        try:
            while True:
                now = datetime.now(UTC)
                should_discover = (
                    current_market is None
                    or websocket is None
                    or now >= next_discover_at
                    or (
                        current_market is not None
                        and now
                        >= current_market.end_time + timedelta(seconds=args.rollover_grace_seconds)
                    )
                )

                if should_discover:
                    try:
                        discovered = await discover_market(
                            session,
                            market_prefix=args.market_prefix,
                            slug=args.slug,
                        )
                    except aiohttp.ClientError:
                        await asyncio.sleep(1)
                        continue
                    except RuntimeError as exc:
                        notice = str(exc)
                        if notice != last_wait_notice:
                            write_line(f"# waiting label={label} reason={notice}")
                            last_wait_notice = notice
                        next_discover_at = now + timedelta(seconds=args.discover_interval_seconds)
                        if websocket is not None:
                            await websocket.close()
                            websocket = None
                        await asyncio.sleep(1)
                        continue
                    next_discover_at = now + timedelta(seconds=args.discover_interval_seconds)
                    last_wait_notice = None
                    if (
                        current_market is None
                        or discovered.slug != current_market.slug
                        or websocket is None
                    ):
                        current_market = discovered
                        states = build_states(current_market)
                        last_emitted = None
                        recorder.rotate_market(current_market)
                        recorder.write_meta(current_market)
                        if websocket is not None:
                            await websocket.close()
                        try:
                            websocket = await open_market_ws(current_market)
                        except OSError:
                            websocket = None
                            await asyncio.sleep(1)
                            continue
                        banner = {
                            "ts": iso_now(),
                            "label": label,
                            "event": "subscribed",
                            "question": current_market.question,
                            "slug": current_market.slug,
                            "end_time": current_market.end_time.isoformat().replace("+00:00", "Z"),
                            "outcomes": current_market.outcomes,
                            "token_ids": current_market.token_ids,
                        }
                        write_line(
                            json.dumps(banner, ensure_ascii=False, separators=(",", ":"))
                            if args.json
                            else (
                                f"# subscribed label={label} slug={current_market.slug} "
                                f"end={banner['end_time']} outcomes={current_market.outcomes} "
                                f"tokens={current_market.token_ids}"
                            )
                        )

                if websocket is None or current_market is None:
                    await asyncio.sleep(0.5)
                    continue

                try:
                    raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except ConnectionClosed:
                    websocket = None
                    await asyncio.sleep(0.5)
                    continue

                recorder.write_event(current_market, raw_message)
                payload = json.loads(raw_message)
                if apply_payload(payload, states):
                    snapshot = make_snapshot(current_market, states)
                    snapshot["label"] = label
                    recorder.write_snapshot(snapshot)
                    last_emitted = emit_snapshot(snapshot, args.json, last_emitted)
        finally:
            if websocket is not None:
                await websocket.close()
            recorder.close()


async def main() -> None:
    args = parse_args()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def handle_stop() -> None:
        stop.set()

    for signame in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signame):
            loop.add_signal_handler(getattr(signal, signame), handle_stop)

    stream_task = asyncio.create_task(run_stream(args))
    stop_task = asyncio.create_task(stop.wait())

    done, pending = await asyncio.wait(
        {stream_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    if stop_task in done and not stream_task.done():
        stream_task.cancel()
        try:
            await stream_task
        except asyncio.CancelledError:
            pass
        return
    await stream_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except OutputClosed:
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(130)
    except ConnectionClosed as exc:
        print(f"websocket 连接关闭: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"运行失败: {exc}", file=sys.stderr)
        sys.exit(1)
