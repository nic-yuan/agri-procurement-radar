#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
新发地农产品批发价格抓取器

数据源：北京新发地市场官方价格发布页（http://www.xinfadi.com.cn/priceDetail.html）
接口  ：POST http://www.xinfadi.com.cn/getPriceData.html

特性：
  - 并发抓取，支持并发度与请求间隔配置（默认礼貌抓取）
  - 指数退避重试，容忍网络抖动与源站限流
  - 断点续传：已抓取日期自动跳过，重复运行只补增量
  - 幂等写入：以源站记录 id 为主键，INSERT OR REPLACE

用法：
  python fetch_xinfadi.py                # 抓取近 3 个月（默认）
  python fetch_xinfadi.py --days 7       # 日常增量更新
  python fetch_xinfadi.py --days 1095 --workers 4 --delay 0.6
                                         # 首次建更大历史库，耗时较长；
                                         # 源站有限流，请保持较低并发

说明：失败日期不会写入 fetch_log，直接重跑即可断点续传补抓。
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import requests

import db  # 与抓取端共享的表结构与单位换算规则

# ---------------------------------------------------------------- 常量配置

API_URL = "http://www.xinfadi.com.cn/getPriceData.html"
PAGE_REFERER = "http://www.xinfadi.com.cn/priceDetail.html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": PAGE_REFERER,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}

# 单次请求最大条数：单日全品种约 475 条，预留充足余量
PAGE_LIMIT = 2000
MAX_RETRY = 4
RETRY_BASE_SLEEP = 1.2

PROJECT_ROOT = db.PROJECT_ROOT
DEFAULT_DB = db.DEFAULT_DB


# ---------------------------------------------------------------- 工具函数


def _to_float(value) -> float | None:
    """源站价格以字符串返回，空值与非数字统一归为 None。"""
    if value is None:
        return None
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def get_done_dates(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT pub_date FROM fetch_log").fetchall()
    return {r[0] for r in rows}


def fetch_one_day(
    session: requests.Session,
    day: date,
) -> list[tuple]:
    """抓取指定日期的全部价格记录，失败时按指数退避重试。"""
    day_str = day.isoformat()
    payload = {
        "limit": PAGE_LIMIT,
        "current": 1,
        "pubDateStartTime": day_str,
        "pubDateEndTime": day_str,
        "prodPcatid": "",
        "prodCatid": "",
        "prodName": "",
    }

    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = session.post(API_URL, data=payload, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("list") or []
            records: list[tuple] = []
            for r in rows:
                avg = _to_float(r.get("avgPrice"))
                # 均价缺失时退化用最低最高价的中值兜底
                if avg is None:
                    lo = _to_float(r.get("lowPrice"))
                    hi = _to_float(r.get("highPrice"))
                    if lo is not None and hi is not None:
                        avg = (lo + hi) / 2
                    else:
                        avg = lo if lo is not None else hi
                if avg is None:
                    continue
                records.append(
                    (
                        int(r["id"]),
                        (r.get("pubDate") or day_str)[:10],
                        (r.get("prodName") or "").strip(),
                        r.get("prodCat") or "",
                        r.get("prodPcat") or "",
                        _to_float(r.get("lowPrice")),
                        _to_float(r.get("highPrice")),
                        avg,
                        r.get("place") or "",
                        r.get("specInfo") or "",
                        r.get("unitInfo") or "斤",
                    )
                )
            return records
        except Exception as exc:  # noqa: BLE001 - 网络层异常统一兜底重试
            last_error = exc
            if attempt < MAX_RETRY:
                time.sleep(RETRY_BASE_SLEEP * (2 ** (attempt - 1)))

    raise RuntimeError(f"抓取 {day_str} 失败（重试 {MAX_RETRY} 次）: {last_error}")


def write_day(conn: sqlite3.Connection, day: date, records: list[tuple]) -> None:
    """单日数据写入 + 进度标记，放在同一事务里保证一致性。"""
    with conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO prices
                (id, pub_date, prod_name, prod_cat, prod_pcat,
                 low_price, high_price, avg_price, place, spec_info, unit_info)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        conn.execute(
            "INSERT OR REPLACE INTO fetch_log (pub_date, fetched_at, row_count) VALUES (?,?,?)",
            (day.isoformat(), time.strftime("%Y-%m-%d %H:%M:%S"), len(records)),
        )


# ---------------------------------------------------------------- 主流程


def main() -> int:
    parser = argparse.ArgumentParser(description="抓取新发地农产品批发价格")
    parser.add_argument("--days", type=int, default=90, help="回溯天数，默认 90（约 3 个月）")
    parser.add_argument("--workers", type=int, default=4, help="并发线程数，默认 4")
    parser.add_argument("--delay", type=float, default=0.6, help="单线程请求间隔秒数，源站有限流，默认 0.6s")
    parser.add_argument("--db", type=str, default=str(DEFAULT_DB), help="SQLite 数据库路径")
    args = parser.parse_args()

    db_path = Path(args.db)
    conn = db.init_db(db_path)
    done = get_done_dates(conn)

    today = date.today()
    targets = [today - timedelta(days=i) for i in range(0, args.days)]
    pending = [d for d in targets if d.isoformat() not in done]

    print(f"[*] 数据库: {db_path}")
    print(f"[*] 目标天数: {args.days}，已完成: {len(targets) - len(pending)}，待抓取: {len(pending)}")

    if not pending:
        print("[✓] 无新增数据，抓取已最新")
        conn.close()
        return 0

    ok, fail = 0, 0
    failed_dates: list[str] = []
    session = requests.Session()

    def worker(day: date):
        return day, fetch_one_day(session, day)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(worker, d) for d in pending]
        for idx, fut in enumerate(as_completed(futures), start=1):
            try:
                day, records = fut.result()
                write_day(conn, day, records)
                ok += 1
                if idx % 50 == 0 or idx == len(pending):
                    print(f"    进度 {idx}/{len(pending)}  成功 {ok}  失败 {fail}")
            except Exception as exc:  # noqa: BLE001
                fail += 1
                failed_dates.append(str(exc))
            if args.delay:
                time.sleep(args.delay)

    conn.close()

    print()
    print(f"[✓] 抓取完成：成功 {ok} 天，失败 {fail} 天")
    if failed_dates:
        print("[!] 失败明细（前 5 条）：")
        for msg in failed_dates[:5]:
            print("   ", msg)
        print("    提示：直接重跑本脚本即可断点续传补抓")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
