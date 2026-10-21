# -*- coding: utf-8 -*-
"""数据库结构、单位换算规则与连接工具（抓取端与分析端共用）。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "agri.db"

# 仅保留可换算为「斤」的重量单位；按个/袋/箱等计价的非标品不参与波动分析
UNIT_TO_JIN_FACTOR = {
    "斤": 1.0,
    "公斤": 0.5,   # 1 公斤 = 2 斤 ⇒ 元/斤 = 元/公斤 ÷ 2
    "千克": 0.5,
    "kg": 0.5,
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS prices (
    id           INTEGER PRIMARY KEY,   -- 源站记录 id，用于幂等写入
    pub_date     TEXT    NOT NULL,      -- 发布日期 YYYY-MM-DD
    prod_name    TEXT    NOT NULL,      -- 品名
    prod_cat     TEXT,                  -- 一级品类：蔬菜/水果/肉禽蛋/水产/粮油
    prod_pcat    TEXT,                  -- 二级品类：叶菜类/茄果类…
    low_price    REAL,                  -- 最低价（元/unit_info）
    high_price   REAL,                  -- 最高价
    avg_price    REAL,                  -- 均价（分析主用）
    place        TEXT,                  -- 产地
    spec_info    TEXT,                  -- 规格
    unit_info    TEXT                   -- 计价单位：斤/公斤
);

CREATE INDEX IF NOT EXISTS idx_prices_name_date ON prices(prod_name, pub_date);
CREATE INDEX IF NOT EXISTS idx_prices_date      ON prices(pub_date);
CREATE INDEX IF NOT EXISTS idx_prices_cat       ON prices(prod_cat);

-- 抓取进度表，用于断点续传
CREATE TABLE IF NOT EXISTS fetch_log (
    pub_date   TEXT PRIMARY KEY,
    fetched_at TEXT NOT NULL,
    row_count  INTEGER NOT NULL
);
"""


def connect(readonly: bool = False) -> sqlite3.Connection:
    """连接数据库；分析端以只读方式打开更安全。"""
    if readonly:
        return sqlite3.connect(f"file:{DEFAULT_DB}?mode=ro", uri=True)
    return sqlite3.connect(DEFAULT_DB)


def init_db(db_path: Path | None = None) -> sqlite3.Connection:
    """确保表结构存在，返回可写连接。"""
    path = Path(db_path) if db_path else DEFAULT_DB
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn
