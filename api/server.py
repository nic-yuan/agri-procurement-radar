# -*- coding: utf-8 -*-
"""
农产品采购决策台 —— 后端服务

启动：
    uvicorn api.server:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

# 支持 `uvicorn api.server:app`（项目根目录）与 `python api/server.py` 两种运行方式
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "scraper"))   # db 模块
sys.path.insert(0, str(_ROOT / "api"))      # analytics 模块

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

import db
from analytics import (
    DEFAULT_DB,
    get_series_json,
    list_products,
    scan_all,
)

PROJECT_ROOT = _ROOT
WEB_DIR = PROJECT_ROOT / "web"

app = FastAPI(
    title="农产品采购决策台",
    description="面向生鲜电商采购岗的农产品价格波动决策工具",
    version="1.0.0",
)

# ------------------------------------------------------------ 扫描结果缓存

_cache: dict = {"data": None, "stamp": 0.0, "lock": threading.Lock()}
CACHE_TTL = 600  # 秒


def get_scan() -> dict:
    """带 TTL 的扫描缓存，避免每个请求都跑全库计算。"""
    now = time.time()
    with _cache["lock"]:
        if _cache["data"] is None or now - _cache["stamp"] > CACHE_TTL:
            _cache["data"] = scan_all()
            _cache["stamp"] = now
        return _cache["data"]


# ------------------------------------------------------------------- 路由


@app.get("/api/meta")
def api_meta():
    """数据覆盖概览。"""
    data = get_scan()
    return {"meta": data["meta"]}


@app.get("/api/products")
def api_products(
    cat: str = Query("", description="按一级品类过滤"),
    min_days: int = Query(60, description="最少历史天数"),
):
    """可用品种清单及数据完备度。"""
    conn = db.connect(readonly=True)
    try:
        items = [
            p
            for p in list_products(conn)
            if p["days"] >= min_days and (not cat or p["prod_cat"] == cat)
        ]
        return {"count": len(items), "items": items}
    finally:
        conn.close()


@app.get("/api/advisories")
def api_advisories(
    cat: str = Query("", description="按一级品类过滤"),
    level: str = Query("", description="按等级过滤，如 低位/偏低/正常/偏高/高位"),
    limit: int = Query(60, ge=1, le=500),
    order: str = Query("asc", description="asc=便宜优先，desc=昂贵优先"),
):
    """采购时机建议列表。"""
    data = get_scan()
    rows = [
        a for a in data["advisories"]
        if (not cat or a["prod_cat"] == cat) and (not level or a["level"] == level)
    ]
    rows.sort(key=lambda x: x["score"], reverse=(order == "desc"))
    return {"count": len(rows), "items": rows[:limit]}


@app.get("/api/alerts")
def api_alerts(
    cat: str = Query("", description="按一级品类过滤"),
    severity: str = Query("", description="red/orange/yellow"),
    limit: int = Query(40, ge=1, le=200),
):
    """异常波动预警列表。"""
    data = get_scan()
    rows = [
        a for a in data["alerts"]
        if (not cat or a["prod_cat"] == cat) and (not severity or a["severity"] == severity)
    ]
    return {"count": len(rows), "items": rows[:limit]}


@app.get("/api/series")
def api_series(
    name: str = Query(..., description="品种名称，如 大白菜"),
    days: int = Query(365, ge=30, le=2000),
):
    """单品种价格序列 + 决策结论。"""
    try:
        return get_series_json(DEFAULT_DB, name, days=days)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"查询失败: {exc}") from exc


@app.get("/api/refresh")
def api_refresh():
    """强制刷新扫描缓存。"""
    with _cache["lock"]:
        _cache["data"] = scan_all()
        _cache["stamp"] = time.time()
    return {"ok": True, "refreshed_at": time.strftime("%Y-%m-%d %H:%M:%S")}


@app.get("/api/health")
def api_health():
    return {"status": "ok", "ts": time.time()}


# --------------------------------------------------------------- 静态页面

app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")


@app.exception_handler(404)
async def _not_found(request, exc):  # noqa: ANN001
    return JSONResponse(status_code=404, content={"detail": "Not Found"})
