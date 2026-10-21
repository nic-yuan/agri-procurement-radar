# -*- coding: utf-8 -*-
"""
农产品采购决策分析引擎

面向生鲜电商（盒马 / 叮咚买菜 / 小象超市）采购岗，输出两类可执行结论：
  1. 采购时机建议 —— 现在买贵不贵、该买多少、要不要建库存
  2. 异常波动预警 —— 短期异动识别与分级，避免被行情打脸

设计原则：
  - 农产品价格强季节性，因此以「同期（季节）分位数」为主判据，全历史分位数为辅。
    只看绝对价格会得出「冬天的白菜比夏天贵」这类无意义结论。
  - 不同品种波动率差异极大（叶菜 >> 粮油），预警阈值必须自适应，
    故统一采用「相对自身波动率的 Z-score」而非固定涨跌幅。
  - 不做价格预测。本引擎只回答「当前价格处在历史什么位置」这一可验证问题。
"""

from __future__ import annotations

import sqlite3
import statistics
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Sequence

# 允许以 `python analytics.py` 直接运行：把 scraper 目录加入模块搜索路径，
# 从而复用其中的表结构与单位换算规则，避免与抓取端重复定义。
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scraper"))

import db  # noqa: E402

PROJECT_ROOT = db.PROJECT_ROOT
DEFAULT_DB = db.DEFAULT_DB
UNIT_TO_JIN_FACTOR = db.UNIT_TO_JIN_FACTOR

# 目标用户（生鲜电商）真正高频采购的一级品类
TARGET_CATEGORIES = ("蔬菜", "水果", "肉禽蛋", "水产")

# 相邻两条记录相隔超过该天数即视为数据断点，不参与异动判定
MAX_GAP_DAYS = 3

# 波动率地板（日收益率标准差下限，单位 %）。
# 鸡蛋、鸭蛋一类批发价可长期完全不变，其收益率标准差趋近 0，
# 若不设下限，极小的绝对变动也会被放大成数十倍标准差的假预警。
# 取 2.0：接近肉禽蛋与粮油类品种的典型日波动水平。
VOL_FLOOR = 2.0

# Z 值超过该阈值即认为分母已被地板接管，倍数不再具备统计解释力，
# 此时改用定性表述而非输出具体倍数。
Z_INTERPRETABLE = 8.0

# ------------------------------------------------------------------ 数据模型


@dataclass
class PricePoint:
    """单日单品种的归一化价格。"""

    day: date
    price: float  # 元/斤


@dataclass
class TimingAdvice:
    """采购时机建议。"""

    prod_name: str
    prod_cat: str
    unit: str
    place: str
    current_price: float

    # 分位数：0 = 历史最低，1 = 历史最高
    season_quantile: float | None
    hist_quantile: float | None

    # 相对基准：正 = 比基准贵
    vs_season_median: float | None   # 相对往年同期中位数
    vs_hist_median: float | None     # 相对全历史中位数
    mom_7d: float | None             # 相对近 7 日均价
    yoy: float | None                # 同比去年同期同月

    # 结论
    score: float                     # 0（最便宜）~ 1（最贵）
    level: str                       # 见 LEVELS
    action: str
    buy_multiplier: float            # 建议采购倍数（相对常规量）
    reason: str

    history_days: int
    season_sample: int = 0           # 往年同期可比样本数
    mode: str = "hist"               # season=含同期基准 / hist=仅全历史基准
    confidence: str = "低"            # 结论可信度：高 / 中 / 低


@dataclass
class VolatilityAlert:
    """异常波动预警。"""

    prod_name: str
    prod_cat: str
    unit: str
    place: str
    date: date
    price: float
    prev_price: float
    change_pct: float      # 日涨跌幅（%）
    z_score: float | None  # 相对自身近期波动的偏离倍数
    severity: str          # red / orange / yellow
    direction: str         # up / down
    message: str
    cum_3d_pct: float | None = None
    reference_price: float | None = None  # 近 30 日中位数，作为回归参考
    quality: str = "ok"                   # ok=行情异动 / suspect=疑似口径或数据问题


# 采购时机分级表
LEVELS = [
    (0.20, "低位",   "建议加量采购、适度建库存", 1.40),
    (0.40, "偏低",   "建议小幅增量采购",        1.15),
    (0.60, "正常",   "按常规量采购",            1.00),
    (0.80, "偏高",   "建议压缩采购量、缩短锁价", 0.80),
    (1.01, "高位",   "建议减量观望、评估替代产地", 0.60),
]

# 预警严重度排序权重（数值越大越紧急）
SEV_RANK = {"red": 3, "orange": 2, "yellow": 1}


# ------------------------------------------------------------------ 数据加载


def _parse_day(text: str) -> date:
    return datetime.strptime(text[:10], "%Y-%m-%d").date()


def load_series(
    conn: sqlite3.Connection,
    prod_name: str,
    end: date | None = None,
    start: date | None = None,
) -> list[PricePoint]:
    """
    加载单一品种的价格序列。

    关键处理 —— 口径连续性选择：
    源站对同一品种会同时发布多路报价。实测菠菜存在两路并行报价：
    「蒙冀」产地约 1.15 元/斤，另一路无产地标注约 4.0 元/斤，相差 3 倍以上。
    它们本就是不同的货，若对当日全部记录取均值或中位数，
    则任一路货缺报时序列就会跳变，产生「单日暴涨 212%、次日暴跌 50%」
    这类纯属口径切换的假异动，会直接误导采购决策。

    因此逐日选择与前一日价格最接近的报价，使序列尽量沿同一路货延续；
    首日取当日中位数作为起点。价格统一折算为元/斤。
    """
    sql = """
        SELECT pub_date, avg_price, unit_info
        FROM prices
        WHERE prod_name = ? AND avg_price > 0
    """
    params: list = [prod_name]

    if start:
        sql += " AND pub_date >= ?"
        params.append(start.isoformat())
    if end:
        sql += " AND pub_date <= ?"
        params.append(end.isoformat())

    by_day: dict[date, list[float]] = {}
    for day_s, price, unit in conn.execute(sql, params):
        factor = UNIT_TO_JIN_FACTOR.get((unit or "").strip())
        if factor is None or price is None:
            continue
        by_day.setdefault(_parse_day(day_s), []).append(float(price) * factor)

    out: list[PricePoint] = []
    prev: float | None = None
    for d in sorted(by_day):
        vals = by_day[d]
        # 首日取中位数作为起点；其后选择与前一日最接近的报价，延续同一口径
        cur = statistics.median(vals) if prev is None else min(vals, key=lambda v: abs(v - prev))
        out.append(PricePoint(day=d, price=cur))
        prev = cur

    return out


def list_products(conn: sqlite3.Connection) -> list[dict]:
    """列出可用品种及其数据完备度，供前端下拉选择。"""
    sql = """
        SELECT prod_name,
               MAX(prod_cat)  AS prod_cat,
               MAX(prod_pcat) AS prod_pcat,
               MIN(pub_date)  AS first_date,
               MAX(pub_date)  AS last_date,
               COUNT(DISTINCT pub_date) AS days
        FROM prices
        WHERE avg_price > 0
        GROUP BY prod_name
    """
    rows = []
    for name, cat, pcat, first, last, days in conn.execute(sql):
        rows.append(
            {
                "prod_name": name,
                "prod_cat": cat or "",
                "prod_pcat": pcat or "",
                "first_date": first,
                "last_date": last,
                "days": days,
            }
        )
    rows.sort(key=lambda r: (r["prod_cat"], -r["days"]))
    return rows


# ------------------------------------------------------------------ 统计工具


def percentile_rank(values: Sequence[float], x: float) -> float | None:
    """x 在样本中的百分位（0~1），采用中位秩以平滑重复值。"""
    if not values:
        return None
    below = sum(1 for v in values if v < x)
    equal = sum(1 for v in values if v == x)
    return (below + 0.5 * equal) / len(values)


def moving_stats(series: list[PricePoint], window: int) -> tuple[float | None, float | None]:
    """最近 window 个点的均值与日收益率标准差（%）。"""
    if len(series) < 2:
        return (None, None)
    tail = [p.price for p in series[-window:]]
    mean = statistics.fmean(tail)
    if len(tail) < 3:
        return (mean, None)
    rets = []
    for a, b in zip(tail, tail[1:]):
        if a > 0:
            rets.append((b - a) / a * 100.0)
    if len(rets) < 2:
        return (mean, None)
    return (mean, statistics.stdev(rets))


# ------------------------------------------------------------------ 核心算法


def analyze_timing(
    series: list[PricePoint],
    prod_name: str = "",
    prod_cat: str = "",
    unit: str = "斤",
    place: str = "",
    lookback_years: int = 3,
    season_half_window: int = 21,
) -> TimingAdvice | None:
    """
    采购时机建议。

    评分构成：
        score = 0.50 × 季节分位数 + 0.30 × 全历史分位数 + 0.20 × 动量项
        动量项 = 0.5 + clamp(相对近 7 日均值的偏离, -0.5, +0.5)

    季节分位数取「过去 lookback_years 年、同一日历日 ±season_half_window 天」的样本，
    用于回答「在一年中的这个时点，现在的价格算贵还是便宜」——这才是采购真正关心的。
    """
    if len(series) < 30:
        return None

    today = series[-1].day
    current = series[-1].price
    cutoff = today - timedelta(days=365 * lookback_years)

    hist = [p.price for p in series if p.day >= cutoff] or [p.price for p in series]

    # 季节窗口：往年同日历日 ±21 天
    season: list[float] = []
    month, day_of_month = today.month, today.day
    for year_offset in range(1, lookback_years + 1):
        try:
            anchor = date(today.year - year_offset, month, day_of_month)
        except ValueError:  # 2 月 29 日等边界
            continue
        lo = anchor - timedelta(days=season_half_window)
        hi = anchor + timedelta(days=season_half_window)
        season.extend(p.price for p in series if lo <= p.day <= hi)

    q_season = percentile_rank(season, current) if len(season) >= 20 else None
    q_hist = percentile_rank(hist, current)

    med_season = statistics.median(season) if len(season) >= 20 else None
    med_hist = statistics.median(hist)

    ma7, _sigma = moving_stats(series, 7)
    vs_season = ((current - med_season) / med_season * 100.0) if med_season else None
    vs_hist = ((current - med_hist) / med_hist * 100.0) if med_hist else None
    mom7 = ((current - ma7) / ma7 * 100.0) if ma7 else None

    # 同比：去年同期同月均价
    yoy = None
    try:
        last_year = today.year - 1
        lo = date(last_year, month, 1)
        hi = date(last_year, month, 28) + timedelta(days=4)
        hi = hi.replace(day=1) - timedelta(days=1)  # 该月最后一天
        same = [p.price for p in series if lo <= p.day <= hi]
        if same:
            base = statistics.fmean(same)
            if base > 0:
                yoy = (current - base) / base * 100.0
    except ValueError:
        pass

    # ---- 综合评分 ----
    # 季节基准不可用时（历史不足一年），把权重转移给全历史分位与动量，
    # 避免用「全历史分位」冒充「同期分位」却仍按季节口径加权。
    if q_season is not None:
        w_season, w_hist, w_mom = 0.50, 0.30, 0.20
        q_season_eff: float = q_season
        mode = "season"
    else:
        w_season, w_hist, w_mom = 0.00, 0.65, 0.35
        q_season_eff = q_hist
        mode = "hist"

    if mom7 is None:
        momentum_term = 0.5
    else:
        momentum_term = 0.5 + max(-0.5, min(0.5, mom7 / 30.0))

    score = w_season * q_season_eff + w_hist * q_hist + w_mom * momentum_term
    score = max(0.0, min(1.0, score))

    level, action, mult = "正常", "按常规量采购", 1.00
    for threshold, lvl, act, m in LEVELS:
        if score < threshold:
            level, action, mult = lvl, act, m
            break

    # ---- 理由生成：用最显著的那条信号 ----
    reason = _build_reason(q_season, vs_season, q_hist, vs_hist, mom7, yoy, level, mode)

    if mode == "season":
        confidence = "高" if len(season) >= 100 else "中"
    else:
        confidence = "中" if len(series) >= 90 else "低"

    return TimingAdvice(
        prod_name=prod_name,
        prod_cat=prod_cat,
        unit=unit,
        place=place,
        current_price=round(current, 2),
        season_quantile=round(q_season, 3) if q_season is not None else None,
        hist_quantile=round(q_hist, 3),
        vs_season_median=round(vs_season, 1) if vs_season is not None else None,
        vs_hist_median=round(vs_hist, 1) if vs_hist is not None else None,
        mom_7d=round(mom7, 1) if mom7 is not None else None,
        yoy=round(yoy, 1) if yoy is not None else None,
        score=round(score, 3),
        level=level,
        action=action,
        buy_multiplier=mult,
        reason=reason,
        history_days=len(series),
        season_sample=len(season),
        mode=mode,
        confidence=confidence,
    )


def _build_reason(
    q_season: float | None,
    vs_season: float | None,
    q_hist: float | None,
    vs_hist: float | None,
    mom7: float | None,
    yoy: float | None,
    level: str,
    mode: str = "hist",
) -> str:
    """挑选最强的一条信号说人话，避免输出一坨数字。"""
    parts: list[str] = []

    if q_season is not None:
        pct = q_season * 100
        if pct <= 20:
            parts.append(f"当前价处于往年同期最低 {pct:.0f}% 区间")
        elif pct >= 80:
            parts.append(f"当前价处于往年同期最高 {100 - pct:.0f}% 区间")
        else:
            parts.append(f"当前价处于往年同期 {pct:.0f}% 分位")
        if vs_season is not None:
            direction = "高于" if vs_season >= 0 else "低于"
            parts.append(f"{direction}同期中位数 {abs(vs_season):.1f}%")
    else:
        if vs_hist is not None:
            direction = "高于" if vs_hist >= 0 else "低于"
            parts.append(f"{direction}历史中位数 {abs(vs_hist):.1f}%")

    if mom7 is not None and abs(mom7) >= 8:
        trend = "上涨" if mom7 > 0 else "下跌"
        parts.append(f"近 7 日{trend} {abs(mom7):.1f}%")

    if yoy is not None and abs(yoy) >= 20:
        direction = "高于" if yoy >= 0 else "低于"
        parts.append(f"同比{direction}去年同期 {abs(yoy):.1f}%")

    if not parts:
        parts.append("价格处于历史正常波动区间")

    text = f"{level}：{'，'.join(parts)}"
    if mode == "hist":
        text += "（历史不足一年，暂无往年同期基准，当前判断仅基于近三个月数据）"
    return text


def detect_alerts(
    series: list[PricePoint],
    prod_name: str = "",
    prod_cat: str = "",
    unit: str = "斤",
    place: str = "",
    vol_window: int = 60,
    scan_days: int = 5,
) -> list[VolatilityAlert]:
    """
    异常波动预警。

    判据（满足任一即触发，取较高等级）：
      🔴 一级：|Z| ≥ 4.0 或 |日涨跌| ≥ 20%
      🟠 二级：|Z| ≥ 2.8 或 |日涨跌| ≥ 12%
      🟡 三级：|Z| ≥ 2.0 或 |日涨跌| ≥  8%

    绝对涨跌幅阈值偏高，是因为批发端日常波动本就不小，
    阈值过低会产出大量对采购无决策价值的噪声预警。

    Z 为当日涨跌相对该品种近 vol_window 日波动分布的偏离倍数，
    用以适配叶菜与粮油之间数量级的波动差异。

    每个品种最终最多返回一条预警。
    """
    if len(series) < vol_window // 2 + 5:
        return []

    alerts: list[VolatilityAlert] = []

    # 只扫描最近 scan_days 天：更早的异动决策窗口已过，对当下采购无意义
    scan_start = max(1, len(series) - scan_days)
    for i in range(scan_start, len(series)):
        prev, cur = series[i - 1], series[i]
        if prev.price <= 0:
            continue
        # 相邻记录跨天过多说明中间缺数据，此时「日涨跌」实为多日累计涨跌，
        # 与正常日频涨跌不同口径，直接剔除以免误报。
        if (cur.day - prev.day).days > MAX_GAP_DAYS:
            continue

        change = (cur.price - prev.price) / prev.price * 100.0

        lookback = series[max(0, i - vol_window): i]
        rets = [
            (b.price - a.price) / a.price * 100.0
            for a, b in zip(lookback, lookback[1:])
            if a.price > 0 and (b.day - a.day).days <= MAX_GAP_DAYS
        ]

        z: float | None = None
        if len(rets) >= 10:
            mu = statistics.fmean(rets)
            sd = max(statistics.stdev(rets), VOL_FLOOR)
            z = (change - mu) / sd

        severity = _classify(change, z)
        if severity is None:
            continue

        # 锯齿识别：单日剧烈变动后次日大幅反向，通常反映产地/规格口径切换
        # 或数据录入问题，而非真实行情。降级并标注，避免采购被假异动误导。
        quality = "ok"
        if abs(change) >= 40.0 and i + 1 < len(series) and cur.price > 0:
            back = (series[i + 1].price - cur.price) / cur.price * 100.0
            if back * change < 0 and abs(back) >= 25.0:
                quality = "suspect"
                severity = "yellow"

        # 近 3 日累计涨跌
        cum3: float | None = None
        if i >= 3:
            base = series[i - 3].price
            if base > 0:
                cum3 = (cur.price - base) / base * 100.0

        ref = None
        tail30 = [p.price for p in series[max(0, i - 30): i]]
        if tail30:
            ref = round(statistics.median(tail30), 2)

        direction = "up" if change > 0 else "down"
        verb = "暴涨" if change > 0 else "暴跌"
        msg = f"{cur.day.isoformat()} {verb} {abs(change):.1f}%"
        if quality == "suspect":
            msg += "（疑似产地/规格口径切换，非真实行情）"
        elif z is not None:
            if abs(z) >= Z_INTERPRETABLE:
                msg += "（远超该品种常态波动范围）"
            else:
                msg += f"（偏离自身常态波动 {abs(z):.1f} 倍标准差）"
        if cum3 is not None and abs(cum3) >= 25:
            msg += f"；近 3 日累计{'涨' if cum3 > 0 else '跌'} {abs(cum3):.1f}%"

        alerts.append(
            VolatilityAlert(
                prod_name=prod_name,
                prod_cat=prod_cat,
                unit=unit,
                place=place,
                date=cur.day,
                price=round(cur.price, 2),
                prev_price=round(prev.price, 2),
                change_pct=round(change, 1),
                z_score=round(z, 2) if z is not None else None,
                severity=severity,
                direction=direction,
                message=msg,
                cum_3d_pct=round(cum3, 1) if cum3 is not None else None,
                reference_price=ref,
                quality=quality,
            )
        )

    if not alerts:
        return []
    # 同一品种只保留一条：取窗口内严重度最高者，并列时取日期更新者。
    # 否则同一品种连续多日的行情会把预警列表刷屏，采购无法聚焦。
    return [max(alerts, key=lambda a: (SEV_RANK[a.severity], a.date))]


def _classify(change: float, z: float | None) -> str | None:
    """
    按最高匹配等级返回预警级别（满足任一即触发，取较高等级）。

      🔴 red    ：|日涨跌| ≥ 20% 或 |Z| ≥ 4.0
      🟠 orange ：|日涨跌| ≥ 12% 或 |Z| ≥ 2.8
      🟡 yellow ：|日涨跌| ≥  8% 或 |Z| ≥ 2.0

    绝对涨跌幅阈值定得偏高，是因为批发端日常波动本就不小；
    阈值过低会产出大量对采购无决策价值的噪声预警。
    """
    a = abs(change)
    az = abs(z) if z is not None else 0.0

    if az >= 4.0 or a >= 20.0:
        return "red"
    if az >= 2.8 or a >= 12.0:
        return "orange"
    if az >= 2.0 or a >= 8.0:
        return "yellow"
    return None


# ------------------------------------------------------------------ 批量入口


def get_latest_meta(conn: sqlite3.Connection, prod_name: str) -> tuple[str, str]:
    """取该品种最近一条记录的单位与产地。"""
    row = conn.execute(
        "SELECT unit_info, place FROM prices WHERE prod_name = ? ORDER BY pub_date DESC LIMIT 1",
        (prod_name,),
    ).fetchone()
    if not row:
        return ("斤", "")
    return (row[0] or "斤", row[1] or "")


def scan_all(
    db_path: Path = DEFAULT_DB,
    categories: Sequence[str] = TARGET_CATEGORIES,
    min_days: int = 60,
    top_alerts: int = 40,
) -> dict:
    """
    全库扫描，输出看板所需的全部数据：
      - advisories：各品种采购时机建议（按 score 升序，越便宜越靠前）
      - alerts    ：异常波动预警（按严重度与偏离度排序）
      - meta      ：数据覆盖情况
    """
    conn = db.connect(readonly=True)
    try:
        products = [p for p in list_products(conn) if p["prod_cat"] in categories]
        advisories: list[dict] = []
        alerts: list[VolatilityAlert] = []

        for p in products:
            if p["days"] < min_days:
                continue
            series = load_series(conn, p["prod_name"])
            if len(series) < min_days:
                continue

            unit, place = get_latest_meta(conn, p["prod_name"])
            advice = analyze_timing(
                series,
                prod_name=p["prod_name"],
                prod_cat=p["prod_cat"],
                unit=unit,
                place=place,
            )
            if advice:
                advisories.append(advice.__dict__)

            alerts.extend(
                detect_alerts(series, prod_name=p["prod_name"], prod_cat=p["prod_cat"], unit=unit, place=place)
            )

        advisories.sort(key=lambda x: x["score"])

        alerts.sort(
            key=lambda a: (-SEV_RANK[a.severity], -(abs(a.z_score or 0)))
        )

        coverage = conn.execute(
            "SELECT MIN(pub_date), MAX(pub_date), COUNT(DISTINCT prod_name), COUNT(*) FROM prices"
        ).fetchone()

        return {
            "meta": {
                "source": "北京新发地市场官方价格发布",
                "source_url": "http://www.xinfadi.com.cn/priceDetail.html",
                "first_date": coverage[0],
                "last_date": coverage[1],
                "product_count": coverage[2],
                "record_count": coverage[3],
                "analyzed_count": len(advisories),
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
            "advisories": advisories,
            "alerts": [a.__dict__ for a in alerts[:top_alerts]],
        }
    finally:
        conn.close()


def get_series_json(
    db_path: Path,
    prod_name: str,
    days: int = 365,
) -> dict:
    """返回单品种价格序列，供前端绘图。"""
    conn = db.connect(readonly=True)
    try:
        end = conn.execute("SELECT MAX(pub_date) FROM prices").fetchone()[0]
        end_d = _parse_day(end) if end else date.today()
        start_d = end_d - timedelta(days=days)
        series = load_series(conn, prod_name, start=start_d)

        unit, place = get_latest_meta(conn, prod_name)
        cat_row = conn.execute(
            "SELECT prod_cat FROM prices WHERE prod_name = ? LIMIT 1", (prod_name,)
        ).fetchone()
        prod_cat = (cat_row[0] or "") if cat_row else ""

        advice = analyze_timing(
            series, prod_name=prod_name, prod_cat=prod_cat, unit=unit, place=place
        )

        return {
            "prod_name": prod_name,
            "prod_cat": prod_cat,
            "unit": unit,
            "place": place,
            "points": [{"d": p.day.isoformat(), "p": round(p.price, 3)} for p in series],
            "advice": advice.__dict__ if advice else None,
        }
    finally:
        conn.close()


if __name__ == "__main__":
    import json

    result = scan_all()
    print(json.dumps(result["meta"], ensure_ascii=False, indent=2))
    print(f"\n采购时机建议 {len(result['advisories'])} 条，预警 {len(result['alerts'])} 条\n")
    print("=== 最值得买（价格最低位）Top 8 ===")
    for a in result["advisories"][:8]:
        print(
            f"  {a['prod_name']:<8} {a['current_price']:>6} 元/{a['unit']}  "
            f"分位{a['score']:.2f}  {a['level']}  建议{a['buy_multiplier']}x"
        )
    print("\n=== 最该警惕（价格最高位）Top 8 ===")
    for a in result["advisories"][-8:][::-1]:
        print(
            f"  {a['prod_name']:<8} {a['current_price']:>6} 元/{a['unit']}  "
            f"分位{a['score']:.2f}  {a['level']}  建议{a['buy_multiplier']}x"
        )
    print("\n=== 波动预警 ===")
    for al in result["alerts"][:10]:
        print(f"  [{al['severity']:<6}] {al['prod_name']:<8} {al['message']}")
