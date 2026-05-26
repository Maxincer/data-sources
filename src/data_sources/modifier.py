#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modifier: 原始数据清洗规则汇总。

所有对交易所原始数据的调整（筛除、代码修正、精度处理等）
集中在此模块，供各 parser 调用。
"""

import math
import re
from typing import Optional


# ===================================================================
# 0. 浮点安全的取整工具
# ===================================================================


def safe_ceil(value: float, tick: float) -> float:
    """先 round 消除 FP 噪声，再向上取整至 tick 整数倍。

    round(6534.000000000001 / 1, 6) → 6534.0 → ceil * 1 → 6534  ✅
    """
    return math.ceil(round(value / tick, 6)) * tick


def safe_floor(value: float, tick: float) -> float:
    """先 round 消除 FP 噪声，再向下取整至 tick 整数倍。"""
    return math.floor(round(value / tick, 6)) * tick

# ===================================================================
# 1. 合约代码修正
# ===================================================================

# CZCE 代码 3→4 位转换
CZCE_TICK_SIZE = {
    "AP": 1.0, "CF": 5.0, "CJ": 1.0, "CY": 5.0, "FG": 1.0,
    "JR": 1.0, "MA": 1.0, "OI": 1.0, "PF": 2.0, "PK": 2.0,
    "PL": 1.0, "PM": 1.0, "PX": 2.0,
    "RI": 1.0, "RM": 1.0, "RS": 1.0,
    "SA": 1.0, "SF": 2.0, "SH": 1.0, "SM": 2.0, "SR": 1.0,
    "TA": 2.0, "UR": 1.0, "WH": 1.0, "ZC": 0.2, "PR": 1.0,
}


def pad_czce_code(raw_code: str, ref_date: datetime.date = None) -> str:
    """CZCE 3-digit to 4-digit: ZC605 -> ZC2605, ZC605.CZC -> ZC2605.CZC.

    Deterministic via listing horizon: valid contracts at any time span
    [ref_date - 1month, ref_date + 12months].  Picks the candidate year
    (decade ± 10 around the year digit) whose delivery month is closest
    to ref_date — exactly one candidate falls within the valid range.
    """
    # Strip exchange suffix (e.g. ".CZC") for matching, re-attach later
    suffix = ""
    if "." in raw_code and raw_code.rsplit(".", 1)[-1].isalpha():
        body, suffix = raw_code.rsplit(".", 1)
        suffix = f".{suffix}"
    else:
        body = raw_code

    m = re.search(rf"([a-zA-Z]+)(\d{{3,4}})$", body)
    if not m:
        return raw_code
    digits = m.group(2)
    if len(digits) == 4:
        return raw_code

    if ref_date is None:
        import datetime as _dt
        ref_date = _dt.date.today()

    year_digit = int(digits[0])
    month = int(digits[1:3])
    decade = (ref_date.year // 10) * 10
    ref_months = ref_date.year * 12 + ref_date.month - 1  # month ordinal

    best_offset: int = 9999
    best_year: int | None = None
    for candidate_year in (decade + year_digit - 10,
                            decade + year_digit,
                            decade + year_digit + 10):
        offset = (candidate_year * 12 + month) - ref_months
        if abs(offset) < abs(best_offset):
            best_offset = offset
            best_year = candidate_year

    return f"{m.group(1)}{best_year % 100:02d}{digits[1:]}{suffix}"


def czc_to_wind_code(code: str) -> str:
    """CZCE 4-digit → 3-digit for Wind WSD query.

    t_futures stores 4-digit CZCE codes (e.g. CF2605.CZC), but Wind
    WSD expects 3-digit (CF605.CZC).  Non-CZCE codes pass through.
    """
    if not code.endswith(".CZC"):
        return code
    m = re.search(r"(\d+)", code)
    if not m:
        return code
    digits = m.group(1)
    if len(digits) == 4:
        return code.replace(digits, digits[1:])
    return code


# ===================================================================
# 2. 筛除规则
# ===================================================================

# CFFEX 有效合约前缀
CFFEX_VALID_PREFIXES = ("IF", "IC", "IH", "IM", "T", "TF", "TL", "TS")

# SHFE 产品代码
SHFE_PRODUCTS = {"CU", "AL", "ZN", "PB", "NI", "SN", "AU", "AG", "RB", "WR",
                 "HC", "SS", "FU", "BU", "BR", "RU", "SP", "OP", "AD", "AO"}

# INE 产品代码
INE_PRODUCTS = {"SC", "BC", "LU", "NR", "EC"}


def is_subtotal_row(code_or_name: str) -> bool:
    """判断是否为 小计/合计 等汇总行"""
    return bool(code_or_name and ("小计" in str(code_or_name) or "合计" in str(code_or_name)))


def is_garbage_row(code: str) -> bool:
    """判断是否为垃圾解析行（CFFEX 结算表中非合约行）"""
    if not code:
        return True
    if "业务参数表" in code or "合约系列" in code:
        return True
    return False


def is_option_contract(code: str) -> bool:
    """判断是否为期权合约"""
    return "-C-" in code or "-P-" in code


def is_tas_contract(code: str) -> bool:
    """判断是否为 TAS (Trade at Settlement) 合约"""
    # TAS 标记在 codes 中以 "TAS" 出现，是结算价交易机制
    return "TAS" in code.upper()


def is_efp_contract(code: str) -> bool:
    """判断是否为 EFP (Exchange for Physical) 合约

    EFP/期转现：场外大宗交易转场内对冲的特殊成交。
    特征：只有成交价和成交量，无 OHLC/结算价/涨跌停。
    DB t_futures_info 中无对应记录，应在入库前过滤。
    """
    return "EFP" in code.upper()


def should_filter_contract(code: str) -> bool:
    """综合判断记录是否应在入库前过滤掉。

    过滤规则：
    1. EFP（期转现）— DB 中无对应记录
    2. TAS（结算价交易）— 无需入库
    """
    if not code:
        return True
    if is_efp_contract(code):
        return True
    if is_tas_contract(code):
        return True
    return False


def should_filter_cffex_code(code: str) -> bool:
    """CFFEX: 只保留 IF/IC/IH/IM/T/TF/TL/TS 期货合约"""
    if not code:
        return True
    if is_garbage_row(code):
        return True
    if is_subtotal_row(code):
        return True
    if is_option_contract(code):
        return True
    if not code.startswith(CFFEX_VALID_PREFIXES):
        return True
    return False


def should_filter_shfe_ine_product(pgid: str, exchange_code: str) -> bool:
    """SHFE: 排除 INE 产品; INE: 排除 SHFE 产品"""
    pgid_up = pgid.upper()
    if exchange_code == "SHF" and pgid_up in INE_PRODUCTS:
        return True
    if exchange_code == "INE" and pgid_up in SHFE_PRODUCTS:
        return True
    return False


def should_filter_tas_from_shfe(pgid: str, exchange_code: str) -> bool:
    """TAS 合约不应归入 SHF（应归 INE）"""
    pgid_up = pgid.upper()
    if exchange_code == "SHF" and pgid_up.startswith("SC") and "TAS" in pgid_up:
        return True
    return False


# ===================================================================
# 3. 数据精度处理
# ===================================================================

PRICE_COLS = ("open", "high", "low", "close")


def zero_price_to_none(rec: dict) -> dict:
    """OHLC 价格为 0 时转为 None"""
    for col in PRICE_COLS:
        if rec.get(col) == 0.0:
            rec[col] = None
    return rec


# ===================================================================
# 4. TAS 合约代码修正
# ===================================================================

TAS_CODE_PREFIXES = {"SC"}  # 目前只有 SC（原油）有 TAS


def fix_tas_code(pgid: str, dmonth: str) -> tuple:
    """TAS 合约代码修正: SC_TAS2606 -> SC2606TAS"""
    pgid_up = pgid.upper()
    if pgid_up.endswith("_TAS"):
        pgid_up = pgid_up.replace("_TAS", "")
        dmonth = dmonth + "TAS"
    return pgid_up, dmonth


# ===================================================================
# 5. TAS 结算价填充
# ===================================================================

def fill_tas_settle(records: list) -> list:
    """填充 TAS 合约的结算价。

    TAS (Trade at Settlement) 合约的结算价 = 同月份常规期货合约的结算价。
    例如 SC2606TAS 的 settle = SC2606 的 settle。
    原始数据中 TAS 行无 SETTLEMENTPRICE 字段，需从常规合约填补。
    """
    # First pass: collect settle by (product, delivery_month)
    settle_map = {}
    for rec in records:
        code = rec.get("code", "")
        if not code or "TAS" in code.upper():
            continue
        settle = rec.get("settle")
        if settle is None:
            settle = rec.get("pre_settle")
        if settle is not None:
            product = "".join(c for c in code.split(".")[0] if c.isalpha())
            dmonth = "".join(c for c in code.split(".")[0] if c.isdigit())
            key = (product.upper(), dmonth)
            settle_map[key] = settle

    # Second pass: fill TAS settle
    for rec in records:
        code = rec.get("code", "")
        if "TAS" not in code.upper():
            continue
        if rec.get("settle") is not None:
            continue
        # TAS code: SC2606TAS -> product=SCTAS, strip TAS -> SC
        raw_code = code.split(".")[0]
        product = "".join(c for c in raw_code if c.isalpha()).upper()
        product = product.replace("TAS", "")  # Strip TAS suffix
        dmonth = "".join(c for c in raw_code if c.isdigit())
        key = (product.upper(), dmonth)
        if key in settle_map:
            rec["settle"] = settle_map[key]
            rec["volume"] = rec.get("volume") or 0.0
            rec["amt"] = rec.get("amt") or 0.0

    return records


# ===================================================================
# 6. 保证金格式统一（转为百分数，与原表一致）

def margin_to_pct(val):
    """Convert margin to percentage: 0.12 -> 12.0, 12% -> 12.0"""
    if val is None:
        return None
    if isinstance(val, str):
        val = val.replace("%", "").replace(",", "").strip()
        try:
            val = float(val)
        except (ValueError, TypeError):
            return None
    if isinstance(val, (int, float)):
        if val < 1:
            return round(val * 100, 2)
        else:
            return round(val, 2)
    return None


# ===================================================================
# 6. DCE 涨跌停价修正：使用开盘限价口径
# ===================================================================

def fix_dce_limit_prices(records: list) -> list:
    """修正 DCE 合约的涨跌停价，使用开盘限价口径。

    DCE 交易参数 API 每次返回的是**当日结算后生效**的涨跌停参数，
    但这包含了节假日调整等非正常风控参数。
    实际开盘时使用的限价应基于**前一交易日**的涨跌停比例。

    同时修正保证金率（same issue）：
      - 节假日调整日 margin 也上调，原表用的是调整前值
      - 逻辑：date T 的 long_margin/short_margin = date T-1 API 原始值
    """
    # Collect DCE tradepara records, group by code
    dce_records = []
    for rec in records:
        code = rec.get("code", "")
        if code.endswith(".DCE") and rec.get("_rise_limit_rate") is not None:
            dce_records.append(rec)

    from collections import defaultdict
    by_code: Dict[str, List[Dict]] = defaultdict(list)
    for rec in dce_records:
        by_code[rec["code"]].append(rec)

    for code, recs in by_code.items():
        recs.sort(key=lambda r: r.get("date", ""))
        prev = None
        for rec in recs:
            cur = {
                "maxup": rec.get("maxup"),
                "maxdown": rec.get("maxdown"),
                "long_margin": rec.get("long_margin"),
                "short_margin": rec.get("short_margin"),
            }

            if prev is not None:
                # 开盘限价+保证金口径：date T 的值 = date T-1 API 原始值
                # 已验证：250/250 DCE 20260429 = 20260428 API 原始值
                rec["maxup"] = prev["maxup"]
                rec["maxdown"] = prev["maxdown"]
                if prev["long_margin"] is not None:
                    rec["long_margin"] = prev["long_margin"]
                    rec["short_margin"] = prev["short_margin"]

            prev = cur

        # 到期合约（最后交易日被交易所摘牌，今日无TradingParams）
        # 用前一天的涨跌停/保证金数据回填今日的 DailyMarket/Settlement 记录
        if prev is not None:
            for rec in records:
                if rec.get("code") == code and rec.get("maxup") is None:
                    rec["maxup"] = prev["maxup"]
                    rec["maxdown"] = prev["maxdown"]
                    if prev["long_margin"] is not None:
                        rec["long_margin"] = prev["long_margin"]
                        rec["short_margin"] = prev["short_margin"]

    # Clean up internal fields
    for rec in records:
        rec.pop("_rise_limit_rate", None)
        rec.pop("_pre_settle", None)
        rec.pop("_last_clear", None)

    return records


# ===================================================================
# 7. GFE 保证金率修正：开盘限价口径
# ===================================================================


def fix_gfe_margin(records: list) -> list:
    """修正 GFE 合约的保证金率，使用开盘限价口径。

    GFE 结算参数 API 支持按日期查询历史数据（数组格式 trade_date），
    但节假日调整会改变 specBuyRate。原表使用调整前的值。

    逻辑：date T 的 margin = date T-1 结算参数表的原始值（投机买/卖字段）
    """
    from collections import defaultdict
    return _fix_margin_inherit(
        records,
        exchange_suffix=".GFE",
        _code_filter="GFE",
    )


# ===================================================================
# 7b. GFE 涨跌停价继承：开盘限价口径
# ===================================================================


def fix_gfe_limit_prices(records: list) -> list:
    """修正 GFE 合约的涨跌停价，使用开盘限价口径。

    GFE 交易参数 API 永远返回最新快照，不支持按日期查询历史数据。
    节假日调整后的涨跌停价不应在节后使用，需继承前一交易日的数据。

    逻辑：按合约分组、按日期排序，date T 的 maxup/maxdown =
          date T-1 的原始值。
    """
    from collections import defaultdict

    gfe_records = [
        r for r in records
        if r.get("code", "").endswith(".GFE")
        and r.get("maxup") is not None
    ]
    by_code: Dict[str, list] = defaultdict(list)
    for rec in gfe_records:
        by_code[rec["code"]].append(rec)

    for _code, recs in by_code.items():
        recs.sort(key=lambda r: r.get("date", ""))
        prev_maxup = None
        prev_maxdown = None
        for rec in recs:
            cur_maxup = rec.get("maxup")
            cur_maxdown = rec.get("maxdown")
            if prev_maxup is not None and prev_maxdown is not None:
                rec["maxup"] = prev_maxup
                rec["maxdown"] = prev_maxdown
            if cur_maxup is not None:
                prev_maxup = cur_maxup
            if cur_maxdown is not None:
                prev_maxdown = cur_maxdown

    return records


# ===================================================================
# 8. 通用保证金率继承修正
# ===================================================================


def fix_all_margin(records: list) -> list:
    """
    通用保证金率假日调整修正（CZC / INE / SHF）。

    DCE 和 GFE 已有专用函数，此函数处理其余交易所。
    逻辑：date T 的 margin = date T-1 的 margin（继承前一日值）
    """
    for suffix in (".CZC", ".INE", ".SHF"):
        records = _fix_margin_inherit(records, exchange_suffix=suffix)
    return records


def _fix_margin_inherit(
    records: list,
    exchange_suffix: str = "",
    _code_filter: str = "",
) -> list:
    """通用保证金继承逻辑。

    对指定交易所的记录，按合约代码分组后按日期排序，
    若当日 margin 与前一交易日不同且变化超过阈值，
    则继承前一交易日的 margin 值。
    """
    from collections import defaultdict

    target_records = []
    for rec in records:
        code = rec.get("code", "")
        if code.endswith(exchange_suffix) and rec.get("long_margin") is not None:
            target_records.append(rec)

    by_code: Dict[str, List[Dict]] = defaultdict(list)
    for rec in target_records:
        by_code[rec["code"]].append(rec)

    for code, recs in by_code.items():
        recs.sort(key=lambda r: r.get("date", ""))
        prev_lm = None
        prev_sm = None
        for rec in recs:
            cur_lm = rec.get("long_margin")
            cur_sm = rec.get("short_margin")

            if prev_lm is not None and cur_lm is not None:
                if abs(cur_lm - prev_lm) > 0.001:
                    rec["long_margin"] = prev_lm
                    rec["short_margin"] = prev_lm if prev_sm is None else prev_sm

            if cur_lm is not None:
                prev_lm = cur_lm
            if cur_sm is not None:
                prev_sm = cur_sm
            else:
                prev_sm = cur_lm

    return records


# ===================================================================
# 9. 零交易合约 close 回退为 settle
# ===================================================================


def fill_zero_volume_close(records: list) -> list:
    """close 为空时回退为 settle。

    即使有少量成交，临近到期合约的 OHLC 也可能为 None，
    但 Wind 等数据源会用 settle 填充 close。
    """
    for rec in records:
        if rec.get("close") is None and rec.get("settle") is not None:
            rec["close"] = rec["settle"]
    return records


# ===================================================================
# 11. 涨跌停板计算的取整规则
# ===================================================================


def calc_czce_limit_prices(pre_settle: float, limit_pct: float,
                           tick_size: Optional[float] = None,
                           code: str = "") -> tuple:
    """CZCE 涨停价/跌停价计算与取整

    涨停价: 向上取整至 tick 整数倍
    跌停价: 向下取整至 tick 整数倍
    """
    if tick_size is None:
        product = "".join(c for c in code.split(".")[0] if c.isalpha())
        tick_size = CZCE_TICK_SIZE.get(product)
    raw_up = pre_settle * (1 + limit_pct)
    raw_down = pre_settle * (1 - limit_pct)
    if tick_size:
        maxup = round(safe_ceil(raw_up, tick_size), 2)
        maxdown = round(safe_floor(raw_down, tick_size), 2)
    else:
        maxup = round(raw_up, 2)
        maxdown = round(raw_down, 2)
    return maxup, maxdown


from decimal import Decimal, ROUND_DOWN


def calc_shfe_ine_limit_prices(pre_settle: float, limit_pct_up: float,
                               limit_pct_down: float, tick_size_real: float,
                               _code: str = "") -> tuple:
    """SHFE/INE 涨停价/跌停价计算与取整

    SHFE/INE 采取向下取整原则：计算结果强制舍去小数，取最小变动价位的
    整数倍中不大于原计算值的最大者。

    真实最小变动价位 = product_tick × 10^(-decimal_number)
    涨停价 = floor(前结算 × (1 + 涨停板幅度) / real_tick) × real_tick
    跌停价 = floor(前结算 × (1 - 跌停板幅度) / real_tick) × real_tick

    使用 Decimal 进行精确运算，避免浮点误差。
    """
    settle = Decimal(str(pre_settle))
    up_rate = Decimal(str(limit_pct_up))
    down_rate = Decimal(str(limit_pct_down))
    tick = Decimal(str(tick_size_real))

    raw_up = settle * (Decimal("1") + up_rate)
    raw_down = settle * (Decimal("1") - down_rate)

    maxup = (raw_up / tick).to_integral_value(rounding=ROUND_DOWN) * tick
    maxdown = (raw_down / tick).to_integral_value(rounding=ROUND_DOWN) * tick

    if maxdown < 0:
        maxdown = Decimal("0")

    return float(maxup), float(maxdown)


# ===================================================================
# 12. CFFEX margin 从历史结算文件中继承
# ===================================================================

import csv
from pathlib import Path


# 辅助：CFFEX 结算文件中百分数字段转浮点

def _pct_val(val: str) -> float | None:
    """将 '12%' 或 '--' 转为小数形式（0.12 表示 12%）。"""
    if not val or val.strip() in ("", "--", "N/A"):
        return None
    try:
        return float(val.replace("%", "").strip()) / 100.0
    except (ValueError, TypeError):
        return None


RAW_DATA_DIR = Path("./data/raw")


def fill_cffex_margin_from_history(records: list) -> list:
    """
    对 CFFEX 记录中 margin 为 NULL 的，从**非今日**的最近
    SettlementParameters 文件中读取 margin 值并填充。

    即使今日发布了新的结算文件，也不使用它填充——
    只用历史文件（非今日），避免用今日尚未完整的结算数据覆盖。
    """
    dates = {r.get("date", "") for r in records if r.get("code", "").endswith(".CFE")}
    if not dates:
        return records

    today = max(dates)

    # 查找非今日的最近 SettlementParameters 文件
    settle_files = sorted(
        f for f in RAW_DATA_DIR.glob("*.CFFEX.SettlementParameters.*")
        if today not in f.name
    )
    if not settle_files:
        return records

    latest = settle_files[-1]  # sorted ascending → last = most recent

    # 解析历史结算文件，建立 {code_prefix → (long_margin, short_margin)}
    margin_map: dict[str, tuple[float | None, float | None]] = {}
    try:
        with open(latest, encoding="gbk", errors="replace") as f:
            lines = f.read().strip().split("\n")
        reader = csv.DictReader(lines[1:])  # skip title line
        for row in reader:
            code = (row.get("期货合约", "") or "").strip()
            if not code:
                continue
            lm = _pct_val(row.get("合约多头保证金标准", ""))
            sm = _pct_val(row.get("合约空头保证金标准", ""))
            if lm is not None or sm is not None:
                margin_map[code] = (lm, sm)
    except Exception:
        return records  # 静默失败

    # 回填 CFFEX 记录中 margin 为空的
    for rec in records:
        code = rec.get("code", "")
        if not code.endswith(".CFE"):
            continue
        if rec.get("long_margin") is not None or rec.get("short_margin") is not None:
            continue
        prefix = code.replace(".CFE", "")
        if prefix in margin_map:
            lm, sm = margin_map[prefix]
            if rec.get("long_margin") is None and lm is not None:
                rec["long_margin"] = lm
            if rec.get("short_margin") is None and sm is not None:
                rec["short_margin"] = sm

    return records


# ===================================================================
# 13. CFFEX 基差（if_basis）计算
# ===================================================================

# 股指期货 → 对应现货指数
_CFFEX_INDEX_MAP = {
    "IF": "000300",  # 沪深300
    "IC": "000905",  # 中证500
    "IH": "000016",  # 上证50
    "IM": "000852",  # 中证1000
}


def fill_if_basis(records: list) -> list:
    """计算 CFFEX 股指期货的基差: if_basis = close - index_close.

    从 records 中查找对应 CSI 指数的 close 值，与 CFFEX 合约的
    close 做差。国债期货（T/TF/TL/TS）无对应指数，跳过。
    """
    # 构建 index_code → close 查找表
    index_close: dict[str, float] = {}
    for rec in records:
        code = rec.get("code", "")
        if not code.endswith(".CSI"):
            continue
        c = rec.get("close")
        if c is not None:
            index_close[code.split(".")[0]] = float(c)

    if not index_close:
        return records

    for rec in records:
        code = rec.get("code", "")
        if not code.endswith(".CFE"):
            continue
        raw = code.split(".")[0]
        product = "".join(c for c in raw if c.isalpha())
        index_code = _CFFEX_INDEX_MAP.get(product)
        if index_code is None:
            continue  # 国债期货，无对应指数
        ic = index_close.get(index_code)
        c = rec.get("close")
        if ic is not None and c is not None:
            rec["if_basis"] = round(float(c) - ic, 4)

    return records
