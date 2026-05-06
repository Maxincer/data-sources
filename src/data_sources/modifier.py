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


def pad_czce_code(raw_code: str, ref_year: int = None) -> str:
    """CZCE 3-digit to 4-digit: ZC605 -> ZC2605"""
    m = re.search(rf"([a-zA-Z]+)(\d{{3,4}})$", raw_code)
    if not m:
        return raw_code
    if len(m.group(2)) == 4:
        return raw_code
    if ref_year is None:
        import datetime
        ref_year = datetime.datetime.now().year
    w = (ref_year // 10) * 10 + ord(m.group(2)[0]) - 0x30
    candidates = [w - 10, w, w + 10]
    candidates.sort(key=lambda x: abs(x + 3.9 - ref_year))
    return f"{m.group(1)}{candidates[0] % 100:02d}{m.group(2)[1:]}"


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
        code_filter="GFE",
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

    for code, recs in by_code.items():
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
    code_filter: str = "",
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
    """零交易合约的 close 回退为 settle。

    日行情文件只包含有交易的合约。未列出的零交易合约
    没有 close 值，但原表中它们的 close = settle。
    """
    for rec in records:
        if rec.get("close") is None and rec.get("settle") is not None:
            vol = rec.get("volume")
            if vol is None or vol == 0.0:
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
                               code: str = "") -> tuple:
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
