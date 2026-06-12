#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parse raw exchange data files into unified daily bar records.

Each parser function reads a raw file from data/raw/ and yields
dicts with keys matching t_futures_info columns.

Stat tracking: total_records, missing count per field, missing deviation.
"""

import csv
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from data_sources.modifier import (
    safe_ceil,
    safe_floor,
    calc_shfe_ine_limit_prices,
    zero_price_to_none,
)

# Exchange product sets for cross-exchange filtering
SHFE_PRODUCTS = {"CU","AL","ZN","PB","NI","SN","AU","AG","RB","WR",
                 "HC","SS","FU","BU","BR","RU","SP","OP","AD","AO"}
INE_PRODUCTS = {"SC","BC","LU","NR","EC"}


RAW_DIR = Path(os.environ.get("DATA_DIR", "./data")) / "raw"

# Exchange code suffixes for DB
SUFFIX_MAP = {
    "CFE": ".CFE", "SHF": ".SHF", "CZC": ".CZC",
    "DCE": ".DCE", "GFE": ".GFE", "INE": ".INE",
    "CSI": ".CSI",
}

# DB columns in order
DB_COLS = [
    "code", "date", "open", "high", "low", "close",
    "volume", "amt", "oi", "settle", "maxup", "maxdown",
    "if_basis", "long_margin", "short_margin", "minoq", "maxoq",
]


class ParseStats:
    """Track parsing statistics per exchange."""

    def __init__(self, exchange: str, data_type: str):
        self.exchange = exchange
        self.data_type = data_type
        self.total = 0                     # total parsed records
        self.missing: Dict[str, int] = {}  # field -> count of missing values
        self.errors: List[str] = []        # error messages

    def add_record(self, record: Dict):
        """Count a record and its missing fields."""
        self.total += 1
        for col in DB_COLS:
            val = record.get(col)
            if val is None or (isinstance(val, float) and val != val):
                self.missing[col] = self.missing.get(col, 0) + 1

    def add_error(self, msg: str):
        self.errors.append(msg)

    def merge(self, other: "ParseStats"):
        self.total += other.total
        for col, cnt in other.missing.items():
            self.missing[col] = self.missing.get(col, 0) + cnt
        self.errors.extend(other.errors)

    @property
    def stats_summary(self) -> Dict:
        # Fields that are naturally absent for this exchange (not an anomaly)
        _EXPECTED_NULLS = {
            "CFFEX": set(),
            "CZCE": {"if_basis"},
            "DCE": {"if_basis"},
            "SHFE": {"if_basis"},
            "INE": {"if_basis"},
            "GFEX": {"if_basis"},
            "CSI": {"if_basis", "long_margin", "short_margin", "minoq", "maxoq"},
        }
        expected = _EXPECTED_NULLS.get(self.exchange, set())
        return {
            "exchange": self.exchange,
            "data_type": self.data_type,
            "total_records": self.total,
            "missing": {k: v for k, v in self.missing.items()
                        if v > 0 and k not in expected},
            "errors": len(self.errors),
        }


# -------------------------------------------------------------------------
# Verification helpers
# -------------------------------------------------------------------------

def verify_price_order(record: Dict) -> Optional[str]:
    """Check open/high/low/close price ordering."""
    o, h, l, c = [record.get(k) for k in ("open", "high", "low", "close")]
    if o is not None and h is not None and l is not None:
        if l > h:
            return "low exceeds high"
        if o is not None and (o > h or o < l):
            return "open outside high/low range"
        if c is not None and (c > h or c < l):
            return "close outside high/low range"
    return None


def verify_settle_vs_close(record: Dict) -> Optional[str]:
    """Check settle is close to close (within 5%)."""
    s, c = record.get("settle"), record.get("close")
    if s is not None and c is not None and c != 0:
        dev = abs(s - c) / abs(c) * 100
        if dev > 5:
            return f"settle deviates {dev:.2f}% from close"
    return None


# -------------------------------------------------------------------------
# Exchange-specific parsers
# -------------------------------------------------------------------------

def parse_directory(dirpath: Path) -> List[Dict]:
    """Auto-discover and parse all raw files in data/raw/."""
    records = []
    for fpath in sorted(dirpath.iterdir()):
        if fpath.suffix == ".jsonl":
            continue
        try:
            parsed = parse_file(fpath)
            records.extend(parsed)
        except Exception:
            continue
    return records


def parse_file(fpath: Path) -> List[Dict]:
    """
    Parse a single raw file and return list of record dicts.
    If verifier is provided, run business-logic checks and log issues.
    """
    name = fpath.name
    if "CFFEX" in name and "MarketData" in name:
        return _parse_cffex_market(fpath)
    if "CFFEX" in name and "Settlement" in name:
        return _parse_cffex_settlement(fpath)
    if ("SHFE" in name or "INE" in name) and "DailyMarketData" in name:
        return _parse_shfe_ine_daily(fpath)
    if ("SHFE" in name or "INE" in name) and "Settlement" in name:
        return _parse_shfe_ine_settlement(fpath)
    if "CZCE" in name and "DailyMarketData" in name:
        return _parse_czce_daily(fpath)
    if "CZCE" in name and "Settlement" in name:
        return _parse_czce_settlement(fpath)
    if "CZCE" in name and "TradingParameters" in name:
        return _parse_czce_tradepara(fpath)
    if "DCE" in name and "DailyMarketData" in name:
        return _parse_dce_daily(fpath)
    if "DCE" in name and "Settlement" in name:
        return _parse_dce_settlement(fpath)
    if "GFEX" in name and "DailyMarketData" in name:
        return _parse_gfex_daily(fpath)
    if "GFEX" in name and "Settlement" in name:
        return _parse_gfex_settlement(fpath)
    # Trading parameters
    if "CFFEX" in name and "TradingParameters" in name:
        return _parse_cffex_tradepara(fpath)
    if "SHFE" in name and "TradingParameters" in name:
        return _parse_shfe_ine_tradepara(fpath, ".SHF")
    if "INE" in name and "TradingParameters" in name:
        return _parse_shfe_ine_tradepara(fpath, ".INE")
    if "DCE" in name and "TradingParameters" in name and "Variety" not in name:
        return _parse_dce_tradepara(fpath)
    if "DCE" in name and "VarietyTradingParam" in name:
        return _parse_dce_tradingparam(fpath)
    if "GFEX" in name and "TradingParameters" in name:
        return _parse_gfex_tradepara(fpath)
    if "CSI" in name and "MarketData" in name:
        return _parse_csi_market(fpath)
    return []


def _exchange_suffix(exchange: str) -> str:
    """Map exchange name to DB code suffix."""
    m = {"SHFE": "SHF", "CFFEX": "CFE", "CZCE": "CZC",
         "DCE": "DCE", "GFEX": "GFE", "INE": "INE"}
    return SUFFIX_MAP.get(m.get(exchange, exchange), f".{exchange}")


# -----------------------------------------------------------------------
# CFFEX market CSV (GBK, from hqsj)
# -----------------------------------------------------------------------

def _parse_cffex_market(fpath: Path) -> List[Dict]:
    """Parse CFFEX daily market data CSV (GBK)."""
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="gbk", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("合约代码", "").strip()
            if not code or code == "小计" or "总计" in code or "合计" in code:
                continue
            if any(kw in code for kw in ["-C-", "-P-"]):
                continue  # skip options
            # Keep only IF/IC/IH index futures
            if not code.startswith(("IF","IC","IH","IM","T","TF","TL","TS")):
                continue
            rec = {
                "code": code + ".CFE",
                "date": date,
                "open": _float(row.get("今开盘")),
                "high": _float(row.get("最高价")),
                "low": _float(row.get("最低价")),
                "close": _float(row.get("今收盘")),
                "volume": _float(row.get("成交量")),
                "amt": _float(row.get("成交金额")),
                "oi": _float(row.get("持仓量")),
                "settle": _float(row.get("今结算")),
            }
            if rec["amt"] is not None:
                rec["amt"] = rec["amt"] * 1e4  # 亿元->元
            records.append(rec)
    return records


# -----------------------------------------------------------------------
# CFFEX settlement CSV (GBK, from jscs)
# -----------------------------------------------------------------------

def _parse_cffex_settlement(fpath: Path) -> List[Dict]:
    """Parse CFFEX settlement parameters CSV (GBK).

    Line 0 is a title/date row, line 1 is actual headers.
    Columns: 期货合约,合约多头保证金标准,合约空头保证金标准,...
    """
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="gbk", errors="replace") as f:
        lines = f.read().strip().split("\n")
    reader = csv.DictReader(lines[1:])
    for row in reader:
        code = row.get("期货合约", "").strip()
        if not code:
            continue
        if not code.startswith(("IF","IC","IH","IM","T","TF","TL","TS")):
            continue
        lm = row.get("合约多头保证金标准", "")
        sm = row.get("合约空头保证金标准", "")
        records.append({
            "code": code + ".CFE",
            "date": date,
            "long_margin": _pct_val(lm),
            "short_margin": _pct_val(sm),
        })
    return records


# -----------------------------------------------------------------------
# SHFE / INE daily market data JSON
# -----------------------------------------------------------------------

def _parse_shfe_ine_daily(fpath: Path) -> List[Dict]:
    """Parse SHFE/INE kx*.dat daily market data JSON."""
    records = []
    date = _extract_date_from_filename(fpath)
    name = fpath.name
    exchange_code = "SHF" if "SHFE" in name else "INE"
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("o_curinstrument", []):
        pgid = item.get("PRODUCTGROUPID", "")
        dmonth = str(item.get("DELIVERYMONTH", ""))
        if not pgid or not dmonth:
            continue
        pgid_up = pgid.upper()
        if "小计" in dmonth or "TAS" in dmonth.upper():
            continue
        if pgid_up.endswith("_TAS"):
            pgid_up = pgid_up.replace("_TAS", "")
            dmonth = dmonth + "TAS"
        if pgid_up in INE_PRODUCTS and exchange_code == "SHF":
            continue
        if pgid_up in SHFE_PRODUCTS and exchange_code == "INE":
            continue
        code = f"{pgid_up}{dmonth}.{exchange_code}"
        rec = {
            "code": code,
            "date": date,
            "open": _float(item.get("OPENPRICE")),
            "high": _float(item.get("HIGHESTPRICE")),
            "low": _float(item.get("LOWESTPRICE")),
            "close": _float(item.get("CLOSEPRICE")),
            "volume": _float(item.get("VOLUME")),
            "amt": _float(item.get("TURNOVER")),
            "oi": _float(item.get("OPENINTEREST")),
            "settle": _float(item.get("SETTLEMENTPRICE")),
            "pre_settle": _float(item.get("PRESETTLEMENTPRICE")),
        }
        if rec["amt"] is not None:
            rec["amt"] = rec["amt"] * 1e4
        records.append(rec)
    return records


# -----------------------------------------------------------------------
# SHFE / INE settlement JSON# SHFE / INE settlement JSON
# -----------------------------------------------------------------------

def _parse_shfe_ine_settlement(fpath: Path) -> List[Dict]:
    """Parse SHFE/INE js*.dat settlement JSON."""
    records = []
    date = _extract_date_from_filename(fpath)
    name = fpath.name
    exchange_code = "SHF" if "SHFE" in name else "INE"
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("o_cursor", []):
        instr = item.get("INSTRUMENTID", "")
        if not instr or "小计" in instr:
            continue
        instr_up = instr.upper()
        if exchange_code == "SHF" and instr_up[:2] in {"SC","BC","LU","NR","EC"}:
            continue
        if exchange_code == "INE" and instr_up[:2] in {"CU","AL","ZN","PB","NI","SN",
            "AU","AG","RB","WR","HC","SS","FU","BU","BR","RU","SP","OP","AD","AO"}:
            continue
        code = instr_up + f".{exchange_code}"
        records.append({
            "code": code,
            "date": date,
            "settle": _float(item.get("SETTLEMENTPRICE")),
            "long_margin": _float(item.get("SPECLONGMARGINRATIO")),
            "short_margin": _float(item.get("SPECSHORTMARGINRATIO")),
        })
    return records


# -----------------------------------------------------------------------
# CZCE daily market data TXT
# -----------------------------------------------------------------------

def _parse_czce_daily(fpath: Path) -> List[Dict]:
    """Parse CZCE daily market data pipe-delimited TXT."""
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        lines = f.read().strip().split("\n")
        for line in lines[2:]:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 14:
                continue
            code_raw = parts[0]
            if not code_raw or not re.match(r"^[A-Z]+\d+", code_raw):
                continue
            rec = {
                "code": code_raw + ".CZC",
                "date": date,
                "pre_settle": _float(parts[1]),
                "open": _float(parts[2]),
                "high": _float(parts[3]),
                "low": _float(parts[4]),
                "close": _float(parts[5]),
                "settle": _float(parts[6]),
                "volume": _float(parts[9]),
                "oi": _float(parts[10]),
                "amt": _float(parts[12]),
            }
            if rec["amt"] is not None:
                rec["amt"] = rec["amt"] * 1e4
            records.append(rec)
            records.append(rec)
    return records


# -----------------------------------------------------------------------

def _parse_czce_settlement(fpath: Path) -> List[Dict]:
    """Parse CZCE settlement parameters TXT (UTF-8).

    Columns: \u5408\u7ea6\u4ee3\u7801|\u5f53\u65e5\u7ed3\u7b97\u4ef7|\u662f\u5426\u5355\u8fb9\u5e02|
             \u8fde\u7eed\u5355\u8fb9\u5e02\u5929\u6570|\u4ea4\u6613\u4fdd\u8bc1\u91d1\u7387(%)|\u6da8\u8dcc\u505c\u677f(%)|
    """
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        lines = f.read().strip().split("\n")
        for line in lines[2:]:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 6:
                continue
            code_raw = parts[0]
            if not code_raw or not re.match(r"^[A-Z]+\d+", code_raw):
                continue
            settle = _float(parts[1])
            margin_pct = float(parts[4].replace(",","")) / 100.0 if parts[4].strip().replace(",","") else None
            rec = {
                "code": code_raw + ".CZC",
                "date": date,
                "settle": settle,
                "long_margin": margin_pct,
                "short_margin": margin_pct,
            }
            records.append(rec)
    return records


# -----------------------------------------------------------------------
# CZCE trading parameters TXT (FutureTradeParam.txt)
# Columns: 合约代码|上市日期|交易单位|最小变动价位|上日结算价|
#          涨跌停板幅度(%)|日持仓限额|交易限额|最小开仓量下单量|
#          限价单最大下单量|市价单最大下单量
# -----------------------------------------------------------------------

def _parse_czce_tradepara(fpath: Path) -> List[Dict]:
    """Parse CZCE FutureTradeParam.txt (UTF-8)."""
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        lines = f.read().strip().split("\n")
    for line in lines[2:]:
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 11:
            continue
        code_raw = parts[0]
        if not code_raw or not re.match(r"^[A-Z]+\d+", code_raw):
            continue
        limit_raw = parts[5]
        tick = _float(parts[3])
        pre_settle = _float(parts[4])
        limit_pct = _float(limit_raw) / 100.0 if limit_raw else None
        minoq = _float_to_int(_float(parts[8]))
        maxoq = _float_to_int(_float(parts[9]))  # 限价单最大下单量
        rec = {
            "code": code_raw + ".CZC",
            "date": date,
            "pre_settle": pre_settle,
            "_limit_pct": limit_pct,
            "_tick_size": tick,
            "minoq": minoq,
            "maxoq": maxoq,
        }
        records.append(rec)
    return records


# -----------------------------------------------------------------------
# DCE daily market data JSON
# -----------------------------------------------------------------------

def _parse_dce_daily(fpath: Path) -> List[Dict]:
    """Parse DCE daily market data JSON."""
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("data", []):
        cid = item.get("contractId", "")
        if not cid:
            continue
        rec = {
            "code": cid + ".DCE",
            "date": date,
            "open": _float(item.get("open")),
            "high": _float(item.get("high")),
            "low": _float(item.get("low")),
            "close": _float(item.get("close")),
            "volume": _float(item.get("volumn")),
            "amt": _float(item.get("turnover")),
            "oi": _float(item.get("openInterest")),
            "settle": _float(item.get("clearPrice")),
            "_last_clear": _float(item.get("lastClear")),
        }
        if rec["amt"] is not None:
            rec["amt"] = rec["amt"] * 1e4  # 万元->元
        records.append(rec)
    return records


# -----------------------------------------------------------------------
# DCE settlement JSON
# -----------------------------------------------------------------------

def _parse_dce_settlement(fpath: Path) -> List[Dict]:
    """Parse DCE settlement parameters JSON."""
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("data", []):
        cid = item.get("contractId", "")
        if not cid:
            continue
        records.append({
            "code": cid + ".DCE",
            "date": date,
            "settle": _float(item.get("clearPrice")),
            "long_margin": _float(item.get("specBuyRate")),
            "short_margin": _float(item.get("specSellRate")),
        })
    return records


# -----------------------------------------------------------------------
# GFEX daily market data JSON
# -----------------------------------------------------------------------

def _parse_gfex_daily(fpath: Path) -> List[Dict]:
    """Parse GFEX daily market data JSON."""
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("data", []):
        variety = item.get("variety", "")
        if not variety or variety == "总计":
            continue
        deliv = str(item.get("delivMonth", "")).strip()
        if not deliv:
            continue
        var_order = item.get("varietyOrder", "").strip()
        code = f"{var_order}{deliv}.GFE" if var_order else None
        if not code:
            continue
        rec = {
            "code": code,
            "date": date,
            "open": _float(item.get("open")),
            "high": _float(item.get("high")),
            "low": _float(item.get("low")),
            "close": _float(item.get("close")),
            "volume": _float(item.get("volumn")),
            "amt": _float(item.get("turnover")),
            "oi": _float(item.get("openInterest")),
            "settle": _float(item.get("clearPrice")),
        }
        if rec["amt"] is not None:
            rec["amt"] = rec["amt"] * 1e4  # 万元->元
        records.append(rec)
    return records


# -----------------------------------------------------------------------
# GFEX settlement JSON
# -----------------------------------------------------------------------

def _parse_gfex_settlement(fpath: Path) -> List[Dict]:
    """Parse GFEX settlement parameters JSON."""
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("data", []):
        cid = item.get("contractId", "")
        if not cid:
            continue
        records.append({
            "code": cid + ".GFE",
            "date": date,
            "settle": _float(item.get("clearPrice")),
            "long_margin": _float(item.get("specBuyRate")),
            "short_margin": _float(item.get("specSellRate")),
        })
    return records


# -----------------------------------------------------------------------
# CFFEX trading parameters CSV (jycs)
# Columns: 合约代码,合约月份,挂盘基准价,上市日,最后交易日,
#          涨停板幅度(%),跌停板幅度(%),涨停板价位,跌停板价位,持仓限额
# File is GBK encoded
# -----------------------------------------------------------------------

def _parse_cffex_tradepara(fpath: Path) -> List[Dict]:
    """Parse CFFEX trading parameters CSV (GBK)."""
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="gbk", errors="replace") as f:
        lines = f.read().strip().split("\n")

    reader = csv.DictReader(lines[1:])  # skip title line
    for row in reader:
        code = row.get("合约代码", "").strip()
        if not code or "-" in code:
            continue  # skip option contracts
        if not code.startswith(("IF","IC","IH","IM","T","TF","TL","TS")):
            continue
        rec = {
            "code": code + ".CFE",
            "date": date,
            "maxup": _float(row.get("涨停板价位")),
            "maxdown": _float(row.get("跌停板价位")),
        }
        records.append(rec)
    return records


# -----------------------------------------------------------------------
# SHFE/INE trading parameters JSON (ContractDailyTradeArgument)
# Keys: INSTRUMENTID, UPPER_VALUE, LOWER_VALUE, PRICE_LIMITS,
#       SPEC_LONGMARGINRATIO, SPEC_SHORTMARGINRATIO, TRADINGDAY
# -----------------------------------------------------------------------

def _parse_shfe_ine_tradepara(fpath: Path, suffix: str) -> List[Dict]:
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("ContractDailyTradeArgument", []):
        instr = item.get("INSTRUMENTID", "")
        if not instr or "小计" in instr:
            continue
        instr_up = instr.upper()
        if suffix == ".SHF" and instr_up[:2] in {"SC","BC","LU","NR","EC"}:
            continue
        if suffix == ".INE" and instr_up[:2] in {"CU","AL","ZN","PB","NI","SN",
            "AU","AG","RB","WR","HC","SS","FU","BU","BR","RU","SP","OP","AD","AO"}:
            continue
        upper = _float(item.get("UPPER_VALUE"))
        upper = _float(item.get("UPPER_VALUE"))
        lower = _float(item.get("LOWER_VALUE"))
        lm = _float(item.get("SPEC_LONGMARGINRATIO"))
        sm = _float(item.get("SPEC_SHORTMARGINRATIO"))
        rec = {
            "code": instr_up + suffix,
            "date": date,
            "long_margin": lm,
            "short_margin": sm,
            "_limit_pct": upper,
            "_limit_pct_down": lower,
        }
        records.append(rec)
    return records


# -----------------------------------------------------------------------
# DCE trading parameters JSON (dayTradPara API)
# Keys: contractId, riseLimitRate, riseLimit, fallLimit,
#       specBuyRate, selfTotBuyPosiQuota
# -----------------------------------------------------------------------

def _parse_dce_tradepara(fpath: Path) -> List[Dict]:
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    items = data.get("data", [])
    for item in items:
        cid = item.get("contractId", "")
        if not cid or "-" in cid or "小计" in cid or "TAS" in cid.upper():
            continue
        rec = {
            "code": cid + ".DCE",
            "date": date,
            "maxup": _float(item.get("riseLimit")),
            "maxdown": _float(item.get("fallLimit")),
            "long_margin": _float(item.get("specBuyRate")),
            "short_margin": _float(item.get("specSellRate") or item.get("specBuyRate")),
            "_rise_limit_rate": _float(item.get("riseLimitRate")),
            "_pre_settle": _float(item.get("hedgeBuy") or item.get("specBuy")),
        }
        records.append(rec)
    return records


def _parse_dce_tradingparam(fpath: Path) -> List[Dict]:
    """Parse DCE product-level tradingParam (varietyId + maxHand).

    Extracts maxHand → maxoq for all DCE products.
    """
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    items = data.get("data", [])
    for item in items:
        vid = item.get("varietyId", "")
        if not vid:
            continue
        max_hand = _float(item.get("maxHand"))
        if max_hand is None or max_hand <= 0:
            continue
        records.append({
            "code": vid + ".DCE",
            "date": date,
            "maxoq": _float_to_int(max_hand),
        })
    return records


# -----------------------------------------------------------------------
# GFEX trading parameters JSON (loadDayList API)
# Keys: contractId, riseLimitRate, riseLimit, fallLimit,
#       specBuyRate, selfTotBuyPosiQuota
# -----------------------------------------------------------------------

def _parse_gfex_tradepara(fpath: Path) -> List[Dict]:
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    items = data.get("data", [])
    for item in items:
        cid = item.get("contractId", "")
        if not cid or "-" in cid or "TAS" in cid.upper():
            continue
        rec = {
            "code": cid + ".GFE",
            "date": date,
            "long_margin": _float(item.get("specBuyRate")),
            "short_margin": _float(item.get("specSellRate")),
            "maxup": _float(item.get("riseLimit")),
            "maxdown": _float(item.get("fallLimit")),
        }
        records.append(rec)
    return records


# -----------------------------------------------------------------------
# CSI index market data (JSON API from csindex.com.cn)
# Keys: indexCode, indexName, latestClose
# -----------------------------------------------------------------------

def _parse_csi_market(fpath: Path) -> List[Dict]:
    records = []
    date = _extract_date_from_filename(fpath)
    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)
    for item in data.get("data", []):
        code = item.get("indexCode", "")
        close = _float(item.get("latestClose"))
        if not code:
            continue
        records.append({
            "code": f"{code}.CSI",
            "date": date,
            "close": close,
        })
    return records


# -----------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------

# -----------------------------------------------------------------------
# Tick size lookup (CZCE products)
# 最小变动价位, used for price limit rounding
# -----------------------------------------------------------------------

CZCE_TICK_SIZE = {
    "AP": 1.0, "CF": 5.0, "CJ": 1.0, "CY": 5.0, "FG": 1.0,
    "JR": 1.0, "MA": 1.0, "OI": 1.0, "PF": 2.0, "PK": 2.0,
    "PL": 1.0, "PM": 1.0, "PX": 2.0,
    "RI": 1.0, "RM": 1.0, "RS": 1.0,
    "SA": 1.0, "SF": 2.0, "SH": 1.0, "SM": 2.0, "SR": 1.0,
    "TA": 2.0, "UR": 1.0, "WH": 1.0, "ZC": 0.2, "PR": 1.0,
}


def _get_tick_size(code: str) -> Optional[float]:
    """Get minimum tick size for a contract code."""
    product = "".join(c for c in code.split(".")[0] if c.isalpha())
    return CZCE_TICK_SIZE.get(product)


def _parse_limit_pct(raw: str) -> Optional[float]:
    """Parse CZCE limit pct: '\u00b113' -> 0.13, '\u00b120' -> 0.20."""
    if raw is None:
        return None
    raw = raw.strip().lstrip("\u00b1").strip()
    if not raw:
        return None
    try:
        return float(raw.replace(",", "")) / 100.0
    except (ValueError, TypeError):
        return None


def _float_to_int(val: Optional[float]) -> Optional[int]:
    """Convert float to int, or None."""
    return int(val) if val is not None else None


def _float(val) -> Optional[float]:
    """Convert string/None to float or None."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    val = str(val).replace(",", "").replace(" ", "")
    if not val or val == "null" or val == "None":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _pct_val(val: str) -> Optional[float]:
    """Parse percentage string like '12%' -> 0.12.
    Also handles '12%' (strips %% before float conversion), and plain '12'.
    """
    if val is None:
        return None
    raw = str(val).strip()
    has_pct = "%" in raw
    cleaned = raw.replace("%", "").replace(",", "").replace(" ", "")
    if not cleaned or cleaned in ("null", "None", ""):
        return None
    try:
        v = float(cleaned)
    except (ValueError, TypeError):
        return None
    return v / 100.0 if has_pct else v


# -----------------------------------------------------------------------
# 最小变动价位（从原始 HTML 解析）
# -----------------------------------------------------------------------

PRODUCT_CONFIG_DIR = RAW_DIR / "product_configs"

# 无独立产品页面的品种，直接从交易所规则硬编码
# op_f（胶版印刷纸）在 SHFE 官网上无合约规格独立页面，无法通过 HTML 爬取
HARDCODED_TICK = {
    "op": 2.0,  # 胶版印刷纸
}


def _parse_tick_from_html(html: str) -> Optional[float]:
    """从 SHFE/INE 合约规格 HTML 页面提取最小变动价位。

    SHFE/INE 产品页面的合约规格表为 <table class='table_info'>，
    其中"最小变动价位"在 <td> 中，其下一个 <td> 为对应数值。
    """
    soup = BeautifulSoup(html, "html.parser")
    # 定位合约规格表
    table = soup.find("table", class_="table_info")
    if not table:
        return None
    for td in table.find_all(["td", "th"]):
        if td.get_text(strip=True) == "最小变动价位":
            next_td = td.find_next_sibling(["td", "th"])
            if next_td:
                tick_str = next_td.get_text(strip=True)
                num_match = re.search(r"([\d.]+)", tick_str)
                if num_match:
                    return float(num_match.group(1))
    return None


def _load_product_tick_map() -> Dict[str, float]:
    """从 product_configs HTML 文件解析 tick 映射。

    读取 data/raw/product_configs/ 目录下所有 HTML 文件，
    从合约规格表中提取各品种"最小变动价位"。
    """
    tick_map: Dict[str, float] = {}
    if not PRODUCT_CONFIG_DIR.exists():
        return tick_map

    for fpath in sorted(PRODUCT_CONFIG_DIR.iterdir()):
        if fpath.suffix != ".html":
            continue
        # filename: YYYYMMDD.EXCHANGE.product_code_f.html
        parts = fpath.stem.split(".")
        if len(parts) < 3:
            continue
        code_key = parts[-1].lower()  # e.g. cu_f
        if not code_key.endswith("_f"):
            continue
        pgid = code_key.rsplit("_", 1)[0]  # e.g. cu

        try:
            html = fpath.read_text(encoding="utf-8")
            tick = _parse_tick_from_html(html)
            if tick is not None:
                tick_map[pgid] = tick
        except Exception:
            continue

    # 补充硬编码品种（无独立产品页面的品种）
    for pgid, tick in HARDCODED_TICK.items():
        if pgid not in tick_map:
            tick_map[pgid] = tick

    return tick_map


# -----------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------


def _extract_date_from_filename(fpath: Path) -> str:
    """Extract YYYYMMDD date from filename prefix."""
    name = fpath.stem
    date_match = re.match(r"(\d{8})", name)
    if date_match:
        return date_match.group(1)
    return ""


# -----------------------------------------------------------------------
# Orchestration
# -----------------------------------------------------------------------

def parse_all():
    """Parse all raw files and return (records, stats_summary)."""
    all_records: List[Dict] = []
    all_stats = []
    for fpath in sorted(RAW_DIR.iterdir()):
        if not fpath.is_file() or fpath.suffix == ".jsonl":
            continue
        name = fpath.name
        exchange = name.split(".")[1] if len(name.split(".")) > 1 else "?"
        data_type = name.split(".")[2] if len(name.split(".")) > 2 else "?"
        stats = ParseStats(exchange, data_type)
        try:
            records = parse_file(fpath)
            stats.total = len(records)
            for rec in records:
                stats.add_record(rec)
            all_records.extend(records)
        except Exception as e:
            stats.add_error(str(e))
        all_stats.append(stats)
    return all_records, [s.stats_summary for s in all_stats]


def merge_by_code_date(records: List[Dict]) -> List[Dict]:
    """Merge records with same (code, date), filling None from either."""
    merged: Dict[str, Dict] = {}
    for rec in records:
        key = f"{rec.get('code')}|{rec.get('date')}"
        if key in merged:
            existing = merged[key]
            for k, v in rec.items():
                if v is not None and existing.get(k) is None:
                    if k not in ("code", "date"):
                        existing[k] = v
        else:
            merged[key] = dict(rec)
    # DCE 品种级 maxHand → 合约级 maxoq 传播
    # 品种级 TradingParam.json 按 varietyId 返回 maxHand（如 l-F: maxHand=1000）
    # 解析后 code 为 l.DCE / l-f.DCE（无合约月份数字），合约级如 l2607.DCE / l2607f.DCE
    # 按字母前缀（品种代码 + 可选后缀）匹配传播
    dce_variety: dict[str, dict] = {}
    for rec in merged.values():
        code = rec.get("code", "")
        suffix = code.split(".")[-1] if "." in code else ""
        if suffix.upper() != "DCE":
            continue
        raw = code.split(".")[0]
        if not any(c.isdigit() for c in raw):
            prefix = "".join(c for c in raw if c.isalpha()).upper()
            if prefix:
                dce_variety[prefix] = dict(rec)
    if dce_variety:
        for rec in merged.values():
            code = rec.get("code", "")
            suffix = code.split(".")[-1] if "." in code else ""
            if suffix.upper() != "DCE":
                continue
            raw = code.split(".")[0]
            if any(c.isdigit() for c in raw):
                prefix = "".join(c for c in raw if c.isalpha()).upper()
                if prefix in dce_variety:
                    vrec = dce_variety[prefix]
                    for k, v in vrec.items():
                        if v is not None and rec.get(k) is None and k not in ("code", "date"):
                            rec[k] = v

    # OHLC 0 → None（通过 Modifier 做数据清洗）
    for rec in merged.values():
        zero_price_to_none(rec)

    # Compute maxup/maxdown when both pre_settle and _limit_pct are available
    tick_map: Dict[str, float] = {}
    for rec in merged.values():
        code = rec.get("code", "")
        pre_settle = rec.get("pre_settle") or rec.get("settle")
        limit_pct = rec.get("_limit_pct")
        limit_pct_down = rec.get("_limit_pct_down", limit_pct)
        if pre_settle is not None and limit_pct is not None:
            if rec.get("maxup") is None:
                if code.endswith((".SHF", ".INE")):
                    # SHFE/INE: 向下取整 via Modifier
                    if not tick_map:
                        tick_map = _load_product_tick_map()
                    if tick_map:
                        product = "".join(
                            c.lower() for c in code.split(".")[0]
                            if c.isalpha()
                        )
                        tick = rec.get("_tick_size") or tick_map.get(product)
                        if tick and tick > 0:
                            rec["maxup"], rec["maxdown"] = (
                                calc_shfe_ine_limit_prices(
                                    pre_settle, limit_pct,
                                    limit_pct_down, tick, code
                                )
                            )
                else:
                    # CZCE and others: CZCE-style rounding
                    tick = rec.get("_tick_size") or _get_tick_size(code)
                    raw_up = pre_settle * (1 + limit_pct)
                    raw_down = pre_settle * (1 - limit_pct_down)
                    rec["maxup"] = (
                        safe_ceil(raw_up, tick)
                        if tick else round(raw_up, 2)
                    )
                    rec["maxdown"] = (
                        safe_floor(raw_down, tick)
                        if tick else round(raw_down, 2)
                    )
        # Clean up internal fields
        for key in ("_limit_pct", "_limit_pct_down", "pre_settle",
                    "_tick_size"):
            rec.pop(key, None)

    return list(merged.values())


# ══════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════
#  LLM 提取 maxoq / minoq
# ══════════════════════════════════════════════════════

_DS_BASE = "https://open.bigmodel.cn/api/paas/v4"
_DS_KEY = "f2f4fa94c73c4721ab3d7223548f49eb.e460UuOGcDGIuP6C"
_DS_MODEL = "glm-4-flash"


def parse_minoq_maxoq(html: str, exchange: str, title: str,
                      publish_date: str) -> list[dict]:
    """LLM 提取 maxoq/minoq. 无相关信息返回 []."""
    import requests

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer",
                     "noscript", "iframe"]):
        tag.decompose()
    body = None
    for cls in ["TRS_Editor", "article-content", "articleContent"]:
        el = soup.find(class_=re.compile(cls, re.I))
        if el:
            body = el
            break
    text = body.get_text(separator=" ", strip=True) if body else soup.get_text()
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\]\(http[^)]*\)", "", text)
    text = re.sub(r"\[javascript:.*?\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) > 6000:
        for marker in ["各会员单位", "各会员", "第1章", "第一章",
                       "第一条", "第1条"]:
            idx = text.find(marker)
            if idx > 500:
                text = text[max(0, idx - 100):]
                break

    prompt = (
        "提取公告中每个品种的每次最大/最小下单手数(也称最大/最小开仓下单数量)，输出JSON。"
        + chr(10) + "- product_code用英文代码(如IF,EB,BZ),通用用ALL"
        + chr(10) + '- 提取所有品种, 无则{"items":[]}'
        + chr(10) + chr(10) + f"交易所:{exchange} 日期:{publish_date}"
        + chr(10) + chr(10) + text[:3000]
    )

    try:
        resp = requests.post(
            f"{_DS_BASE}/chat/completions",
            headers={"Authorization": f"Bearer {_DS_KEY}",
                     "Content-Type": "application/json"},
            json={"model": _DS_MODEL,
                  "messages": [{"role": "user", "content": prompt}],
                  "response_format": {"type": "json_object"},
                  "temperature": 0, "max_tokens": 1024},
            timeout=120,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
    except Exception:
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if not m:
            return []
        data = json.loads(m.group(0))

    items = data if isinstance(data, list) else data.get("items", [])

    results = []
    for item in items:
        pc = item.get("product_code") or item.get("product", "ALL")
        field = item.get("field", "")
        if not field:
            for k in item:
                if any(x in k.lower() for x in ("minoq","min_qty","min_open","min_order","minopen","min")):
                    field = "minoq"
                    break
                if any(x in k.lower() for x in ("maxoq","max_qty","max_open","max_order","maxopen","max")):
                    field = "maxoq"
                    break
        if not field:
            continue
        value = item.get("value") or 0
        if not value:
            for kk, vv in item.items():
                kl = kk.lower()
                if any(x in kl for x in ("min_qty","minoq","min_order","min_open")):
                    value = vv; break
                if any(x in kl for x in ("max_qty","maxoq","max_order","max_open")):
                    value = vv; break
        if not isinstance(value, (int, float)) or value <= 0:
            continue
        results.append({
            "product_code": str(pc),
            "security_id": str(item.get("security_id", "ALL")),
            "field": field,
            "value": int(value),
            "effective_date": str(item.get("effective_date", publish_date)),
        })
    return results

# ══════════════════════════════════════════
#  附件解析 + parse_announcement_fields
# ══════════════════════════════════════════

_ZHIPU_KEY = "f2f4fa94c73c4721ab3d7223548f49eb.e460UuOGcDGIuP6C"
_ZHIPU_URL = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
_ZHIPU_VISION_MODEL = "glm-4v-flash"

# ═══ 交易所品种映射 ═══
_EXCHANGE_PRODUCTS = {
    "CFFEX": "IF,IH,IC,IM,T,TF,TL,TS,HO,MO",
    "DCE": "A,B,BB,BZ,C,CS,EB,EG,FB,I,J,JD,JM,L,L-F,LG,LH,M,P,PG,PP,PP-F,RR,V,V-F,Y",
    "SHFE": "CU,AL,ZN,PB,NI,SN,AU,AG,RB,WR,HC,SS,FU,BU,RU,SP,BR,AO",
    "INE": "SC,BC,LU,NR,EC",
    "GFEX": "SI,LC,PS,PT,PD",
}


def _extract_links(html, page_url):
    from urllib.parse import urljoin, unquote
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        ext = Path(href).suffix.lower()
        if ext in (".pdf", ".xls", ".xlsx"):
            results.append({"url": urljoin(page_url, href),
                            "filename": Path(unquote(href.split("/")[-1])).name,
                            "ext": ext})
    return results


def _download(url, save_dir):
    import requests as _req
    from urllib.parse import unquote
    fname = unquote(url.split("/")[-1].split("?")[0])
    if not fname or "." not in fname:
        fname = f"att_{abs(hash(url)) % 10**8}{Path(url).suffix or '.pdf'}"
    local = save_dir / fname
    if local.exists():
        return local
    save_dir.mkdir(parents=True, exist_ok=True)
    r = _req.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    local.write_bytes(r.content)
    return local


def _pdf_to_text(pdf_path):
    import fitz, base64, requests as _req
    doc = fitz.open(str(pdf_path))
    parts = []
    for p in range(min(len(doc), 10)):
        try:
            pix = doc[p].get_pixmap(dpi=150)
            b64 = base64.b64encode(pix.tobytes("png")).decode()
            r = _req.post(_ZHIPU_URL,
                headers={"Authorization": f"Bearer {_ZHIPU_KEY}",
                         "Content-Type": "application/json"},
                json={"model": _ZHIPU_VISION_MODEL, "messages": [{"role": "user",
                    "content": [{"type": "image_url",
                     "image_url": {"url": f"data:image/png;base64,{b64}"}},
                    {"type": "text", "text": "提取此页中与下单量、开仓手数、交易限额相关的文字，逐字输出。"}
                ]}], "max_tokens": 1024, "temperature": 0}, timeout=60)
            parts.append(r.json()["choices"][0]["message"]["content"]
                        if r.status_code == 200 else "")
        except Exception:
            pass
    doc.close()
    return "\n".join(parts)


def _xlsx_to_text(path):
    import openpyxl
    try:
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        out = []
        for sn in wb.sheetnames[:2]:
            ws = wb[sn]
            rows = [",".join(str(v) for v in r if v is not None)
                    for r in list(ws.iter_rows(values_only=True))[:30] if any(r)]
            if rows:
                out.append(f"[Sheet:{sn}]\n" + "\n".join(rows))
        wb.close()
        return "\n".join(out)
    except Exception:
        return ""


def parse_announcement_fields(
    html: str, exchange: str, title: str, publish_date: str,
    page_url: str = "", attachment_dir=None,
) -> list[dict]:
    """解析公告提取 minoq/maxoq。含附件检测、PDF 视觉解析。"""
    import requests as _req2
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "nav", "header", "footer",
                   "noscript", "iframe"]):
        t.decompose()
    body = None
    for cls in ["TRS_Editor", "article-content", "articleContent"]:
        el = soup.find(class_=re.compile(cls, re.I))
        if el:
            body = el
            break
    text = (body.get_text(separator=" ", strip=True) if body
            else soup.get_text())
    text = re.sub(r"\s+", " ", text).strip()
    for mark in ["第一条", "第一章", "第1条", "第1章",
                 "各会员单位", "各会员"]:
        idx = text.find(mark)
        if idx > 500:
            text = text[idx:]
            break

    # 快速关键词过滤
    if not any(kw in text for kw in ("下单", "开仓量", "交易指令")):
        return []


    # 附件解析
    atext = ""
    if page_url and attachment_dir:
        for link in _extract_links(html, page_url):
            local = _download(link["url"], attachment_dir)
            if local:
                try:
                    t = (_pdf_to_text(local) if link["ext"] == ".pdf"
                         else _xlsx_to_text(local))
                    atext += f"\n[附件:{link['filename']}]\n{t}\n"
                except Exception:
                    pass

    full_text = (text[:3000] if atext else text) + atext
    if len(full_text) > 5000:
        full_text = full_text[:5000]

    prompt = (
        "从以下公告提取所有品种的下单/开仓限制数据，输出JSON。\n"
        "【字段】\n"
        "- maxoq: 每次最大下单手数（如'每次最大下单数量为300手'→maxoq=300）\n"
        "- minoq: 每次最小开仓下单数量（如'每次最小开仓下单数量调整为8手'→min oq=8）\n"
        "- 不要提取：持仓限额(限仓X手)、单日开仓限额、保证金、涨跌停板\n"
        "- product_code: 英文代码, 通用用ALL, 严格大小写(A不是a, AG不是ag)\n"
        "- 无信息返回 {\"items\":[]}\n"
        + chr(10) + chr(10)
        + f"交易所:{exchange} (品种范围: {_EXCHANGE_PRODUCTS.get(exchange, '')})"
        + chr(10) + f"日期:{publish_date}"
        + chr(10) + chr(10) + full_text
    )

    try:
        r = _req2.post(f"{_DS_BASE}/chat/completions",
            headers={"Authorization": f"Bearer {_DS_KEY}",
                     "Content-Type": "application/json"},
            json={"model": _DS_MODEL,
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0, "max_tokens": 1024},
            timeout=120)
        r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"]
    except Exception:
        return []

    raw = raw.strip()
    raw = raw.removeprefix("```json").removeprefix("```").strip()
    # 截断到最后一个 ``` 之前
    idx = raw.rfind("```")
    if idx > 0:
        raw = raw[:idx].strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # 尝试提取 JSON 对象/数组
        try:
            m = re.search(r'[{].*[}]', raw, re.DOTALL)
            if not m:
                m = re.search(r'[\[].*[\]]', raw, re.DOTALL)
            if m:
                data = json.loads(m.group(0))
            else:
                return []
        except Exception:
            return []

    items = data if isinstance(data, list) else data.get("items", [])
    results = []
    for item in items:
        pc = item.get("product_code") or item.get("product", "ALL")
        field = ""
        for k in item:
            kl = k.lower()
            if any(x in kl for x in ("minoq", "min_qty", "min_open",
                "min_order", "minopen", "min", "最小")):
                field = "minoq"
                break
            if any(x in kl for x in ("maxoq", "max_qty", "max_open",
                "max_order", "max", "下单", "order_quantity")):
                field = "maxoq"
                break
        if not field:
            continue
        val = item.get("value") or 0
        if not val:
            for kk, vv in item.items():
                if vv and isinstance(vv, (int, float)) and vv > 0:
                    val = int(vv)
                    break
        if not isinstance(val, (int, float)) or val <= 0:
            continue
        results.append({
            "product_code": str(pc),
            "security_id": str(item.get("security_id", "ALL")),
            "field": field,
            "value": int(val),
            "effective_date": str(item.get("effective_date", publish_date)),
        })
    return results
