"""Wind API client via cpp_py WSS (batch snapshot).

Uses cpp_py.w.wss() to fetch full-market futures data for a single date
in one RPC call. Only available on machines with cpp_py installed
(e.g. db201, server202).
"""

from typing import Optional

import os

from data_sources.modifier import czc_to_wind_code

# Wind RPC host/port — read from env at call time (not import time)
def _rpc_endpoint():
    return os.environ["WIND_RPC_HOST"], int(os.environ["WIND_RPC_PORT"])

# Default fields matching t_futures_info.py
_WSS_FIELDS = [
    "open", "high", "low", "close", "volume",
    "amt", "oi", "settle", "maxup", "maxdown",
    "if_basis", "long_margin", "short_margin", "minoq", "maxoq",
]


def _ensure_connected() -> bool:
    """Set Wind RPC server and check connectivity."""
    try:
        import cpp_py
        host, port = _rpc_endpoint()
        cpp_py.w.set_server(f"{host}:{port}")
        return cpp_py.w.is_connected()
    except Exception:
        return False


def fetch_wind_data(
    target_date: str,
    fields: Optional[list[str]] = None,
) -> list[dict]:
    """Fetch Wind WSS snapshot for all active futures contracts.

    Mirrors t_futures_info.py's contract selection logic:
      1. SELECT code FROM t_futures WHERE ipo <= date AND lasttrade >= date
      2. CZCE codes: 4-digit → 3-digit for Wind query, restore afterwards
      3. Single wss() call for all codes + fields

    Args:
        target_date: YYYYMMDD
        fields: fields to fetch, default=_WSS_FIELDS (15 fields)

    Returns:
        {code: {field: float}} — empty dict if Wind unavailable.
    """
    if not _ensure_connected():
        return {}

    if fields is None:
        fields = list(_WSS_FIELDS)

    # ---- Get active contracts ----
    from data_sources.db import get_connection

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT i.code
                FROM future_cn.t_futures i
                WHERE i.ipo_date <= %s AND i.lasttrade_date >= %s
            """, (target_date, target_date))
            db_codes = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not db_codes:
        return {}

    # ---- CZCE 4→3 for Wind query ----
    wind_codes = [czc_to_wind_code(c) for c in db_codes]
    code_map = dict(zip(wind_codes, db_codes))  # wind_code → db_code

    # ---- Single batch wss call ----
    import cpp_py

    codes_str = ",".join(wind_codes)
    fields_str = ",".join(fields)

    # code_col=code conflicts with minoq/maxoq indicators (CWSSService: invalid indicators)
    has_order_fields = any(f in ("minoq", "maxoq") for f in fields)
    if has_order_fields:
        options = f"tradeDate={target_date};futinstrtype=1"
    else:
        options = f"tradeDate={target_date};futinstrtype=1;code_col=code"

    ec, _, df = cpp_py.w.wss(codes_str, fields_str, options)
    if ec != 0 or df is None or df.empty:
        return {}

    # ---- Parse DataFrame → [{code, date, field: float}] ----
    result: list[dict] = []
    for i, (_, row) in enumerate(df.iterrows()):
        # With code_col=code: use row["code"]; without: match by position
        if has_order_fields:
            wind_code = wind_codes[i] if i < len(wind_codes) else ""
        else:
            wind_code = row.get("code", "")
        if not wind_code:
            continue
        db_code = code_map.get(wind_code, wind_code)
        rec = {"code": db_code, "date": target_date}
        for f in fields:
            val = row.get(f)
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue
            if v != v:  # NaN check
                continue
            rec[f] = v
        result.append(rec)

    return result
