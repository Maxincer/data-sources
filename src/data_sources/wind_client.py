"""Minimal Wind API client via cpp_py RPC (no WindPy, no dataproc).

Uses cpp_py.w.set_server() + w.wsd() to call Wind RPC at 192.168.2.9:3801.
Only available on machines with cpp_py installed (e.g. db201).
Gracefully unavailable elsewhere.
"""

from typing import Optional


class WindClient:
    """Thin wrapper around cpp_py.w.wsd() for futures daily data.

    Usage:
        client = WindClient()
        if client.available:
            data = client.get_wsd("CU2606.SHF", "20260522",
                                  ["open", "close", "volume"])
    """

    # Wind RPC 配置（固定值，与 data-crawler 一致）
    _RPC_HOST = "192.168.2.9"
    _RPC_PORT = 3801

    def __init__(self):
        self.available = False
        self._wsd = None  # 缓存 wsd 函数引用，避免每次属性查找
        try:
            import cpp_py  # noqa: F811
            cpp_py.w.set_server(f"{self._RPC_HOST}:{self._RPC_PORT}")
            if cpp_py.w.is_connected():
                self._wsd = cpp_py.w.wsd
                self.available = True
        except Exception:
            pass

    def get_wsd(
        self,
        code: str,
        date: str,
        fields: list[str],
    ) -> Optional[dict]:
        """Call Wind WSD for one contract on one date via RPC.

        Args:
            code: 合约代码，如 "CU2606.SHF"
            date: 交易日 YYYYMMDD，如 "20260522"
            fields: 字段列表，如 ["open","high","low","close"]

        Returns:
            {field: value} dict, or None if unavailable / no data.
        """
        if not self.available:
            return None

        ec, _es, df = self._wsd(
            code,
            ",".join(fields),
            date,
            date,
            "futinstrtype=1",
        )
        if ec != 0 or df is None or df.empty:
            return None

        row = df.iloc[0]
        out = {}
        for f in fields:
            val = row.get(f)
            if val is not None:
                try:
                    out[f] = float(val)
                except (TypeError, ValueError):
                    pass
        return out if out else None

    def batch_wsd(
        self,
        codes: list[str],
        date: str,
        fields: list[str],
    ) -> dict[str, Optional[dict]]:
        """Batch WSD for multiple contracts on one date.

        Returns:
            {code: {field: value}} — slow path, one RPC call per code.
        """
        return {code: self.get_wsd(code, date, fields) for code in codes}
