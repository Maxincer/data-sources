#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Futures Exchange Daily Settlement Price Fetcher.
Downloads raw data from SHFE, INE, GFEX, DCE, CZCE, CFFEX.
"""

from datetime import datetime
import json
from pathlib import Path
from typing import Dict, List, Optional, NamedTuple

import fire
import requests

from mxz_utils.logging_config import get_logger

RAW_DATA_DIR = Path("./data/raw")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
METADATA_FILE = RAW_DATA_DIR / ".metadata.jsonl"
DCE_API_KEY = "ofxc69rpmd59"
DCE_API_SECRET = "2UdFW^2G4!4^7#@URqWx"
DCE_BASE_URL = "http://www.dce.com.cn"
SHFE_BASE_URL = "http://www.shfe.com.cn"
INE_BASE_URL = "http://www.ine.com.cn"
GFEX_BASE_URL = "http://www.gfex.com.cn"


class SaveContext(NamedTuple):
    exchange: str
    suffix: str
    description: str
    url: str
    status_code: int


class Fetcher:
    def __init__(self):
        self.logger = get_logger(
            name="Fetcher",
            level="INFO",
            dirpath_logs="./logs",
            logfile_basename="Fetcher",
        )
        self.fake_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        self.tasks = [
            (
                "SHFE",
                self._fetch_shfe_settlement,
                "dat",
                "SettlementParameters",
                SHFE_BASE_URL + "/data/tradedata/future/dailydata/js{}.dat"
            ),
            (
                "INE",
                self._fetch_ine_settlement,
                "dat",
                "SettlementParameters",
                INE_BASE_URL + "/data/tradedata/future/dailydata/js{}.dat"
            ),
            (
                "GFEX",
                self._fetch_gfex_settlement,
                "json",
                "SettlementParameters",
                GFEX_BASE_URL + "/u/interfacesWebTiFutAndOptSettle/loadList"
            ),
            (
                "DCE",
                self._fetch_dce_settlement,
                "json",
                "SettlementParameters",
                f"{DCE_BASE_URL}/dceapi/forward/publicweb/tradepara/"
                f"futAndOptSettle",
            ),
            (
                "CZCE",
                self._fetch_czce_settlement,
                "txt",
                "SettlementParameters",
                "https://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/"
                "{}/FutureDataClearParams.txt",
            ),
            (
                "CFFEX",
                self._fetch_cffex_market,
                "csv",
                "MarketData",
                "http://www.cffex.com.cn/sj/hqsj/rtj/{}{}/{}/{}_1.csv",
            ),
        ]

    def _build_filename(
        self, trade_date: str, exchange: str, suffix: str, description: str
    ) -> str:
        return f"{trade_date}.{exchange}.{description}.{suffix}"

    def _extract_original_filename(
        self, exchange: str, url: str, trade_date: str
    ) -> str:
        if exchange in ("SHFE", "INE"):
            fn = f"js{trade_date}.dat"
        elif exchange == "CZCE":
            fn = "FutureDataClearParams.txt"
        elif exchange == "CFFEX":
            fn = f"{trade_date}_1.csv"
        elif exchange == "GFEX":
            fn = f"loadList?trade_date={trade_date}"
        elif exchange == "DCE":
            fn = "futAndOptSettle (DCE API)"
        else:
            fn = url.split("/")[-1] or "unknown"
        return fn

    def _write_metadata(self, record: Dict) -> None:
        """Append a metadata record in JSON Lines format."""
        try:
            with open(METADATA_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            self.logger.debug("Metadata written for %s",record.get("exchange"))
        except Exception as e:
            self.logger.error(
                "Failed to write metadata for %s: %s",
                record.get("exchange"),
                e,
                exc_info=True,
            )

    def _save_and_record(
        self,
        content: bytes,
        trade_date: str,
        ctx: SaveContext,
    ) -> Path:
        """Save content to file and record metadata."""
        local_filename = self._build_filename(
            trade_date, ctx.exchange, ctx.suffix, ctx.description
        )
        filepath = RAW_DATA_DIR / local_filename
        with open(filepath, "wb") as f:
            f.write(content)
        self.logger.info("Saved %s raw data to %s", ctx.exchange, filepath)

        original_source = self._extract_original_filename(
            ctx.exchange, ctx.url, trade_date
        )
        self._write_metadata(
            {
                "trade_date": trade_date,
                "exchange": ctx.exchange,
                "url": ctx.url,
                "status_code": ctx.status_code,
                "original_source": original_source,
                "local_filename": local_filename,
                "local_path": str(filepath),
                "download_time": datetime.now().strftime('%Y%m%dT%H%M%S'),
                "file_size_bytes": len(content),
                "verified": False,
            }
        )
        return filepath

    def _get_dce_token(self) -> Optional[str]:
        """Retrieve a valid Bearer token for the DCE API."""
        try:
            url = f"{DCE_BASE_URL}/dceapi/cms/auth/accessToken"
            headers = {"apikey": DCE_API_KEY}
            payload = {"secret": DCE_API_SECRET}
            resp = requests.post(
                url, headers=headers, json=payload, timeout=15
            )
            resp.raise_for_status()

            data = resp.json()
            if data.get("success"):
                token = data["data"]["token"]
                self.logger.info("DCE token obtained successfully")
                return token
            self.logger.error("DCE token request failed: %s", data.get("msg"))
            return None
        except Exception as e:
            self.logger.error(
                "Exception while getting DCE token: %s", e, exc_info=True
            )
            return None

    def _fetch_shfe_settlement(self, trade_date: str, url: str) -> Dict:
        """Fetch SHFE daily settlement parameters."""
        try:
            resp = requests.get(url, headers=self.fake_headers, timeout=30)
            resp.raise_for_status()
            if len(resp.content) == 0:
                raise ValueError("Empty content")
            filepath = self._save_and_record(
                resp.content,
                trade_date,
                SaveContext(
                    "SHFE", "dat", "SettlementParameters",
                    url, resp.status_code
                )
            )
            return {"success": True, "filepath": filepath}
        except Exception as e:
            self.logger.error("SHFE fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def _fetch_ine_settlement(self, trade_date: str, url: str) -> Dict:
        """Fetch INE daily settlement parameters."""
        try:
            resp = requests.get(url, headers=self.fake_headers, timeout=30)
            resp.raise_for_status()
            if len(resp.content) == 0:
                raise ValueError("Empty content")
            filepath = self._save_and_record(
                resp.content,
                trade_date,
                SaveContext(
                    "INE", "dat", "SettlementParameters",
                    url, resp.status_code
                ),
            )
            return {"success": True, "filepath": filepath}
        except Exception as e:
            self.logger.error("INE fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def _fetch_gfex_settlement(self, trade_date: str, url: str) -> Dict:
        """Fetch GFEX daily settlement parameters via POST."""
        try:
            payload = {"trade_date": trade_date}
            resp = requests.post(
                url, data=payload, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()

            data = resp.json()
            if not isinstance(data, dict) or "data" not in data:
                raise ValueError("Invalid response structure")
            if data.get("code") != "0":
                raise ValueError(f"API error: {data.get('msg')}")

            records = data.get("data", [])
            if not isinstance(records, list) or len(records) == 0:
                raise ValueError("Empty data list")

            # 保存完整原始 JSON 响应
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            filepath = self._save_and_record(
                content,
                trade_date,
                SaveContext(
                    "GFEX", "json", "SettlementParameters",
                    url, resp.status_code
                ),
            )
            self.logger.info(
                "GFEX settlement data fetched, %d records", len(records)
            )
            return {"success": True, "filepath": filepath}
        except Exception as e:
            self.logger.error("GFEX fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def _fetch_dce_settlement(self, trade_date: str, url: str) -> Dict:
        """Fetch DCE daily settlement parameters."""
        token = self._get_dce_token()
        if not token:
            return {"success": False, "error": "Token acquisition failed"}

        try:
            headers = {
                "apikey": DCE_API_KEY,
                "Authorization": f"Bearer {token}",
            }
            payload = {
                "varietyId": "all",
                "tradeDate": trade_date,
                "tradeType": "1",
                "lang": "zh",
            }
            resp = requests.post(
                url, headers=headers, json=payload, timeout=30
            )
            resp.raise_for_status()

            data = resp.json()
            if not data.get("success") or "data" not in data:
                raise ValueError(f"API error: {data.get('msg')}")
            if isinstance(data["data"], list) and len(data["data"]) == 0:
                raise ValueError("Empty data list")

            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            filepath = self._save_and_record(
                content,
                trade_date,
                SaveContext(
                    "DCE", "json", "SettlementParameters",
                    url, resp.status_code
                ),
            )
            self.logger.info(
                "DCE settlement data fetched, %d records", len(data["data"])
            )
            return {"success": True, "filepath": filepath}
        except Exception as e:
            self.logger.error("DCE fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def _fetch_czce_settlement(self, trade_date: str, url: str) -> Dict:
        """Fetch CZCE daily settlement parameters."""
        try:
            resp = requests.get(url, headers=self.fake_headers, timeout=30)
            resp.raise_for_status()
            if len(resp.content) == 0:
                raise ValueError("Empty content")
            filepath = self._save_and_record(
                resp.content,
                trade_date,
                SaveContext(
                    "CZCE", "txt", "SettlementParameters",
                    url, resp.status_code
                ),
            )
            return {"success": True, "filepath": filepath}
        except Exception as e:
            self.logger.error("CZCE fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def _fetch_cffex_market(self, trade_date: str, url: str) -> Dict:
        """Fetch CFFEX daily market data (CSV format)."""
        try:
            resp = requests.get(url, headers=self.fake_headers, timeout=30)
            resp.raise_for_status()

            if len(resp.content) == 0:
                raise ValueError("Empty content")

            content_preview = resp.content[:200]
            if (
                b"<html" in content_preview.lower()
                or b"<!doctype html" in content_preview.lower()
            ):
                if b"\xcd\xf8\xd2\xb3\xb4\xed\xce\xf3" in resp.content:
                    self.logger.warning(
                        "CFFEX returned 404 error page for %s, no data.",
                        trade_date
                    )
                    return {"success": True, "no_data": True}
                raise ValueError("HTML response instead of CSV")

            content_str = None
            for enc in ("utf-8", "gbk", "gb2312", "latin-1"):
                try:
                    content_str = resp.content.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            if content_str is None:
                raise ValueError("Unable to decode CSV content")

            lines = content_str.strip().split("\n")
            if len(lines) < 2:
                raise ValueError("CSV has insufficient rows")

            header = lines[0].strip()
            required_fields = ("合约代码", "今开盘", "今收盘")
            if not any(field in header for field in required_fields):
                self.logger.warning(
                    "CFFEX CSV header unexpected: %s", header[:100]
                )
                raise ValueError("CSV header missing required fields")

            filepath = self._save_and_record(
                resp.content,
                trade_date,
                SaveContext(
                    "CFFEX", "csv", "MarketData", url, resp.status_code
                ),
            )
            self.logger.info(
                "CFFEX market data fetched, %d data rows", len(lines) - 1
            )
            return {"success": True, "filepath": filepath}
        except Exception as e:
            self.logger.error("CFFEX fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def _retry_failed_exchanges(
        self, failed_tasks: List[tuple], trade_date: str
    ) -> None:
        """
        Perform a second round of attempts for failed exchanges.
        """
        failed_tasks_after_retry = []
        if not failed_tasks:
            return failed_tasks_after_retry

        self.logger.info(
            "=== Starting second round retry for %d failed exchange(s) ===",
            len(failed_tasks),
        )
        for failed_task in failed_tasks:
            exchange, fetch_func, _, _, url_template = failed_task
            self.logger.info("Second attempt for %s", exchange)
            if exchange in ("CZCE", "CFFEX"):
                year = trade_date[:4]
                month = trade_date[4:6]
                day = trade_date[6:8]
                if exchange == "CZCE":
                    url = url_template.format(year, trade_date)
                else:
                    url = url_template.format(year, month, day, trade_date)
            else:
                url = url_template.format(trade_date)
            result = fetch_func(trade_date, url)
            if result.get("success"):
                self.logger.info("Second round succeeded for %s", exchange)
            else:
                failed_tasks_after_retry.append(failed_task)
                self.logger.alert(
                    "CRITICAL: Second round request failed for %s on %s: %s",
                    exchange,
                    trade_date,
                    result['error']
                )
        return failed_tasks_after_retry

    def run(self, trade_date: Optional[str] = None) -> None:
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")
        else:
            trade_date = str(trade_date)

        self.logger.info(
            "=== Starting futures data download for %s ===", trade_date
        )

        failed_tasks = []
        for exchange, fetch_func, suffix, description, url_temp in self.tasks:
            if exchange in ("CZCE", "CFFEX"):
                year = trade_date[:4]
                month = trade_date[4:6]
                day = trade_date[6:8]
                if exchange == "CZCE":
                    url = url_temp.format(year, trade_date)
                else:
                    url = url_temp.format(year, month, day, trade_date)
            else:
                url = url_temp.format(trade_date)

            self.logger.info("Fetching %s data from %s", exchange, url)
            result = fetch_func(trade_date, url)

            if not result.get("success"):
                failed_tasks.append(
                    (exchange, fetch_func, suffix, description, url_temp)
                )

        if not self._retry_failed_exchanges(failed_tasks, trade_date):
            self.logger.alert(
                f'{self.logger.name}: '
                f'All raw data for {trade_date} downloaded'
            )
        self.logger.info(
            "=== Download process completed for %s ===", trade_date
        )


def main():
    fire.Fire(Fetcher)


if __name__ == "__main__":
    main()
