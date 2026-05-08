#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Futures Exchange Daily Settlement Price Fetcher.
Downloads raw data from SHFE, INE, GFEX, DCE, CZCE, CFFEX.
"""

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import fire
import requests

from mxz_utils.logging_config import get_logger

from data_sources.task import Task
from data_sources.verifier import Verifier
from data_sources.reporter import Reporter
from data_sources.configs import build_task_configs, DCE_BASE_URL

RAW_DATA_DIR = Path("./data/raw")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
METADATA_FILE = RAW_DATA_DIR / ".metadata.jsonl"
DCE_API_KEY = "ofxc69rpmd59"
DCE_API_SECRET = "2UdFW^2G4!4^7#@URqWx"


# ---------------------------------------------------------------------------
# CFFEX helpers
# ---------------------------------------------------------------------------

CFFEX_JSCS_URL = "http://www.cffex.com.cn/cn/jscs.html"
_CFFEX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


def fetch_cffex_jscs_links() -> List[dict]:
    """
    Fetch CFFEX settlement params page and extract all <a> links.
    Returns list of {"url": str, "text": str}.
    """
    resp = requests.get(CFFEX_JSCS_URL, headers=_CFFEX_HEADERS, timeout=15)
    resp.encoding = "utf-8"
    html = resp.text

    base = "http://www.cffex.com.cn"
    links: List[dict] = []
    pattern = re.compile(
        r'<a\s[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html):
        href = match.group(1).strip()
        raw_text = match.group(2).strip()
        text = re.sub(r"<[^>]+>", "", raw_text).strip()
        if not href or href.startswith("#") or href.startswith("javascript"):
            continue
        full_url = (
            href if href.startswith("http")
            else f"{base}/{href.lstrip('/')}"
        )
        links.append({"url": full_url, "text": text or href})
    return links


def get_cffex_settlement_available(start_date: str = "20260401") -> list[dict]:
    """
    Get all available CFFEX settlement CSV links from jscs.html.

    Returns:
        [{"date": "YYYYMMDD", "url": str}, ...], sorted descending.
    """
    links = fetch_cffex_jscs_links()
    csv_links = []
    for link in links:
        url = link["url"]
        match = re.search(r"/sj/jscs/\d{6}/\d{2}/(\d{8})_1\.csv", url)
        if match:
            d = match.group(1)
            if d >= start_date:
                csv_links.append({"date": d, "url": url})
    seen = set()
    unique = []
    for entry in csv_links:
        if entry["date"] not in seen:
            seen.add(entry["date"])
            unique.append(entry)
    unique.sort(key=lambda x: x["date"], reverse=True)
    return unique


class Fetcher:
    def __init__(self):
        _log_dir = os.environ.get("PROJECT_ROOT", ".") + "/logs"
        self.logger = get_logger(
            name="Fetcher",
            level="INFO",
            dirpath_logs=_log_dir,
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
        self.verifier = Verifier(self.logger, METADATA_FILE)
        self.reporter = Reporter(self.logger)
        self.task_configs = build_task_configs(self)

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _build_filename(self, task: Task) -> str:
        return (
            f"{task.trade_date}.{task.exchange}."
            f"{task.description}.{task.suffix}"
        )

    def _write_metadata(self, task: Task) -> None:
        """Append a metadata record based on current task state."""
        record = {
            "trade_date": task.trade_date,
            "exchange": task.exchange,
            "url": task.url,
            "status_code": getattr(task, "status_code", None),
            "local_filename": task.filepath.name,
            "local_path": str(task.filepath),
            "download_time": datetime.now().strftime('%Y%m%dT%H%M%S'),
            "file_size_bytes": task.size,
            "verified": True,
            "previous_size_bytes": task.previous_size,
            "size_change_percent": task.change_percent,
        }
        try:
            with open(METADATA_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            self.logger.debug(
                "Metadata written for %s", task.exchange
            )
        except Exception as e:
            self.logger.error(
                "Failed to write metadata for %s: %s",
                task.exchange,
                e,
                exc_info=True,
            )

    def _save_and_record(self, content: bytes, task: Task) -> None:
        """Save verified content to disk and record metadata."""
        local_filename = self._build_filename(task)
        filepath = RAW_DATA_DIR / local_filename
        with open(filepath, "wb") as f:
            f.write(content)
        self.logger.info("Saved %s raw data to %s", task.exchange, filepath)

        task.filepath = filepath
        task.size = len(content)
        task.previous_size = self.verifier.get_previous_size(task)
        if task.previous_size and task.previous_size > 0:
            task.change_percent = round(
                ((task.size - task.previous_size) / task.previous_size)
                * 100,
                2,
            )
        else:
            task.change_percent = None
        self._write_metadata(task)

    # -----------------------------------------------------------------
    # Generic GET fetch (used by simple GET + verify + save methods)
    # -----------------------------------------------------------------

    def _do_fetch_get(self, task: Task, label: str = "") -> Dict:
        """Generic GET + verify + save for simple API calls."""
        try:
            resp = requests.get(
                task.url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            return {"success": True}
        except Exception as e:
            self.logger.error("%s fetch failed: %s", label, e, exc_info=True)
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # Exchange-specific fetch methods
    # -----------------------------------------------------------------

    def _fetch_shfe_settlement(self, task: Task) -> Dict:
        return self._do_fetch_get(task, "SHFE settlement")

    def _fetch_ine_settlement(self, task: Task) -> Dict:
        return self._do_fetch_get(task, "INE settlement")

    def _fetch_czce_settlement(self, task: Task) -> Dict:
        return self._do_fetch_get(task, "CZCE settlement")

    def _fetch_cffex_market(self, task: Task) -> Dict:
        return self._do_fetch_get(task, "CFFEX market")

    def _fetch_cffex_tradepara(self, task: Task) -> Dict:
        return self._do_fetch_get(task, "CFFEX tradepara")

    def _fetch_shfe_tradepara(self, task: Task) -> Dict:
        return self._do_fetch_get(task, "SHFE tradepara")

    def _fetch_ine_tradepara(self, task: Task) -> Dict:
        return self._do_fetch_get(task, "INE tradepara")

    def _fetch_czce_tradepara(self, task: Task) -> Dict:
        return self._do_fetch_get(task, "CZCE tradepara")

    def _fetch_gfex_settlement(self, task: Task) -> Dict:
        try:
            # GFEX 结算参数表要求 trade_date 以数组格式传入才能正确区分日期
            payload = {"trade_date": [task.trade_date]}
            resp = requests.post(
                task.url,
                data=payload,
                headers=self.fake_headers,
                timeout=30,
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            data = resp.json()
            if not isinstance(data, dict) or "data" not in data:
                raise ValueError("Invalid response structure")
            if data.get("code") != "0":
                raise ValueError(f"API error: {data.get('msg')}")
            records = data.get("data", [])
            if not isinstance(records, list) or len(records) == 0:
                raise ValueError("Empty data list")
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            task.status_code = resp.status_code
            # Re-verify the normalized JSON content (size may differ)
            passed, reason = self.verifier.verify_response(
                content, prev_size
            )
            if not passed:
                raise ValueError(
                    "Verification after normalization failed: "
                    f"{reason}"
                )
            self._save_and_record(content, task)
            self.logger.info(
                "GFEX settlement data fetched, %d records", len(records)
            )
            return {"success": True}
        except Exception as e:
            self.logger.error("GFEX fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def _fetch_gfex_market(self, task: Task) -> Dict:
        try:
            payload = {"trade_date": task.trade_date, "trade_type": "0"}
            resp = requests.post(
                task.url,
                data=payload,
                headers=self.fake_headers,
                timeout=30,
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            data = resp.json()
            if not isinstance(data, dict) or "data" not in data:
                raise ValueError("Invalid response structure")
            if data.get("code") != "0":
                raise ValueError(f"API error: {data.get('msg')}")
            records = data.get("data", [])
            if not isinstance(records, list) or len(records) == 0:
                raise ValueError("Empty data list")
            # Filter out the summary row
            products = [
                r for r in records
                if r.get("variety") != "总计"
            ]
            if not products:
                self.logger.warning(
                    "GFEX daily quotes: only summary row for %s, no data",
                    task.trade_date,
                )
                return {"success": True, "no_data": True}
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            task.status_code = resp.status_code
            passed, reason = self.verifier.verify_response(
                content, prev_size
            )
            if not passed:
                raise ValueError(
                    "Verification after normalization failed: "
                    f"{reason}"
                )
            self._save_and_record(content, task)
            self.logger.info(
                "GFEX daily quotes fetched, %d products", len(products)
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "GFEX daily fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    def _fetch_dce_settlement(self, task: Task) -> Dict:
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
                "tradeDate": task.trade_date,
                "tradeType": "1",
                "lang": "zh",
            }
            resp = requests.post(
                task.url,
                headers=headers,
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            data = resp.json()
            if not data.get("success") or "data" not in data:
                raise ValueError(f"API error: {data.get('msg')}")
            if isinstance(data["data"], list) and len(data["data"]) == 0:
                raise ValueError("Empty data list")
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            task.status_code = resp.status_code
            # Re-verify normalized content
            passed, reason = self.verifier.verify_response(
                content, prev_size
            )
            if not passed:
                raise ValueError(
                    f"Verification after normalization failed: {reason}"
                )
            self._save_and_record(content, task)
            self.logger.info(
                "DCE settlement data fetched, %d records", len(data["data"])
            )
            return {"success": True}
        except Exception as e:
            self.logger.error("DCE fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def _fetch_dce_market(self, task: Task) -> Dict:
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
                "tradeDate": task.trade_date,
                "tradeType": "1",
                "lang": "zh",
            }
            resp = requests.post(
                task.url,
                json=payload,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            data = resp.json()
            if not data.get("success") or "data" not in data:
                raise ValueError(f"API error: {data.get('msg')}")
            if isinstance(data["data"], list) and len(data["data"]) == 0:
                raise ValueError("Empty data list")
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            task.status_code = resp.status_code
            passed, reason = self.verifier.verify_response(
                content, prev_size
            )
            if not passed:
                raise ValueError(
                    f"Verification after normalization failed: {reason}"
                )
            self._save_and_record(content, task)
            self.logger.info(
                "DCE daily quotes fetched, %d records",
                len(data["data"])
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "DCE daily fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    def _fetch_czce_settlement(self, task: Task) -> Dict:
        try:
            resp = requests.get(
                task.url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            return {"success": True}
        except Exception as e:
            self.logger.error("CZCE fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    def _fetch_cffex_market(self, task: Task) -> Dict:
        try:
            resp = requests.get(
                task.url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                # HTML error page is considered "no data", not a failure
                if "HTML error page" in reason:
                    self.logger.warning(
                        "CFFEX returned error page for %s, no data.",
                        task.trade_date,
                    )
                    return {"success": True, "no_data": True}
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            self.logger.info(
                "CFFEX market data fetched, size=%d bytes", task.size
            )
            return {"success": True}
        except Exception as e:
            self.logger.error("CFFEX fetch failed: %s", e, exc_info=True)
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # New data source: CFFEX settlement parameters
    # -----------------------------------------------------------------

    def _fetch_cffex_settlement(self, task: Task) -> Dict:
        try:
            resp = requests.get(
                task.url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                if "HTML error page" in reason:
                    self.logger.warning(
                        "CFFEX settlement returned error page for %s",
                        task.trade_date,
                    )
                    return {"success": True, "no_data": True}
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            self.logger.info(
                "CFFEX settlement fetched, size=%d bytes", task.size
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "CFFEX settlement fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # New data source: SHFE daily market data (kx*.dat)
    # -----------------------------------------------------------------

    def _fetch_shfe_market(self, task: Task) -> Dict:
        try:
            url = task.url + str(int(time.time() * 1000))
            resp = requests.get(
                url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            self.logger.info(
                "SHFE daily market data fetched, size=%d bytes", task.size
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "SHFE daily fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # New data source: INE daily market data (kx*.dat)
    # -----------------------------------------------------------------

    def _fetch_ine_market(self, task: Task) -> Dict:
        try:
            url = task.url + str(int(time.time() * 1000))
            resp = requests.get(
                url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            self.logger.info(
                "INE daily market data fetched, size=%d bytes", task.size
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "INE daily fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # New data source: CZCE daily market data
    # -----------------------------------------------------------------

    def _fetch_czce_market(self, task: Task) -> Dict:
        try:
            resp = requests.get(
                task.url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            self.logger.info(
                "CZCE daily market data fetched, size=%d bytes",
                task.size
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "CZCE daily fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # DCE token helper
    # -----------------------------------------------------------------

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
            self.logger.error(
                "DCE token request failed: %s", data.get("msg")
            )
            return None
        except Exception as e:
            self.logger.error(
                "Exception while getting DCE token: %s",
                e,
                exc_info=True,
            )
            return None

    # -----------------------------------------------------------------
    # CFFEX trading parameters (simple GET CSV)
    # -----------------------------------------------------------------

    def _fetch_cffex_tradepara(self, task: Task) -> Dict:
        try:
            resp = requests.get(
                task.url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            resp.encoding = "utf-8"
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            self.logger.info(
                "CFFEX trading parameters fetched, size=%d bytes",
                task.size
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "CFFEX trading params fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # SHFE trading parameters (simple GET .dat)
    # -----------------------------------------------------------------

    def _fetch_shfe_tradepara(self, task: Task) -> Dict:
        try:
            url = task.url + "?params=" + str(int(time.time() * 1000))
            resp = requests.get(
                url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            self.logger.info(
                "SHFE trading parameters fetched, size=%d bytes",
                task.size
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "SHFE trading params fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # INE trading parameters (simple GET .dat, same pattern as SHFE)
    # -----------------------------------------------------------------

    def _fetch_ine_tradepara(self, task: Task) -> Dict:
        try:
            resp = requests.get(
                task.url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            self.logger.info(
                "INE trading parameters fetched, size=%d bytes",
                task.size
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "INE trading params fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # DCE trading parameters (POST with Bearer token)
    # -----------------------------------------------------------------

    def _fetch_dce_tradepara(self, task: Task) -> Dict:
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
                "tradeDate": task.trade_date,
                "tradeType": "1",
                "lang": "zh",
            }
            resp = requests.post(
                task.url,
                headers=headers,
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            data = resp.json()
            if not data.get("success") or "data" not in data:
                raise ValueError(f"API error: {data.get('msg')}")
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            task.status_code = resp.status_code
            passed, reason = self.verifier.verify_response(
                content, prev_size
            )
            if not passed:
                raise ValueError(
                    f"Verification after normalization failed: {reason}"
                )
            self._save_and_record(content, task)
            self.logger.info(
                "DCE trading parameters fetched, %d records",
                len(data["data"])
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "DCE trading params fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # GFEX trading parameters (simple POST JSON)
    # -----------------------------------------------------------------

    def _fetch_gfex_tradepara(self, task: Task) -> Dict:
        try:
            payload = {"trade_type": "0", "trade_date": task.trade_date}
            headers = {
                **self.fake_headers,
                "Content-Type": "application/json",
            }
            resp = requests.post(
                task.url, json=payload, headers=headers, timeout=30
            )
            resp.raise_for_status()
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            data = resp.json()
            if data.get("code") != "0" or "data" not in data:
                raise ValueError(f"API error: {data.get('msg')}")
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            task.status_code = resp.status_code
            passed, reason = self.verifier.verify_response(
                content, prev_size
            )
            if not passed:
                raise ValueError(
                    f"Verification after normalization failed: {reason}"
                )
            self._save_and_record(content, task)
            self.logger.info(
                "GFEX trading parameters fetched, %d records",
                len(data["data"])
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "GFEX trading params fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # CZCE trading parameters (simple GET TXT - FutureTradeParam.txt)
    # -----------------------------------------------------------------

    def _fetch_czce_tradepara(self, task: Task) -> Dict:
        try:
            resp = requests.get(
                task.url, headers=self.fake_headers, timeout=30
            )
            resp.raise_for_status()
            resp.encoding = "utf-8"
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                resp.content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(resp.content, task)
            self.logger.info(
                "CZCE trading parameters fetched, size=%d bytes",
                task.size
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "CZCE trading params fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # CSI index market data (POST JSON)
    # -----------------------------------------------------------------

    def _fetch_csi_market(self, task: Task) -> Dict:
        try:
            payload = {
                "sorter": {"sortField": "null", "sortOrder": None},
                "pager": {"pageNum": 1, "pageSize": 40},
                "indexFilter": {
                    "indexSeries": ["1", "2"],
                    "indexClassify": ["17"],
                    "region": None,
                    "hotSpot": None,
                    "currency": None,
                    "ifCustomized": "a",
                    "ifTracked": "1",
                    "indexCompliance": "IOSCO",
                },
            }
            resp = requests.post(
                task.url,
                json=payload,
                headers={
                    **self.fake_headers,
                    "Content-Type": "application/json",
                    "Origin": "https://www.csindex.com.cn",
                    "Referer": "https://www.csindex.com.cn/",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                raise ValueError(f"CSI API error: {data.get('msg')}")
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            prev_size = self.verifier.get_previous_size(task)
            passed, reason = self.verifier.verify_response(
                content, prev_size
            )
            if not passed:
                raise ValueError(f"Verification failed: {reason}")
            task.status_code = resp.status_code
            self._save_and_record(content, task)
            self.logger.info(
                "CSI market data fetched, %d index items",
                len(data["data"])
            )
            return {"success": True}
        except Exception as e:
            self.logger.error(
                "CSI market fetch failed: %s", e, exc_info=True
            )
            return {"success": False, "error": str(e)}

    # -----------------------------------------------------------------
    # Retry logic
    # -----------------------------------------------------------------

    def _retry_failed_tasks(
        self,
        failed_tasks: List[Task],
    ) -> tuple[List[Task], List[Task]]:
        """
        Retry each failed task using the stored fetch_func.
        Returns (still_failed, recovered).
        """
        still_failed: List[Task] = []
        recovered: List[Task] = []
        if not failed_tasks:
            return still_failed, recovered
        self.logger.info(
            "=== Starting second round retry for %d failed task(s) ===",
            len(failed_tasks),
        )
        for task in failed_tasks:
            self.logger.info("Second attempt for %s", task.exchange)
            result = task.fetch_func(task)
            if not result.get("success"):
                still_failed.append(task)
                self.logger.alert(
                    "CRITICAL: Second round request failed "
                    "for %s on %s: %s",
                    task.exchange,
                    task.trade_date,
                    result.get("error"),
                )
            else:
                recovered.append(task)
        return still_failed, recovered

    # -----------------------------------------------------------------
    # SHFE / INE Product Config (Playwright-based HTML download)
    # -----------------------------------------------------------------

    PRODUCT_CONFIG_DIR = RAW_DATA_DIR / "product_configs"

    def _fetch_exchange_product_config(self, task: Task) -> Dict:
        """Download SHFE/INE product specification HTMLs via Playwright.

        使用单个浏览器会话完成产品发现 + 所有页面下载，
        避免每次新建 Chromium 进程的开销。

        headless=True（默认）用于自动运行。手动调用时可在
        launch 参数中改为 False 以绕过 WAF。
        """
        from playwright.sync_api import sync_playwright
        from bs4 import BeautifulSoup
        import re

        nick = "shfe" if task.exchange == "SHFE" else "ine"
        domain = f"www.{nick}.com.cn"
        listing_url = task.url
        self.PRODUCT_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

        products = []
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            page = browser.new_page()

            try:
                # ── Step 1: 访问列表页，发现所有产品 URL ──
                self.logger.info("Discovering %s products from %s", task.exchange, listing_url)
                page.goto(listing_url, wait_until="commit", timeout=30000)
                page.wait_for_timeout(8000)
                html = page.content()

                if "人机识别" in html:
                    page.wait_for_timeout(8000)
                    html = page.content()
                if "人机识别" in html:
                    self.logger.error("BLOCKED by WAF: %s", listing_url)
                    return {"success": False, "error": "WAF blocked"}

                soup = BeautifulSoup(html, "html.parser")
                seen = set()
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    m = re.search(r"(/products/futures/.*/\w+_f/)$", href)
                    if m and m.group(1) not in seen:
                        seen.add(m.group(1))
                        code_match = re.search(r"/(\w+_f)/$", href)
                        code = code_match.group(1) if code_match else "unknown"
                        products.append({"code": code, "url": f"https://{domain}{m.group(1)}"})

                self.logger.info("Found %d products", len(products))

                # ── Step 2: 逐个下载产品页面 ──
                success_count = 0
                for i, prod in enumerate(products):
                    code = prod["code"]
                    save_name = f"{task.trade_date}.{task.exchange}.{code}.html"
                    save_path = self.PRODUCT_CONFIG_DIR / save_name

                    if save_path.exists():
                        self.logger.debug("  [%d/%d] %s already exists", i + 1, len(products), code)
                        success_count += 1
                        continue

                    self.logger.info("  [%d/%d] Fetching %s...", i + 1, len(products), code)
                    page.goto(prod["url"], wait_until="commit", timeout=30000)
                    page.wait_for_timeout(6000)

                    page_html = page.content()
                    if "人机识别" in page_html:
                        page.wait_for_timeout(6000)
                        page_html = page.content()
                    if "人机识别" in page_html:
                        self.logger.warning("    ✗ %s (WAF blocked)", code)
                        continue

                    save_path.write_text(page_html, encoding="utf-8")
                    self.logger.info("    ✓ %s (%d bytes)", save_name, len(page_html))
                    success_count += 1
                    time.sleep(1)

            except Exception as e:
                self.logger.error("Product config fetch failed: %s", e, exc_info=True)
                return {"success": False, "error": str(e)}
            finally:
                page.close()
                browser.close()

        self.logger.info("%s product config: %d/%d saved", task.exchange, success_count, len(products))
        return {"success": True}

    def _fetch_shfe_product_config(self, task: Task) -> Dict:
        """Download SHFE product specification HTMLs."""
        return self._fetch_exchange_product_config(task)

    def _fetch_ine_product_config(self, task: Task) -> Dict:
        """Download INE product specification HTMLs."""
        return self._fetch_exchange_product_config(task)

    # -----------------------------------------------------------------
    # Main entry point
    # -----------------------------------------------------------------

    def run(self, trade_date: Optional[str] = None) -> None:
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")
        else:
            trade_date = str(trade_date)

        self.logger.info(
            "=== Starting futures data download for %s ===", trade_date
        )

        successful_tasks: List[Task] = []
        failed_tasks: List[Task] = []

        # 找出已有 GFE 交易参数表文件中的最近日期
        gfe_latest = ""
        for f in sorted(RAW_DATA_DIR.glob("*.GFEX.TradingParameters.json")):
            d = f.name[:8]
            if d > gfe_latest:
                gfe_latest = d

        # ---- CFFEX Settlement 同步 ----
        try:
            server_files = get_cffex_settlement_available()
            if server_files:
                server_dates = {s["date"] for s in server_files}
                self.logger.info(
                    "CFFEX Settlement: 服务端有 %d 个文件 (%s~%s)",
                    len(server_files), server_files[-1]["date"], server_files[0]["date"],
                )
                for entry in server_files:
                    fname = f"{entry['date']}.CFFEX.SettlementParameters.csv"
                    fpath = RAW_DATA_DIR / fname
                    if fpath.exists():
                        continue
                    self.logger.info("下载缺失的 CFFEX Settlement: %s", fname)
                    try:
                        resp = requests.get(entry["url"], headers=self.fake_headers, timeout=30)
                        resp.raise_for_status()
                        with open(fpath, "wb") as f:
                            f.write(resp.content)
                        self.logger.info("  ✅ %s 已保存 (%d bytes)", fname, len(resp.content))
                    except Exception as e:
                        self.logger.warning("  ⚠ %s 下载失败: %s", fname, e)
                # 清理本地多余文件
                for fpath in sorted(RAW_DATA_DIR.glob("*.CFFEX.SettlementParameters.csv")):
                    d = fpath.name[:8]
                    if d >= "20260401" and d not in server_dates:
                        fpath.unlink()
                        self.logger.warning("  删除本地多余文件: %s", fpath.name)
        except Exception as e:
            self.logger.warning("CFFEX Settlement 同步失败: %s", e)

        for config in self.task_configs:
            task = Task.from_config(config, trade_date)

            # CFFEX Settlement 已改为同步模式，跳过旧的任务循环
            if task.exchange == "CFFEX" and task.description == "SettlementParameters":
                continue

            # GFEX 交易参数表 & 日行情不支持按日期查询——仅当请求日期 >= 已有文件最新日期时才下载
            # 结算参数表（SettlementParameters）已用数组格式支持历史查询，不受影响
            if (task.exchange == "GFEX"
                    and task.description in ("TradingParameters", "DailyMarketData")
                    and trade_date < gfe_latest):
                self.logger.warning(
                    "跳过 GFEX %s %s：API 不支持按日期查询, "
                    "已有数据最新日期为 %s",
                    task.description, trade_date, gfe_latest,
                )
                continue

            self.logger.info(
                "Fetching %s data from %s", task.exchange, task.url
            )
            result = config.fetch_func(task)

            if result.get("success") and not result.get("no_data"):
                successful_tasks.append(task)
            elif result.get("no_data"):
                self.logger.info(
                    "%s returned no data (likely holiday or error page)",
                    task.exchange,
                )
            else:
                failed_tasks.append(task)

        still_failed, recovered = self._retry_failed_tasks(failed_tasks)
        successful_tasks.extend(recovered)

        if not still_failed:
            self.logger.alert(
                "All raw data for %s downloaded successfully", trade_date
            )
        else:
            self.logger.alert(
                "Some exchanges failed after retry: %s",
                [t.exchange for t in still_failed],
            )

        self.reporter.task_report(successful_tasks, trade_date)

        self.logger.info(
            "=== Download process completed for %s ===", trade_date
        )


def main():
    fire.Fire(Fetcher)


# -----------------------------------------------------------------
# Standalone: download DCE trading parameters for a specific date
# -----------------------------------------------------------------


def _fetch_dce_single(date_str: str, config_module: str, fetch_attr: str, label: str) -> bool:
    """下载 DCE 单个接口的数据（generic helper）。"""
    f = Fetcher()
    mod = __import__(
        f"data_sources.fetcher_task_configs."
        f"{config_module}",
        fromlist=["URL_TEMPLATE", "EXCHANGE", "SUFFIX", "DESCRIPTION"],
    )
    task = Task(
        exchange=mod.EXCHANGE,
        suffix=mod.SUFFIX,
        description=mod.DESCRIPTION,
        url=mod.URL_TEMPLATE.format(YYYYMMDD=date_str),
        trade_date=date_str,
    )
    task.fetch_func = getattr(f, fetch_attr)
    result = getattr(f, fetch_attr)(task)
    if result.get("success"):
        f.logger.info("fetch_%s: successfully downloaded %s", label, date_str)
        return True
    else:
        f.logger.error(
            "fetch_%s: failed for %s: %s",
            label, date_str, result.get("error"),
        )
        return False


def fetch_dce_tradepara(date_str: str) -> bool:
    """
    外部按指定日期下载 DCE 交易参数表（dayTradPara）。

    Modifier 在修正开盘限价时，若发现缺失前一交易日的数据，
    可调用此函数主动下载。

    Args:
        date_str: YYYYMMDD 格式日期

    Returns:
        True 表示下载成功，False 表示失败

    Usage:
        from data_sources.fetcher import fetch_dce_tradepara
        fetch_dce_tradepara("20260428")
    """
    return _fetch_dce_single(
        date_str, "dce_tradepara", "_fetch_dce_tradepara", "dce_tradepara"
    )


def fetch_dce_settlement(date_str: str) -> bool:
    """
    外部按指定日期下载 DCE 结算参数表（futAndOptSettle）。

    与 fetch_dce_tradepara 签名一致，用于 Writer 自动补全前一日数据。

    Args:
        date_str: YYYYMMDD 格式日期

    Returns:
        True 表示下载成功，False 表示失败

    Usage:
        from data_sources.fetcher import fetch_dce_settlement
        fetch_dce_settlement("20260428")
    """
    return _fetch_dce_single(
        date_str, "dce_settlement", "_fetch_dce_settlement", "dce_settlement"
    )


if __name__ == "__main__":
    main()
