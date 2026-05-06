#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Tests for fetcher module."""
# pylint: disable=redefined-outer-name

import logging
from pathlib import Path

import pytest

from data_sources.fetcher import Fetcher
from data_sources.models import Task, TaskConfig
from data_sources.verifier import Verifier
from data_sources.reporter import Reporter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def dummy_logger():
    """Create a logger with alert method for testing."""
    _logger = logging.getLogger("test")
    _logger.setLevel(logging.DEBUG)
    _logger.handlers.clear()
    _logger.addHandler(logging.NullHandler())
    _logger.alert = lambda msg, *a, **kw: _logger.error(
        msg % a if a else msg
    )
    return _logger


@pytest.fixture
def _verifier(dummy_logger):
    return Verifier(dummy_logger)


@pytest.fixture
def _reporter(dummy_logger):
    return Reporter(dummy_logger)


# ---------------------------------------------------------------------------
# Verifier tests
# ---------------------------------------------------------------------------

class TestVerifier:
    def test_check_min_size_ok(self, _verifier):
        passed, _ = _verifier.check_min_size(b"x" * 500, 200)
        assert passed

    def test_check_min_size_zero(self, _verifier):
        passed, reason = _verifier.check_min_size(b"", 200)
        assert not passed
        assert "Empty" in reason

    def test_check_min_size_too_small(self, _verifier):
        passed, reason = _verifier.check_min_size(b"x" * 10, 200)
        assert not passed
        assert "< minimum" in reason

    def test_check_html_error_page_plain_data(self, _verifier):
        content = b"20260401,IF,3800.0,3810.0\n"
        passed, reason = _verifier.check_html_error_page(content)
        assert passed
        assert reason == "OK"

    def test_check_html_error_page_html(self, _verifier):
        content = b"<html><body>Welcome</body></html>"
        passed, reason = _verifier.check_html_error_page(content)
        assert not passed
        assert "HTML response" in reason

    def test_check_html_error_page_cffex_error(self, _verifier):
        content = (
            b"\xcd\xf8\xd2\xb3\xb4\xed\xce\xf3"
            b"<html>404</html>"
        )
        passed, reason = _verifier.check_html_error_page(content)
        assert not passed
        assert "CFFEX 404" in reason

    def test_check_size_deviation_no_previous(self, _verifier):
        passed, _ = _verifier.check_size_deviation(1000, None)
        assert passed

    def test_check_size_deviation_zero_previous(self, _verifier):
        passed, _ = _verifier.check_size_deviation(1000, 0)
        assert passed

    def test_check_size_deviation_within_threshold(self, _verifier):
        passed, _ = _verifier.check_size_deviation(1100, 1000, 50.0)
        assert passed

    def test_check_size_deviation_exceeds_threshold(self, _verifier):
        passed, reason = _verifier.check_size_deviation(
            1600, 1000, 50.0
        )
        assert not passed
        assert "Size deviation" in reason

    def test_verify_response_all_checks(self, _verifier):
        passed, _ = _verifier.verify_response(b"x" * 500, 500)
        assert passed

    def test_verify_response_html_first(self, _verifier):
        content = b"<html>short</html>"
        passed, reason = _verifier.verify_response(content)
        assert not passed
        assert "HTML" in reason


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------

class TestTask:
    def test_from_config_shfe(self):
        config = TaskConfig(
            "SHFE",
            lambda t: {"success": True},
            "dat",
            "SettlementParameters",
            "http://example.com/data/js{YYYYMMDD}.dat",
        )
        task = Task.from_config(config, "20260401")
        assert task.exchange == "SHFE"
        assert task.url == "http://example.com/data/js20260401.dat"
        assert task.fetch_func is config.fetch_func

    def test_from_config_czce(self):
        config = TaskConfig(
            "CZCE",
            lambda t: {"success": True},
            "txt",
            "SettlementParameters",
            "http://example.com/{YYYY}/{YYYYMMDD}/data.txt",
        )
        task = Task.from_config(config, "20260401")
        assert task.url == "http://example.com/2026/20260401/data.txt"
        assert task.fetch_func is config.fetch_func

    def test_from_config_cffex(self):
        config = TaskConfig(
            "CFFEX",
            lambda t: {"success": True},
            "csv",
            "MarketData",
            "http://example.com/{YYYY}{MM}/{DD}/{YYYYMMDD}_1.csv",
        )
        task = Task.from_config(config, "20260401")
        assert task.url == "http://example.com/202604/01/20260401_1.csv"

    def test_from_config_ine(self):
        config = TaskConfig(
            "INE",
            lambda t: {"success": True},
            "dat",
            "SettlementParameters",
            "http://example.com/data/js{YYYYMMDD}.dat",
        )
        task = Task.from_config(config, "20260401")
        assert task.url == "http://example.com/data/js20260401.dat"

    def test_default_attributes(self):
        task = Task(
            "TEST", "csv", "desc",
            "http://example.com/x.csv", "20260401",
        )
        assert task.filepath is None
        assert task.size is None
        assert task.previous_size is None
        assert task.change_percent is None
        assert task.fetch_func is None


# ---------------------------------------------------------------------------
# Reporter tests
# ---------------------------------------------------------------------------

class TestReporter:
    def test_task_report_empty(self, _reporter):
        _reporter.task_report([], "20260401")

    def test_task_report_with_tasks(self, _reporter, caplog):
        caplog.set_level(logging.ERROR)
        task = Task(
            "SHFE", "dat", "SettlementParameters",
            "http://example.com/js20260401.dat", "20260401",
        )
        task.filepath = Path(
            "20260401.SHFE.SettlementParameters.dat"
        )
        task.size = 5000
        task.previous_size = 4800
        task.change_percent = 4.17

        _reporter.task_report([task], "20260401")

        records = [
            r for r in caplog.records
            if r.levelno == logging.ERROR
        ]
        assert len(records) >= 1
        combined = " ".join(str(r.msg) for r in records)
        assert "20260401" in combined
        assert "5000" in combined


# ---------------------------------------------------------------------------
# Integration / smoke tests (no real HTTP calls)
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_run_smoke_imports_and_config(self):
        f = Fetcher()
        assert len(f.task_configs) == 12
        exchanges = [c.exchange for c in f.task_configs]
        assert exchanges == [
            "SHFE", "INE", "GFEX", "DCE", "CZCE", "CFFEX",
            "SHFE", "INE", "CZCE", "GFEX", "DCE", "CFFEX",
        ]
        descriptions = [c.description for c in f.task_configs]
        assert descriptions == [
            "SettlementParameters", "SettlementParameters",
            "SettlementParameters", "SettlementParameters",
            "SettlementParameters", "SettlementParameters",
            "DailyMarketData", "DailyMarketData",
            "DailyMarketData", "DailyMarketData",
            "DailyMarketData", "DailyMarketData",
        ]

    def test_task_config_defines_all_fields(self):
        f = Fetcher()
        for config in f.task_configs:
            assert config.exchange
            assert callable(config.fetch_func)
            assert config.suffix
            assert config.description
            assert config.url_template

    def test_task_from_config_preserves_fetch_func(self):
        f = Fetcher()
        for config in f.task_configs:
            task = Task.from_config(config, "20260401")
            assert task.fetch_func is config.fetch_func


# Run with: pytest tests/test_fetcher.py -v
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
