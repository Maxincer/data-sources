#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Data models for pipeline tasks."""

import re
from pathlib import Path
from typing import Callable, NamedTuple


class TaskConfig(NamedTuple):
    """Immutable template for building a Task."""
    exchange: str
    fetch_func: Callable[..., dict]
    suffix: str
    description: str
    url_template: str


class Task:
    """Concrete task produced from config and trade date."""

    def __init__(  # pylint: disable=too-many-positional-arguments
        self,
        exchange: str,
        suffix: str,
        description: str,
        url: str,
        trade_date: str,
    ):
        self.exchange = exchange
        self.suffix = suffix
        self.description = description
        self.url = url
        self.trade_date = trade_date
        self.filepath: Path | None = None
        self.size: int | None = None
        self.previous_size: int | None = None
        self.change_percent: float | None = None
        self.fetch_func: Callable | None = None

    @classmethod
    def from_config(
        cls,
        config: TaskConfig,
        trade_date: str,
    ) -> "Task":
        """Factory method: create Task from template."""
        exchange = config.exchange
        url_template = config.url_template

        # CFFEX settlement: resolve URL from live page instead of template
        if (
            exchange == "CFFEX"
            and config.description == "SettlementParameters"
        ):
            return cls._from_cffex_jscs_page(config, trade_date)

        if exchange in ("CZCE", "CFFEX"):
            year = trade_date[:4]
            month = trade_date[4:6]
            day = trade_date[6:8]
            kwargs = {"YYYYMMDD": trade_date, "YYYY": year,
                      "MM": month, "DD": day, "YYYYMM": year + month}
            url = url_template.format(**kwargs)
        else:
            url = url_template.format(YYYYMMDD=trade_date)
        task = cls(
            exchange=config.exchange,
            suffix=config.suffix,
            description=config.description,
            url=url,
            trade_date=trade_date,
        )
        task.fetch_func = config.fetch_func
        return task

    @classmethod
    def _from_cffex_jscs_page(cls, config: TaskConfig,
                              trade_date: str) -> "Task":
        """
        Build a CFFEX settlement Task by finding the latest CSV link
        on the live jscs.html page; uses the given trade_date as date.
        """
        from data_sources.fetcher import fetch_cffex_jscs_links
        links = fetch_cffex_jscs_links()
        # Filter: must contain /sj/jscs/ and end with _1.csv
        csv_links = [
            link for link in links
            if "/sj/jscs/" in link["url"]
            and link["url"].rstrip().endswith("_1.csv")
        ]
        if not csv_links:
            raise RuntimeError(
                "No CFFEX settlement CSV links found on jscs.html"
            )
        # Find the link with the latest date (YYYYMMDD in URL path)
        def _extract_date(url: str) -> str:
            # URL pattern: .../sj/jscs/YYYYMM/DD/YYYYMMDD_1.csv
            match = re.search(r"/sj/jscs/\d{6}/\d{2}/(\d{8})_1\.csv", url)
            return match.group(1) if match else ""

        best = max(csv_links, key=lambda link: _extract_date(link["url"]))
        task = cls(
            exchange=config.exchange,
            suffix=config.suffix,
            description=config.description,
            url=best["url"],
            trade_date=trade_date,
        )
        task.fetch_func = config.fetch_func
        return task
