#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MySQL database connection and upsert operations for the data pipeline.

New table naming: t_futures_info_{source} where source = "exchange" (cffex, shfe, etc.)
Or a combined table for all exchange-sourced data.
"""

import os

import pymysql
from pymysql.cursors import DictCursor

DB_CONFIG = {
    "host": "192.168.1.202",
    "user": "root",
    "password": "root0808",
    "database": "future_cn",
    "charset": "utf8mb4",
}


def get_connection():
    """Return a pymysql connection to future_cn."""
    return pymysql.connect(**DB_CONFIG)


TABLE_NAME = "t_futures_info_exchange"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS `future_cn`.`{TABLE_NAME}` (
  `id` int NOT NULL AUTO_INCREMENT,
  `code` varchar(128) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `open` decimal(20,4) DEFAULT NULL,
  `high` decimal(20,4) DEFAULT NULL,
  `low` decimal(20,4) DEFAULT NULL,
  `close` decimal(20,4) DEFAULT NULL,
  `volume` decimal(20,4) DEFAULT NULL,
  `amt` decimal(24,2) DEFAULT NULL,
  `oi` decimal(20,4) DEFAULT NULL,
  `settle` decimal(20,4) DEFAULT NULL,
  `maxup` decimal(20,4) DEFAULT NULL,
  `maxdown` decimal(20,4) DEFAULT NULL,
  `if_basis` decimal(20,4) DEFAULT NULL,
  `long_margin` decimal(20,4) DEFAULT NULL,
  `short_margin` decimal(20,4) DEFAULT NULL,
  `minoq` int DEFAULT NULL,
  `maxoq` int DEFAULT NULL,
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `code_date` (`code`, `date`),
  KEY `code` (`code`),
  KEY `date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def create_table():
    """Create the exchange-sourced data table if it doesn't exist."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()
    finally:
        conn.close()


def upsert_records(records):
    """
    Upsert parsed records into t_futures_info_exchange.

    Each record should be a dict with keys matching the table columns.
    """
    if not records:
        return 0
    cols = [
        "code", "date", "open", "high", "low", "close",
        "volume", "amt", "oi", "settle", "maxup", "maxdown",
        "if_basis", "long_margin", "short_margin", "minoq", "maxoq",
    ]
    placeholders = ", ".join([f"%({c})s" for c in cols])
    column_str = ", ".join(cols)
    update_str = ", ".join(
        [f"{c}=VALUES({c})" for c in cols if c not in ("code", "date")]
    )
    sql = (
        f"INSERT INTO `future_cn`.`{TABLE_NAME}` ({column_str}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_str}"
    )
    from data_sources.modifier import margin_to_pct

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for rec in records:
                rec = dict(rec)
                # Convert margin to percentage (0.12 -> 12.0)
                if "long_margin" in rec:
                    rec["long_margin"] = margin_to_pct(rec["long_margin"])
                if "short_margin" in rec:
                    rec["short_margin"] = margin_to_pct(rec["short_margin"])
                # Ensure date is in YYYY-MM-DD format
                d = rec.get("date", "")
                if d and isinstance(d, str) and len(d) == 8:
                    rec["date"] = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                # Filter to only known columns
                clean = {k: rec.get(k) for k in cols}
                cur.execute(sql, clean)
        conn.commit()
        return len(records)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def count_records():
    """Return total record count in exchange table."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM `future_cn`.`{TABLE_NAME}`"
            )
            return cur.fetchone()["cnt"]
    finally:
        conn.close()
