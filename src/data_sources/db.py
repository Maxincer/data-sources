#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MySQL database connection and upsert operations for the data pipeline.

New table naming: t_futures_info_{source} where source = "exchange" (cffex, shfe, etc.)
Or a combined table for all exchange-sourced data.

All functions accept an optional config_override dict to support runtime
parameterization (e.g. switching host/database/table for different deployment
phases without modifying global state).
"""


import os
import pymysql


if os.environ.get("DEV_MODE"):
    # 开发环境（server202 / WSL）
    DB_CONFIG = {
        "host": "192.168.1.202",
        "user": "root",
        "password": "root0808",
        "database": "future_cn",
        "charset": "utf8mb4",
    }
else:
    # 生产环境（db201 → MySQL 集群）
    DB_CONFIG = {
        "host": "192.168.1.27",
        "user": "tools",
        "password": "tools0512",
        "database": "future_cn",
        "charset": "utf8mb4",
    }

TABLE_NAME = "t_futures_info_exchange"


# ---- Helpers ----


def resolve_config(config_override: dict | None = None) -> dict:
    """Return merged DB config, overriding defaults with provided values."""
    cfg = dict(DB_CONFIG)
    if config_override:
        cfg.update({k: v for k, v in config_override.items() if v is not None})
    return cfg


def _table_name(config_override: dict | None = None) -> str:
    if config_override and "table" in config_override:
        return config_override["table"]
    return TABLE_NAME


def _database_name(config_override: dict | None = None) -> str:
    cfg = resolve_config(config_override)
    return cfg.get("database", "future_cn")


# ---- Connection ----


def get_connection(config_override: dict | None = None):
    """Return a pymysql connection."""
    cfg = resolve_config(config_override)
    return pymysql.connect(**cfg)


# ---- DDL ----


CREATE_SQL_TPL = """\
CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (
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


def create_table(config_override: dict | None = None):
    """Create table if it doesn't exist."""
    db_name = _database_name(config_override)
    tbl_name = _table_name(config_override)
    sql = CREATE_SQL_TPL.format(database=db_name, table=tbl_name)
    conn = get_connection(config_override)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


# ---- DML ----


def upsert_records(records, config_override: dict | None = None):
    """
    Upsert parsed records into the configured table.

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
    db_name = _database_name(config_override)
    tbl_name = _table_name(config_override)
    sql = (
        f"INSERT INTO `{db_name}`.`{tbl_name}` ({column_str}) "
        f"VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_str}"
    )
    from data_sources.modifier import margin_to_pct

    conn = get_connection(config_override)
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


def count_records(config_override: dict | None = None):
    """Return total record count in the configured table."""
    db_name = _database_name(config_override)
    tbl_name = _table_name(config_override)
    conn = get_connection(config_override)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM `{db_name}`.`{tbl_name}`"
            )
            return cur.fetchone()["cnt"]
    finally:
        conn.close()
