"""MySQL database connection and upsert operations for the data pipeline.

New table naming: t_futures_info_{source} where source = "exchange" (cffex, shfe, etc.)
Or a combined table for all exchange-sourced data.

All functions accept an optional config_override dict to support runtime
parameterization (e.g. switching host/database/table for different deployment
phases without modifying global state).
"""

import os
import pymysql

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "192.168.1.202"),
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD", ""),
    "database": os.environ.get("DB_DATABASE", "future_cn"),
    "charset": "utf8mb4",
}

def _table_name(config_override: dict | None = None) -> str:
    """Return table name from config_override. Required via --table arg."""
    if config_override and "table" in config_override:
        return config_override["table"]
    raise ValueError("table name required (pass --table to writer)")


def _database_name(config_override: dict | None = None) -> str:
    cfg = resolve_config(config_override)
    return cfg.get("database", "future_cn")


# ---- Helpers ----


def resolve_config(config_override: dict | None = None) -> dict:
    """Return merged DB config, overriding defaults with provided values."""
    cfg = dict(DB_CONFIG)
    if config_override:
        for k, v in config_override.items():
            if v is not None and k not in ("table",):
                cfg[k] = v
    return cfg


def get_connection(config_override: dict | None = None):
    """Return a pymysql connection."""
    cfg = resolve_config(config_override)
    return pymysql.connect(**cfg)


    cfg = resolve_config(config_override)
    return cfg.get("database", "future_cn")


    """Return a pymysql connection."""
    cfg = resolve_config(config_override)
    return pymysql.connect(**cfg)


def fetch_table(table: str, date: str) -> list[dict]:
    """Read all rows from a MySQL table for a given date.

    Returns list of records: [{"code": "...", "date": "YYYY-MM-DD",
                                "open": 105720.0, ...}, ...]
    Dates are converted to YYYYMMDD strings. Numeric fields become floats.
    """
    conn = get_connection()
    conn.cursorclass = pymysql.cursors.DictCursor
    try:
        dt = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM `future_cn`.{table} WHERE date = %s",
                (dt,),
            )
            rows = cur.fetchall()
            result: list[dict] = []
            for row in rows:
                rec = {}
                for k, v in row.items():
                    if v is None:
                        continue  # skip nulls, missing keys will be None on access
                    if hasattr(v, "isoformat"):
                        # date / datetime → YYYYMMDD
                        rec[k] = str(v).replace("-", "")[:8]
                    elif isinstance(v, (int, float)):
                        rec[k] = float(v)
                    else:
                        rec[k] = v
                if rec.get("code"):  # only include rows with a code
                    result.append(rec)
            return result
    finally:
        conn.close()


# ---- DDL ----


CREATE_SQL_TPL = """\
CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (
    `code`    VARCHAR(16)  NOT NULL,
    `date`    DATE         NOT NULL,
    `open`    DOUBLE       DEFAULT NULL,
    `high`    DOUBLE       DEFAULT NULL,
    `low`     DOUBLE       DEFAULT NULL,
    `close`   DOUBLE       DEFAULT NULL,
    `volume`  DOUBLE       DEFAULT NULL,
    `amt`     DOUBLE       DEFAULT NULL,
    `oi`      DOUBLE       DEFAULT NULL,
    `settle`  DOUBLE       DEFAULT NULL,
    `maxup`   DOUBLE       DEFAULT NULL,
    `maxdown` DOUBLE       DEFAULT NULL,
    `if_basis` DOUBLE      DEFAULT NULL,
    `long_margin` DOUBLE   DEFAULT NULL,
    `short_margin` DOUBLE  DEFAULT NULL,
    `minoq`   DOUBLE       DEFAULT NULL,
    `maxoq`   DOUBLE       DEFAULT NULL,
    `last_trade_date` DATE DEFAULT NULL,
    `source`  VARCHAR(16)  DEFAULT NULL,
    PRIMARY KEY (`code`, `date`),
    INDEX `idx_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_table_exists(config_override: dict | None = None):
    """Create target table if it doesn't already exist."""
    conn = get_connection(config_override)
    table = _table_name(config_override)
    database = _database_name(config_override)
    sql = CREATE_SQL_TPL.format(database=database, table=table)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def upsert_rows(records: list[dict], config_override: dict | None = None):
    """INSERT ... ON DUPLICATE KEY UPDATE for a batch of records."""
    if not records:
        return 0

    conn = get_connection(config_override)
    table = _table_name(config_override)
    database = _database_name(config_override)

    col_names = list(records[0].keys())
    col_str = ", ".join(f"`{c}`" for c in col_names)
    placeholder_str = ", ".join(["%s"] * len(col_names))
    update_str = ", ".join(
        f"`{c}` = VALUES(`{c}`)" for c in col_names
        if c not in ("code", "date")
    )

    sql = (
        f"INSERT INTO `{database}`.`{table}` ({col_str}) "
        f"VALUES ({placeholder_str}) "
        f"ON DUPLICATE KEY UPDATE {update_str}"
    )

    try:
        with conn.cursor() as cur:
            affected = 0
            for rec in records:
                values = [rec.get(c) for c in col_names]
                cur.execute(sql, values)
                affected += cur.rowcount
        conn.commit()
        return affected
    finally:
        conn.close()
