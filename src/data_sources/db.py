"""MySQL database connection and upsert operations for the data pipeline.

New table naming: t_futures_info_{source} where source = "exchange" (cffex, shfe, etc.)
Or a combined table for all exchange-sourced data.

All functions accept an optional config_override dict to support runtime
parameterization (e.g. switching host/database/table for different deployment
phases without modifying global state).
"""

import os
import pymysql

# 所有 DB 连接参数通过环境变量传入，pipeline.sh 是唯一配置入口。
# 未设置时直接报错，不做静默回退。


def resolve_config(config_override: dict | None = None) -> dict:
    """Return DB connection config, reading strictly from env vars.

    Required env vars: DB_HOST, DB_USER, DB_DATABASE.
    Optional: DB_PASSWORD (defaults to empty string).
    Config dict may contain 'table' key (stripped before pymysql.connect).
    """
    cfg = {
        "host":     os.environ["DB_HOST"],
        "user":     os.environ["DB_USER"],
        "password": os.environ.get("DB_PASSWORD", ""),
        "database": os.environ["DB_DATABASE"],
        "charset":  "utf8mb4",
    }
    if config_override:
        for k, v in config_override.items():
            if v is not None and k not in ("table",):
                cfg[k] = v
    return cfg


def get_connection(config_override: dict | None = None):
    """Return a pymysql connection. Strips 'table' before connecting."""
    cfg = resolve_config(config_override)
    return pymysql.connect(**cfg)


def fetch_table(table: str, date: str, config_override: dict | None = None) -> list[dict]:
    """Read all rows from a MySQL table for a given date.

    Args:
        table: MySQL table name (e.g. "t_futures_info_exchange")
        date: YYYYMMDD
        config_override: optional dict to override host/user/password

    Returns:
        list of records [{code, date, open, high, ...}]
    """
    conn = get_connection(config_override)
    conn.cursorclass = pymysql.cursors.DictCursor
    database = resolve_config(config_override)["database"]
    try:
        dt = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM `{database}`.`{table}` WHERE date = %s",
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


# 已知 DB 列白名单：只 INSERT 表中实际存在的列，防止 parser 残留内部字段导致报错
_DB_COLS = [
    "code", "date", "open", "high", "low", "close",
    "volume", "amt", "oi", "settle", "maxup", "maxdown",
    "if_basis", "long_margin", "short_margin", "minoq", "maxoq",
]


def delete_rows(date: str, table: str,
                  config_override: dict | None = None) -> int:
    """Delete all records for a given date from a table."""
    conn = get_connection(config_override)
    database = resolve_config(config_override)["database"]
    dt = date if "-" in date else f"{date[:4]}-{date[4:6]}-{date[6:8]}"
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM `{database}`.`{table}` WHERE date = %s",
                (dt,),
            )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def upsert_rows(records: list[dict], table: str,
                config_override: dict | None = None):
    """INSERT ... ON DUPLICATE KEY UPDATE for a batch of records."""
    if not records:
        return 0

    conn = get_connection(config_override)
    database = resolve_config(config_override)["database"]

    col_str = ", ".join(f"`{c}`" for c in _DB_COLS)
    placeholder_str = ", ".join(["%s"] * len(_DB_COLS))
    update_str = ", ".join(
        f"`{c}` = VALUES(`{c}`)" for c in _DB_COLS
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
                # 列白名单过滤：只 INSERT 已知列，防止 parser 残留内部字段导致报错
                values = [rec.get(c) for c in _DB_COLS]
                cur.execute(sql, values)
                affected += cur.rowcount
        conn.commit()
        return affected
    finally:
        conn.close()
