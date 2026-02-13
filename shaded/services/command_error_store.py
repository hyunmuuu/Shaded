from __future__ import annotations

import time
from typing import Optional

import aiosqlite
from shaded.services.sqlite_conn import open_db


async def _fetchall(con: aiosqlite.Connection, sql: str, params: tuple):
    if hasattr(con, "execute_fetchall"):
        return await con.execute_fetchall(sql, params)  # type: ignore

    cur = await con.execute(sql, params)
    try:
        return await cur.fetchall()
    finally:
        await cur.close()


async def _fetchone(con: aiosqlite.Connection, sql: str, params: tuple):
    if hasattr(con, "execute_fetchone"):
        return await con.execute_fetchone(sql, params)  # type: ignore

    cur = await con.execute(sql, params)
    try:
        return await cur.fetchone()
    finally:
        await cur.close()


async def init_command_error_log(db_path: str) -> None:
    async with open_db(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS command_error_log (
              id         INTEGER PRIMARY KEY AUTOINCREMENT,
              command    TEXT NOT NULL,
              error      TEXT NOT NULL,
              created_at INTEGER NOT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_command_error_log_cmd_time
            ON command_error_log(command, created_at)
            """
        )
        await db.commit()


async def record_command_error(db_path: str, command: str, error_text: str) -> None:
    cmd = (command or "unknown").strip()[:120]
    err = (error_text or "").strip()
    if not err:
        err = "unknown error"

    now = int(time.time())
    async with open_db(db_path) as db:
        await db.execute(
            "INSERT INTO command_error_log(command, error, created_at) VALUES(?,?,?)",
            (cmd, err, now),
        )
        await db.commit()


async def fetch_last_errors_by_command(db_path: str, since_epoch: int) -> dict[str, tuple[int, str]]:
    """
    return {command: (last_ts, last_error_tail)}
    """
    async with open_db(db_path) as db:
        # 각 command의 최신 ts
        rows = await _fetchall(
            db,
            """
            SELECT command, MAX(created_at) AS last_ts
              FROM command_error_log
             WHERE created_at >= ?
             GROUP BY command
            """,
            (int(since_epoch),),
        )

        out: dict[str, tuple[int, str]] = {}
        for cmd, last_ts in rows:
            r = await _fetchone(
                db,
                """
                SELECT error
                  FROM command_error_log
                 WHERE command=? AND created_at=?
                 ORDER BY id DESC
                 LIMIT 1
                """,
                (str(cmd), int(last_ts)),
            )
            err = str(r[0]) if r else ""
            # 너무 길면 tail만
            tail = err[-900:] if len(err) > 900 else err
            out[str(cmd)] = (int(last_ts), tail)
        return out


async def fetch_recent_errors(db_path: str, limit: int = 20) -> list[tuple[str, int, str]]:
    async with open_db(db_path) as db:
        rows = await _fetchall(
            db,
            """
            SELECT command, created_at, error
              FROM command_error_log
             ORDER BY created_at DESC, id DESC
             LIMIT ?
            """,
            (int(limit),),
        )
        out: list[tuple[str, int, str]] = []
        for cmd, ts, err in rows:
            e = str(err or "")
            e = e[-900:] if len(e) > 900 else e
            out.append((str(cmd), int(ts), e))
        return out


async def clear_errors(db_path: str) -> None:
    async with open_db(db_path) as db:
        await db.execute("DELETE FROM command_error_log")
        await db.commit()
