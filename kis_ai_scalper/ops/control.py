"""Operator controls with no broker or account side effects."""

from __future__ import annotations

from kis_ai_scalper.storage import RuntimeControl, connect_database


def set_paused(db_path: str, paused: bool, reason: str, source: str) -> RuntimeControl:
    with connect_database(db_path) as database:
        database.init_schema()
        return database.set_runtime_paused(paused, reason, source)


def control_status(db_path: str) -> RuntimeControl:
    with connect_database(db_path) as database:
        database.init_schema()
        return database.get_runtime_control()


def set_environment(db_path: str, environment: str, reason: str, source: str) -> RuntimeControl:
    with connect_database(db_path) as database:
        database.init_schema()
        return database.set_runtime_environment(environment, reason, source)
