from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from functools import cache
import json
import os
from typing import Any, Protocol


class OperatorFlowType(StrEnum):
	WEATHER_CONTROL = "weather_control"
	SIGNAL_PREVIEW = "signal_preview"
	BASELINE_LP = "baseline_lp"
	GATEKEEPER = "gatekeeper"
	DISPATCH_EXECUTION = "dispatch_execution"


class OperatorFlowStatus(StrEnum):
	IDLE = "idle"
	PREPARED = "prepared"
	RUNNING = "running"
	COMPLETED = "completed"
	FAILED = "failed"


@dataclass(slots=True)
class OperatorStatusRecord:
	tenant_id: str
	flow_type: OperatorFlowType
	status: OperatorFlowStatus
	updated_at: datetime
	payload: dict[str, Any] | None = None
	last_error: str | None = None


class OperatorStatusStore(Protocol):
	def upsert_status(self, record: OperatorStatusRecord) -> None: ...

	def get_status(self, *, tenant_id: str, flow_type: OperatorFlowType) -> OperatorStatusRecord | None: ...


class NullOperatorStatusStore:
	def __init__(self) -> None:
		self._records: dict[tuple[str, OperatorFlowType], OperatorStatusRecord] = {}

	def upsert_status(self, record: OperatorStatusRecord) -> None:
		self._records[(record.tenant_id, record.flow_type)] = record

	def get_status(self, *, tenant_id: str, flow_type: OperatorFlowType) -> OperatorStatusRecord | None:
		return self._records.get((tenant_id, flow_type))


class PostgresOperatorStatusStore:
	def __init__(self, dsn: str) -> None:
		self._dsn = dsn
		self._ensure_schema()

	def _connect(self):
		from psycopg import connect
		from psycopg.rows import dict_row

		return connect(self._dsn, row_factory=dict_row)

	def _ensure_schema(self) -> None:
		with self._connect() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					CREATE TABLE IF NOT EXISTS operator_flow_status (
					    tenant_id TEXT NOT NULL,
					    flow_type TEXT NOT NULL,
					    status TEXT NOT NULL,
					    updated_at TIMESTAMPTZ NOT NULL,
					    payload_json JSONB,
					    last_error TEXT,
					    PRIMARY KEY (tenant_id, flow_type)
					)
					"""
				)
			connection.commit()

	def upsert_status(self, record: OperatorStatusRecord) -> None:
		payload_json = json.dumps(record.payload) if record.payload is not None else None
		with self._connect() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					INSERT INTO operator_flow_status (
					    tenant_id,
					    flow_type,
					    status,
					    updated_at,
					    payload_json,
					    last_error
					)
					VALUES (%s, %s, %s, %s, %s::jsonb, %s)
					ON CONFLICT (tenant_id, flow_type)
					DO UPDATE SET
					    status = EXCLUDED.status,
					    updated_at = EXCLUDED.updated_at,
					    payload_json = EXCLUDED.payload_json,
					    last_error = EXCLUDED.last_error
					""",
					(
						record.tenant_id,
						record.flow_type.value,
						record.status.value,
						record.updated_at,
						payload_json,
						record.last_error,
					),
				)
			connection.commit()

	def get_status(self, *, tenant_id: str, flow_type: OperatorFlowType) -> OperatorStatusRecord | None:
		with self._connect() as connection:
			with connection.cursor() as cursor:
				cursor.execute(
					"""
					SELECT tenant_id, flow_type, status, updated_at, payload_json, last_error
					FROM operator_flow_status
					WHERE tenant_id = %s AND flow_type = %s
					""",
					(tenant_id, flow_type.value),
				)
				row = cursor.fetchone()

		if row is None:
			return None

		payload = row["payload_json"]
		if isinstance(payload, str):
			payload = json.loads(payload)

		return OperatorStatusRecord(
			tenant_id=row["tenant_id"],
			flow_type=OperatorFlowType(row["flow_type"]),
			status=OperatorFlowStatus(row["status"]),
			updated_at=row["updated_at"],
			payload=payload,
			last_error=row["last_error"],
		)


def utc_now() -> datetime:
	return datetime.now(tz=UTC)


@cache
def get_operator_status_store() -> OperatorStatusStore:
	dsn = os.getenv("SMART_ARBITRAGE_OPERATOR_STATUS_DSN", "").strip()
	if not dsn:
		return NullOperatorStatusStore()

	return PostgresOperatorStatusStore(dsn)