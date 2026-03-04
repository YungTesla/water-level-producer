"""Iceberg sink consumer: Kafka → batch buffer → Iceberg append."""

import json
import logging
import re
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from src.config import (
    BATCH_SIZE,
    FLUSH_INTERVAL_S,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    KAFKA_SASL_MECHANISM,
)
from src.schemas import (
    WATER_LEVELS_SCHEMA,
    NAMESPACE,
)

logger = logging.getLogger(__name__)


def _build_consumer_config(group_id: str) -> dict:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    if KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD:
        config.update({
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": KAFKA_SASL_MECHANISM,
            "sasl.username": KAFKA_SASL_USERNAME,
            "sasl.password": KAFKA_SASL_PASSWORD,
        })
    return config


def _parse_ts(value) -> datetime | None:
    """Convert a timestamp string to a UTC-aware datetime.

    Handles:
    - ISO-8601 with Z suffix: "2026-02-23T20:01:33Z"
    - ISO-8601 with offset:   "2026-02-23T20:01:33+00:00"
    """
    if value is None or isinstance(value, datetime):
        return value
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Truncate sub-microsecond precision to 6 decimal digits
    s = re.sub(r"(\.\d{6})\d+", r"\1", s)
    # Normalize timezone offset +HHMM → +HH:MM (fromisoformat requires the colon)
    s = re.sub(r"([+-])(\d{2})(\d{2})$", r"\1\2:\3", s)
    # Strip any space before the timezone offset
    s = re.sub(r"\s+([+-]\d{2}:\d{2})$", r"\1", s)
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _records_to_columns(records: list[dict], schema: pa.Schema) -> dict[str, list]:
    """Convert a list of dicts to a column-oriented dict matching the schema."""
    columns: dict[str, list] = {field.name: [] for field in schema}
    for record in records:
        for field in schema:
            value = record.get(field.name)
            if pa.types.is_timestamp(field.type):
                value = _parse_ts(value)
            columns[field.name].append(value)
    return columns


class IcebergSink:
    def __init__(self, topic: str, group_id: str, table: Table, pa_schema: pa.Schema):
        self.topic = topic
        self.table = table
        self.pa_schema = pa_schema
        self.buffer: list[dict[str, Any]] = []
        self.last_flush = time.monotonic()

        self.consumer = Consumer(_build_consumer_config(group_id))
        self.consumer.subscribe([topic])
        logger.info("IcebergSink subscribed to topic: %s (group: %s)", topic, group_id)

    def run(self, stop_event: threading.Event) -> None:
        """Called from a daemon thread. Poll Kafka, buffer records, flush to Iceberg."""
        logger.info("IcebergSink running for topic: %s", self.topic)
        try:
            while not stop_event.is_set():
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    pass
                elif msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error on %s: %s", self.topic, msg.error())
                else:
                    try:
                        record = json.loads(msg.value())
                        self.buffer.append(record)
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning("Failed to decode message on %s: %s", self.topic, e)

                if self._should_flush():
                    self._flush()

            # Final flush on shutdown
            if self.buffer:
                self._flush()
        finally:
            self.consumer.close()
            logger.info("IcebergSink closed for topic: %s", self.topic)

    def _should_flush(self) -> bool:
        if len(self.buffer) >= BATCH_SIZE:
            return True
        if self.buffer and (time.monotonic() - self.last_flush) >= FLUSH_INTERVAL_S:
            return True
        return False

    def _flush(self) -> None:
        if not self.buffer:
            return

        count = len(self.buffer)
        try:
            columns = _records_to_columns(self.buffer, self.pa_schema)
            arrow_table = pa.table(columns, schema=self.pa_schema)
            self.table.append(arrow_table)
            logger.info(
                "Flushed %d records to Iceberg table: %s",
                count,
                self.topic,
            )
        except Exception as e:
            logger.error(
                "Failed to flush %d records to %s: %s",
                count,
                self.topic,
                e,
            )
        finally:
            self.buffer.clear()
            self.last_flush = time.monotonic()


def run_all_sinks(
    catalog: Catalog,
    stop_event: threading.Event,
    executor: ThreadPoolExecutor,
) -> list[Future]:
    """Create and start the water_levels IcebergSink thread."""
    sink_configs = [
        (
            "water.levels",
            "iceberg-sink-water-levels",
            f"{NAMESPACE}.water_levels",
            WATER_LEVELS_SCHEMA,
        ),
    ]

    futures = []
    for topic, group_id, table_name, pa_schema in sink_configs:
        table = catalog.load_table(table_name)
        sink = IcebergSink(topic=topic, group_id=group_id, table=table, pa_schema=pa_schema)
        future = executor.submit(sink.run, stop_event)
        futures.append(future)
        logger.info("Started IcebergSink thread for topic: %s", topic)

    return futures
