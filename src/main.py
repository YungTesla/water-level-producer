"""Entrypoint for the water level ingestion pipeline."""

import asyncio
import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Producer
from pyiceberg.catalog import load_catalog

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    KAFKA_SASL_MECHANISM,
    NESSIE_URI,
    S3_ENDPOINT_URL,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    ICEBERG_WAREHOUSE,
)
from src.schemas import ensure_tables
from src.producers.water_producer import run_water_producer
from src.consumers.iceberg_sink import run_all_sinks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _build_kafka_producer() -> Producer:
    config: dict = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    if KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD:
        config.update({
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": KAFKA_SASL_MECHANISM,
            "sasl.username": KAFKA_SASL_USERNAME,
            "sasl.password": KAFKA_SASL_PASSWORD,
        })
    return Producer(config)


def _build_catalog():
    catalog_props = {
        "uri": NESSIE_URI,
        "s3.endpoint": S3_ENDPOINT_URL,
        "s3.access-key-id": S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "warehouse": ICEBERG_WAREHOUSE,
    }
    return load_catalog("nessie", **catalog_props)


async def main() -> None:
    logger.info("Starting water level ingestion pipeline")

    # Initialize Iceberg catalog and ensure tables exist
    catalog = _build_catalog()
    ensure_tables(catalog)

    kafka_producer = _build_kafka_producer()
    stop_event = threading.Event()
    shutdown = asyncio.Event()

    # Handle SIGINT / SIGTERM gracefully
    loop = asyncio.get_running_loop()

    def _signal_handler():
        logger.info("Shutdown signal received")
        shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Start Iceberg sink thread
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="iceberg-sink")
    sink_futures = run_all_sinks(catalog, stop_event, executor)

    # Run producer as async task
    water_task = asyncio.create_task(run_water_producer(kafka_producer, shutdown))

    logger.info("Pipeline running. Press Ctrl+C to stop.")

    # Wait for shutdown signal
    await shutdown.wait()
    logger.info("Shutting down producer...")

    # Cancel producer task
    water_task.cancel()
    await asyncio.gather(water_task, return_exceptions=True)

    # Flush remaining Kafka messages
    logger.info("Flushing Kafka producer...")
    kafka_producer.flush()

    # Signal sink thread to stop and wait
    logger.info("Stopping Iceberg sink thread...")
    stop_event.set()
    executor.shutdown(wait=True)

    # Log any sink exceptions
    for future in sink_futures:
        exc = future.exception()
        if exc:
            logger.error("Iceberg sink thread raised an exception: %s", exc)

    logger.info("Pipeline stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
