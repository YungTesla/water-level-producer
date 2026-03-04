"""PyArrow schemas and Iceberg table definitions for the water level pipeline."""

import logging

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    FloatType,
    NestedField,
    StringType,
    TimestamptzType,
)

logger = logging.getLogger(__name__)

NAMESPACE = "maritime"

# ---------------------------------------------------------------------------
# PyArrow schema (used by the Iceberg sink for writing batches)
# ---------------------------------------------------------------------------

WATER_LEVELS_SCHEMA = pa.schema([
    pa.field("station_id", pa.string(), nullable=False),
    pa.field("station_name", pa.string()),
    pa.field("source", pa.string()),
    pa.field("reference_datum", pa.string()),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("water_level_cm", pa.float32()),
    pa.field("lat", pa.float32()),
    pa.field("lon", pa.float32()),
])

# ---------------------------------------------------------------------------
# Iceberg schema (field IDs are 1-based and must be explicit)
# ---------------------------------------------------------------------------

WATER_LEVELS_ICEBERG = Schema(
    NestedField(1, "station_id",       StringType(),      required=True),
    NestedField(2, "station_name",     StringType()),
    NestedField(3, "source",           StringType()),
    NestedField(4, "reference_datum",  StringType()),
    NestedField(5, "timestamp",        TimestamptzType(), required=True),
    NestedField(6, "water_level_cm",   FloatType()),
    NestedField(7, "lat",              FloatType()),
    NestedField(8, "lon",              FloatType()),
)


def ensure_tables(catalog: Catalog) -> None:
    """Create namespace and water_levels Iceberg table if they don't already exist."""
    try:
        catalog.create_namespace(NAMESPACE)
        logger.info("Created Iceberg namespace: %s", NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass

    full_name = f"{NAMESPACE}.water_levels"
    try:
        catalog.load_table(full_name)
        logger.info("Iceberg table already exists: %s", full_name)
    except NoSuchTableError:
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=5,
                field_id=1000,
                transform=DayTransform(),
                name="timestamp_day",
            )
        )
        catalog.create_table(
            identifier=full_name,
            schema=WATER_LEVELS_ICEBERG,
            partition_spec=partition_spec,
        )
        logger.info("Created Iceberg table: %s", full_name)
