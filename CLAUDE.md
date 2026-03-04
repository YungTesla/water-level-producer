# water-level-producer

Water level ingestion service: HTTP poll (6 European APIs) → Redpanda → Iceberg.

## What this service does

Polls 77 water level stations across 6 European API providers every 10 minutes, produces readings to Redpanda, and sinks to Iceberg. Extracted from the ais-producer monolith.

## Data providers

| Provider | Country | Stations | API |
|----------|---------|----------|-----|
| RWS (Rijkswaterstaat) | Netherlands | 18 | REST POST |
| Pegelonline | Germany | 34 | REST GET |
| Hub'Eau | France | 10 | REST GET |
| IMGW | Poland | 6 | REST GET (bulk) |
| KiWIS (waterinfo.be) | Belgium | 4 | REST GET |
| Heichwaasser | Luxembourg | 5 | REST GET (bulk) |

## Architecture

```
HTTP poll (6 providers, 77 stations, every 600s)
  → src/producers/water_producer.py  (parse → Kafka)
  → Redpanda topic: water.levels
  → src/consumers/iceberg_sink.py    (Kafka → Iceberg)
  → Iceberg table: maritime.water_levels
```

## Key files

| File | Purpose |
|------|---------|
| `src/main.py` | Entrypoint — starts producer + 1 sink thread |
| `src/config.py` | All configuration (APIs, stations, Kafka, Nessie, MinIO) |
| `src/schemas.py` | PyArrow + Iceberg schema, table creation |
| `src/producers/water_producer.py` | Per-provider HTTP fetchers + poll loop |
| `src/consumers/iceberg_sink.py` | Generic Kafka → Iceberg sink with retry logic |

## Running

```bash
# Local
make install && make run

# Docker
docker compose up -d
```

Requires `.env` with Kafka/Nessie/MinIO connection details. See `.env.example`. No API keys needed — all providers are public.

## Related

- **ais-producer** — sibling service for AIS vessel data (separate repo)
- **Planning card** — `plans/projects/water-level-producer.md`
