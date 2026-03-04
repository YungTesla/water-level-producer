"""Water level producer: HTTP poll → parse → Kafka."""

import asyncio
import json
import logging
import time
import traceback
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from zoneinfo import ZoneInfo

import aiohttp
from confluent_kafka import Producer

from src.config import (
    RWS_BASE_URL, PEGELONLINE_BASE_URL, HUBEAU_BASE_URL,
    IMGW_HYDRO_URL, KIWIS_BASE_URL, HEICHWAASSER_BASE_URL,
    WATER_STATIONS, WATER_POLL_INTERVAL_S,
)

logger = logging.getLogger(__name__)

_RWS_LATEST_URL = (
    f"{RWS_BASE_URL}/ONLINEWAARNEMINGENSERVICES/OphalenLaatsteWaarnemingen"
)

_AQUO_METADATA = {
    "AquoMetadata": {
        "Compartiment": {"Code": "OW"},
        "Grootheid": {"Code": "WATHTE"},
        "Eenheid": {"Code": "cm"},
        "Hoedanigheid": {"Code": "NAP"},
    }
}

_WARSAW_TZ = ZoneInfo("Europe/Warsaw")
_MIN_VALID_YEAR = "2020"

# IMGW cache: bulk-fetch alle stations eenmaal per poll-cyclus
_imgw_cache: dict | None = None
_imgw_cache_ts: float = 0.0

# Heichwaasser (LU) readings cache: {city_lower: {timestamp, value}}
_lu_cache: dict[str, dict] | None = None
_lu_cache_ts: float = 0.0


def _parse_rws_timestamp(ts_str: str) -> str:
    dt = datetime.fromisoformat(ts_str)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Provider: RWS (Nederland)
# ---------------------------------------------------------------------------

async def _fetch_rws(session: aiohttp.ClientSession, station_code: str) -> dict | None:
    payload = {
        "LocatieLijst": [{"Code": station_code}],
        "AquoPlusWaarnemingMetadataLijst": [_AQUO_METADATA],
    }
    try:
        async with session.post(
            _RWS_LATEST_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status == 204:
                logger.debug("No data for RWS station %s (HTTP 204)", station_code)
                return None
            resp.raise_for_status()
            data = await resp.json()

            if not isinstance(data, dict) or not data.get("Succesvol"):
                logger.debug("No data for RWS station %s: %s",
                             station_code, data.get("Foutmelding") if isinstance(data, dict) else data)
                return None

            waarnemingen = data.get("WaarnemingenLijst", [])
            if not waarnemingen:
                return None

            # Find the WaarnemingenLijst entry with the most recent MetingenLijst entry
            best_tijdstip = None
            best_waarde = None
            for waarneming in waarnemingen:
                metingen = waarneming.get("MetingenLijst", [])
                for meting in metingen:
                    tijdstip = meting.get("Tijdstip")
                    waarde = (meting.get("Meetwaarde") or {}).get("Waarde_Numeriek")
                    if tijdstip and waarde is not None:
                        if best_tijdstip is None or tijdstip > best_tijdstip:
                            best_tijdstip = tijdstip
                            best_waarde = waarde

            if best_tijdstip is None:
                return None

            return {"timestamp": _parse_rws_timestamp(best_tijdstip), "value": float(best_waarde)}

    except aiohttp.ClientError as e:
        logger.warning("HTTP error RWS station %s: %s", station_code, e)
        return None
    except (AttributeError, KeyError, IndexError, ValueError) as e:
        logger.warning("Parse error RWS station %s: %s", station_code, e)
        return None


# ---------------------------------------------------------------------------
# Provider: PEGELONLINE (Duitsland)
# ---------------------------------------------------------------------------

async def _fetch_pegelonline(session: aiohttp.ClientSession, shortname: str) -> dict | None:
    url = f"{PEGELONLINE_BASE_URL}/stations/{quote(shortname)}/W/currentmeasurement.json"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 404:
                return None

            resp.raise_for_status()
            data = await resp.json()

            value = data.get("value")
            timestamp = data.get("timestamp")

            if value is None or timestamp is None:
                return None

            return {"timestamp": _parse_rws_timestamp(timestamp), "value": float(value)}

    except aiohttp.ClientError as e:
        logger.warning("HTTP error PEGELONLINE station %s: %s", shortname, e)
        return None
    except (KeyError, ValueError) as e:
        logger.warning("Parse error PEGELONLINE station %s: %s", shortname, e)
        return None


# ---------------------------------------------------------------------------
# Provider: Hub'Eau (Frankrijk)
# ---------------------------------------------------------------------------

async def _fetch_hubeau(session: aiohttp.ClientSession, station_code: str) -> dict | None:
    url = (
        f"{HUBEAU_BASE_URL}/observations_tr"
        f"?code_entite={station_code}&grandeur_hydro=H&size=1&sort=desc"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            body = await resp.json()

            records = body.get("data", [])
            if not records:
                return None

            obs = records[0]
            value_mm = obs.get("resultat_obs")
            timestamp = obs.get("date_obs")

            if value_mm is None or timestamp is None:
                return None

            return {
                "timestamp": timestamp if timestamp.endswith("Z") else _parse_rws_timestamp(timestamp),
                "value": float(value_mm) / 10.0,  # mm → cm
            }

    except aiohttp.ClientError as e:
        logger.warning("HTTP error Hub'Eau station %s: %s", station_code, e)
        return None
    except (KeyError, IndexError, ValueError) as e:
        logger.warning("Parse error Hub'Eau station %s: %s", station_code, e)
        return None


# ---------------------------------------------------------------------------
# Provider: IMGW (Polen) — bulk-fetch met cache
# ---------------------------------------------------------------------------

async def _fetch_imgw_all(session: aiohttp.ClientSession) -> dict:
    global _imgw_cache, _imgw_cache_ts

    if _imgw_cache is not None and (time.monotonic() - _imgw_cache_ts) < 300:
        return _imgw_cache

    try:
        async with session.get(IMGW_HYDRO_URL, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            resp.raise_for_status()
            stations = await resp.json()

        result = {}
        for s in stations:
            code = s.get("id_stacji")
            value_str = s.get("stan_wody")
            ts_str = s.get("stan_wody_data_pomiaru")

            if not code or not value_str or not ts_str:
                continue

            try:
                dt_local = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                dt_utc = dt_local.replace(tzinfo=_WARSAW_TZ).astimezone(timezone.utc)
                ts_utc = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                result[code] = {"timestamp": ts_utc, "value": float(value_str)}
            except (ValueError, TypeError):
                continue

        _imgw_cache = result
        _imgw_cache_ts = time.monotonic()
        logger.debug("IMGW bulk-fetch: %d stations loaded", len(result))
        return result

    except aiohttp.ClientError as e:
        logger.warning("HTTP error IMGW bulk-fetch: %s", e)
        return _imgw_cache or {}
    except (ValueError, KeyError) as e:
        logger.warning("Parse error IMGW bulk-fetch: %s", e)
        return _imgw_cache or {}


async def _fetch_imgw(session: aiohttp.ClientSession, station_code: str) -> dict | None:
    all_data = await _fetch_imgw_all(session)
    return all_data.get(station_code)


# ---------------------------------------------------------------------------
# Provider: KiWIS / waterinfo.be (België)
# ---------------------------------------------------------------------------

async def _fetch_kiwis(session: aiohttp.ClientSession, ts_id: str) -> dict | None:
    url = (
        f"{KIWIS_BASE_URL}?type=queryServices&service=kisters"
        f"&request=getTimeseriesValues&ts_id={ts_id}"
        f"&period=PT3H&format=json"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            body = await resp.json()

            data_list = body if isinstance(body, list) else body.get("data", [])
            if not data_list:
                return None

            # 'rows' is a count string; actual readings are in 'data'
            rows = data_list[0].get("data", [])
            if not rows:
                return None

            last_row = rows[-1]
            timestamp_str = last_row[0]
            value_raw = last_row[1]

            if value_raw is None or timestamp_str is None:
                return None

            return {
                "timestamp": _parse_rws_timestamp(timestamp_str),
                "value": float(value_raw) * 100.0,  # m → cm
            }

    except aiohttp.ClientError as e:
        logger.warning("HTTP error KiWIS ts_id %s: %s", ts_id, e)
        return None
    except (AttributeError, KeyError, IndexError, ValueError, TypeError) as e:
        logger.warning("Parse error KiWIS ts_id %s: %s", ts_id, e)
        return None


# ---------------------------------------------------------------------------
# Provider: Heichwaasser (Luxemburg)
# ---------------------------------------------------------------------------

async def _lu_refresh_cache(session: aiohttp.ClientSession) -> None:
    """Fetch /api/v1/rivers and populate _lu_cache with {city_lower: {timestamp, value}}.
    Values are already in cm. Timestamps are naive UTC+1; converted to UTC.
    Cached for 10 minutes.
    """
    global _lu_cache, _lu_cache_ts
    if _lu_cache is not None and (time.monotonic() - _lu_cache_ts) < 600:
        return

    url = f"{HEICHWAASSER_BASE_URL}/rivers"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            rivers = await resp.json()

        new_cache: dict[str, dict] = {}
        for river in rivers:
            for st in river.get("stations", []):
                city = st.get("city", "").strip()
                current = st.get("current") or {}
                value = current.get("value")
                ts_raw = current.get("timestamp")
                if not city or value is None or not ts_raw:
                    continue
                # Naive → assume UTC+1 fixed offset (no DST adjustment)
                dt_utc = datetime.fromisoformat(str(ts_raw)).replace(tzinfo=timezone.utc) - timedelta(seconds=3600)
                # First occurrence wins (handles duplicate city names on different rivers)
                new_cache.setdefault(city.lower(), {
                    "timestamp": dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "value": float(value),  # API returns cm
                })

        _lu_cache = new_cache
        _lu_cache_ts = time.monotonic()
        logger.debug("Heichwaasser: %d stations loaded", len(new_cache))
    except aiohttp.ClientError as e:
        logger.warning("HTTP error Heichwaasser /rivers: %s", e)
    except (ValueError, KeyError, TypeError) as e:
        logger.warning("Parse error Heichwaasser /rivers: %s", e)


async def _fetch_luxembourg(session: aiohttp.ClientSession, gauge_name: str) -> dict | None:
    """Return cached current reading for a Luxembourg gauge (city name)."""
    await _lu_refresh_cache(session)
    result = (_lu_cache or {}).get(gauge_name.lower())
    if result is None:
        logger.warning("Heichwaasser: gauge '%s' not found (available: %s)",
                       gauge_name, list((_lu_cache or {}).keys())[:10])
    return result


# ---------------------------------------------------------------------------
# Poll loop
# ---------------------------------------------------------------------------

def _delivery_report(err, msg):
    if err:
        logger.warning("Kafka delivery failed for %s: %s", msg.topic(), err)


async def run_water_producer(kafka_producer: Producer, shutdown: asyncio.Event) -> None:
    """Poll water levels for all providers and produce to Kafka."""
    logger.info(
        "Water producer started (%d stations, interval %ds)",
        len(WATER_STATIONS),
        WATER_POLL_INTERVAL_S,
    )
    _last_published: dict[str, str] = {}

    async with aiohttp.ClientSession() as session:
        while not shutdown.is_set():
            try:
                global _imgw_cache_ts, _lu_cache_ts
                _imgw_cache_ts = 0.0  # Invalidate IMGW cache each cycle
                _lu_cache_ts = 0.0    # Invalidate Luxembourg cache each cycle

                success_count = 0
                for station_id, meta in WATER_STATIONS.items():
                    source = meta["source"]

                    if source == "rws":
                        rws_code = station_id.removeprefix("rws:")
                        result = await _fetch_rws(session, rws_code)
                    elif source == "pegelonline":
                        result = await _fetch_pegelonline(session, meta["shortname"])
                    elif source == "hubeau":
                        result = await _fetch_hubeau(session, meta["station_code"])
                    elif source == "imgw":
                        result = await _fetch_imgw(session, meta["station_code"])
                    elif source == "kiwis":
                        result = await _fetch_kiwis(session, meta["ts_id"])
                    elif source == "luxembourg":
                        result = await _fetch_luxembourg(session, meta["gauge_name"])
                    else:
                        continue

                    if result is not None:
                        if result["timestamp"][:4] < _MIN_VALID_YEAR:
                            logger.debug(
                                "Skipping bogus timestamp %s for %s",
                                result["timestamp"], station_id,
                            )
                            continue
                        if _last_published.get(station_id) == result["timestamp"]:
                            continue
                        _last_published[station_id] = result["timestamp"]
                        payload = {
                            "station_id": station_id,
                            "station_name": meta["name"],
                            "source": source,
                            "reference_datum": meta["reference_datum"],
                            "timestamp": result["timestamp"],
                            "water_level_cm": result["value"],
                            "lat": meta["lat"],
                            "lon": meta["lon"],
                        }
                        kafka_producer.produce(
                            topic="water.levels",
                            key=station_id.encode(),
                            value=json.dumps(payload).encode(),
                            callback=_delivery_report,
                        )
                        kafka_producer.poll(0)
                        success_count += 1

                logger.info(
                    "Water poll complete: %d/%d stations successful",
                    success_count,
                    len(WATER_STATIONS),
                )
            except Exception as e:
                logger.warning("Unexpected error during water poll: %s\n%s", e, traceback.format_exc())

            try:
                await asyncio.wait_for(shutdown.wait(), timeout=WATER_POLL_INTERVAL_S)
            except asyncio.TimeoutError:
                pass

    logger.info("Water producer stopped")
