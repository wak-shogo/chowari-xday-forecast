#!/usr/bin/env python3
import argparse
import hashlib
import html
import json
import math
import random
import re
import urllib.parse
import urllib.request
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PAYLOAD_DIR = DATA_DIR / "payloads"
CATALOG_PATH = DATA_DIR / "catalog.json"

CHOWARI_ROOT = "https://www.chowari.jp"
ICHIROUMARU_ROOT = "https://www.ichiroumaru.jp"
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_MARINE = "https://marine-api.open-meteo.com/v1/marine"
TIMEZONE_NAME = "Asia/Tokyo"
SYNODIC_MONTH = 29.53058867
REFERENCE_NEW_MOON = datetime(2000, 1, 6, 18, 14, tzinfo=timezone.utc)

DEFAULT_SHIP_IDS = ["00296", "00297", "ichiroumaru"]
TRAINING_DAYS = 365
FORECAST_DAYS = 365
WEATHER_HISTORY_DAYS = 365 * 3
FORECAST_API_DAYS = 14
VALIDATION_RATIO = 0.25
MIN_VALIDATION_ROWS = 8
MIN_POSITIVE_DAYS = 4
XDAY_MONTE_CARLO_SAMPLES = 4096
SIMULATOR_MAXIMA_SAMPLES = 320

FEATURE_KEYS = ("airTemp", "seaTemp", "moonSin", "moonCos")
FEATURE_TERMS = [
    "intercept",
    "airTemp",
    "seaTemp",
    "moonSin",
    "moonCos",
    "airTemp*seaTemp",
    "airTemp*moonSin",
    "airTemp*moonCos",
    "seaTemp*moonSin",
    "seaTemp*moonCos",
    "airTemp^2",
    "seaTemp^2",
]

ICHIROUMARU_COORDINATES = (35.23999456165066, 139.72319088316416)
ICHIROUMARU_SHIP_CONFIG = {
    "id": "ichiroumaru",
    "source": "ichiroumaru",
    "name": "鴨居一郎丸",
    "location": "神奈川県横須賀市鴨居",
    "homeUrl": f"{ICHIROUMARU_ROOT}/",
    "catchUrl": f"{ICHIROUMARU_ROOT}/result/",
    "latitude": ICHIROUMARU_COORDINATES[0],
    "longitude": ICHIROUMARU_COORDINATES[1],
}
FULLWIDTH_TRANSLATION = str.maketrans("０１２３４５６７８９．〜～－ｃｍＣＭｋｇＫＧｇ", "0123456789.~~-cmCMkgKGg")
MEASUREMENT_UNIT_PATTERN = r"[^\d\s/・,，。、()（）]+"
COUNT_MEASUREMENT_UNITS = {"匹", "杯", "尾", "本", "枚", "羽", "人"}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--today", type=str, default=None)
    parser.add_argument("--ship", action="append", dest="ships", default=None)
    return parser.parse_args()


def fetch_text(url, params=None):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", "ignore")


def fetch_json(url, params=None):
    return json.loads(fetch_text(url, params))


def clean_fragment(raw):
    raw = re.sub(r"<br\s*/?>", " ", raw)
    raw = re.sub(r"<[^>]+>", "", raw)
    raw = html.unescape(raw)
    return re.sub(r"\s+", " ", raw.replace("\u3000", " ")).strip()


def fullwidth_to_ascii(text):
    return text.translate(FULLWIDTH_TRANSLATION)


def normalize_species_name(name):
    name = clean_fragment(name)
    name = re.sub(r"[（(].*?[）)]", "", name)
    return re.sub(r"\s+", "", name)


def measurement_unit_rank(unit):
    normalized = fullwidth_to_ascii(unit).lower()
    return 0 if normalized in COUNT_MEASUREMENT_UNITS else 1


def parse_measurement(text):
    cleaned = fullwidth_to_ascii(clean_fragment(text))
    if not cleaned:
        return None

    candidates = []
    for match in re.finditer(
        rf"(\d+(?:\.\d+)?)\s*[〜~～\-−]\s*(\d+(?:\.\d+)?)\s*({MEASUREMENT_UNIT_PATTERN})",
        cleaned,
    ):
        candidates.append(
            {
                "min": float(match.group(1)),
                "max": float(match.group(2)),
                "unit": match.group(3),
                "raw": cleaned,
                "isRange": True,
                "start": match.start(),
            }
        )
    for match in re.finditer(rf"(\d+(?:\.\d+)?)\s*({MEASUREMENT_UNIT_PATTERN})", cleaned):
        candidates.append(
            {
                "min": float(match.group(1)),
                "max": float(match.group(1)),
                "unit": match.group(2),
                "raw": cleaned,
                "isRange": False,
                "start": match.start(),
            }
        )
    if candidates:
        best = min(
            candidates,
            key=lambda item: (
                measurement_unit_rank(item["unit"]),
                0 if item["isRange"] else 1,
                item["start"],
            ),
        )
        return {
            "min": best["min"],
            "max": best["max"],
            "unit": best["unit"],
            "raw": best["raw"],
        }
    return None


def month_codes_between(start_day, end_day):
    current = date(end_day.year, end_day.month, 1)
    boundary = date(start_day.year, start_day.month, 1)
    codes = []
    while current >= boundary:
        codes.append(f"{current.year % 100:02d}{current.month:02d}")
        if current.month == 1:
            current = date(current.year - 1, 12, 1)
        else:
            current = date(current.year, current.month - 1, 1)
    return codes


def parse_available_month_codes(page_html):
    return set(re.findall(r'<option value="(\d{4})"', page_html))


def parse_ship_meta(ship_id):
    if ship_id == ICHIROUMARU_SHIP_CONFIG["id"]:
        return parse_ichiroumaru_meta()

    url = f"{CHOWARI_ROOT}/ship/{ship_id}/"
    page_html = fetch_text(url)

    title_match = re.search(r"<title>([^<]+)</title>", page_html)
    title = clean_fragment(title_match.group(1)) if title_match else ship_id
    name = title.split("【", 1)[0].strip()
    location = title.split(" - ", 1)[1].strip() if " - " in title else ""

    marker_match = re.search(r"var s_marker = \{'lat':'([0-9.]+)', 'lng':'([0-9.]+)'", page_html)
    if not marker_match:
        raise RuntimeError(f"Ship coordinates were not found for {ship_id}.")

    return {
        "id": ship_id,
        "name": name,
        "location": location,
        "homeUrl": url,
        "catchUrl": f"{url}catch/",
        "latitude": float(marker_match.group(1)),
        "longitude": float(marker_match.group(2)),
    }


def parse_ichiroumaru_meta():
    page_html = fetch_text(f"{ICHIROUMARU_ROOT}/info.html")
    title_match = re.search(r"<title>([^<]+)</title>", page_html)
    title = clean_fragment(title_match.group(1)) if title_match else ICHIROUMARU_SHIP_CONFIG["name"]
    name = title.split("−", 1)[-1].split("【", 1)[0].strip() if "−" in title else ICHIROUMARU_SHIP_CONFIG["name"]
    coords_match = re.search(r"!2d([0-9.]+)!3d([0-9.]+)", page_html)
    latitude, longitude = ICHIROUMARU_COORDINATES
    if coords_match:
        longitude = float(coords_match.group(1))
        latitude = float(coords_match.group(2))

    return {
        "id": ICHIROUMARU_SHIP_CONFIG["id"],
        "name": name or ICHIROUMARU_SHIP_CONFIG["name"],
        "location": ICHIROUMARU_SHIP_CONFIG["location"],
        "homeUrl": ICHIROUMARU_SHIP_CONFIG["homeUrl"],
        "catchUrl": ICHIROUMARU_SHIP_CONFIG["catchUrl"],
        "latitude": latitude,
        "longitude": longitude,
        "source": "ichiroumaru",
    }


def iter_catch_blocks(page_html, ship_id):
    pattern = re.compile(
        rf'(<li data-ship="{re.escape(ship_id)}" data-choka="(\d+)" class="catch_item.*?</li>\s*<!-- /\.\s*catch_item -->)',
        re.S,
    )
    for match in pattern.finditer(page_html):
        block = match.group(1)
        choka_id = match.group(2)
        yield block, choka_id


def parse_temperature_from_block(block_html):
    match = re.search(r'<li class="temperature">.*?<p>気温</p><p>(.*?)</p>', block_html, re.S)
    if not match:
        return None
    values = [float(value) for value in re.findall(r"-?\d+(?:\.\d+)?", clean_fragment(match.group(1)))]
    if not values:
        return None
    return sum(values) / len(values)


def parse_water_temperature_from_block(block_html):
    match = re.search(r'<li class="water_temperature">.*?<p>水温</p><p>(.*?)</p>', block_html, re.S)
    if not match:
        return None
    values = [float(value) for value in re.findall(r"-?\d+(?:\.\d+)?", clean_fragment(match.group(1)))]
    return values[0] if values else None


def parse_moon_age_from_block(block_html):
    match = re.search(r'<li class="moon[^"]*">.*?<p>月齢</p><p>(.*?)</p>', block_html, re.S)
    if not match:
        return None
    values = [float(value) for value in re.findall(r"-?\d+(?:\.\d+)?", clean_fragment(match.group(1)))]
    return values[0] if values else None


def parse_catch_block(block_html, ship_meta, source_url):
    date_match = re.search(r'<div class="catch_item_date">([^<]+)</div>', block_html, re.S)
    if not date_match:
        return None
    day_match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", clean_fragment(date_match.group(1)))
    if not day_match:
        return None

    report_date = date(int(day_match.group(1)), int(day_match.group(2)), int(day_match.group(3)))
    fish_table = re.search(r'<table class="catch_item_fish">(.*?)</table>', block_html, re.S)
    if not fish_table:
        return None

    species = {}
    for row_html in re.findall(r"<tr>(.*?)</tr>", fish_table.group(1), re.S):
        cols = [clean_fragment(col) for col in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row_html, re.S)]
        if len(cols) < 3:
            continue
        species_name = normalize_species_name(cols[0])
        measurement = parse_measurement(cols[2])
        if not species_name or not measurement:
            continue
        unit_bucket = species.setdefault(species_name, {})
        current = unit_bucket.get(measurement["unit"])
        if not current:
            unit_bucket[measurement["unit"]] = measurement
        else:
            current["min"] = min(current["min"], measurement["min"])
            current["max"] = max(current["max"], measurement["max"])

    if not species:
        return None

    location_match = re.search(r'<div class="catch_item_location">([^<]+)</div>', block_html)
    location = clean_fragment(location_match.group(1)) if location_match else ship_meta["location"]

    return {
        "date": report_date,
        "sourceUrl": source_url,
        "location": location,
        "airTemp": parse_temperature_from_block(block_html),
        "seaTemp": parse_water_temperature_from_block(block_html),
        "moonAge": parse_moon_age_from_block(block_html),
        "species": species,
    }


def parse_ichiroumaru_list_page(page_html):
    items = []
    pattern = re.compile(
        r'<a class="result__list__item__link" href="(\./detail\.html\?[^"]+)".*?<time class="result__list__item__link__date" datetime="([^"]+)"',
        re.S,
    )
    for match in pattern.finditer(page_html):
        try:
            report_date = date.fromisoformat(match.group(2))
        except ValueError:
            continue
        items.append(
            {
                "date": report_date,
                "url": urllib.parse.urljoin(f"{ICHIROUMARU_ROOT}/result/", match.group(1)),
            }
        )
    return items


def parse_ichiroumaru_species_cards(page_html):
    pattern = re.compile(
        r'<div class="result-detail__list__item">.*?<div class="result-detail__list__item__head__title">\s*(.*?)\s*</div>.*?<div class="result-detail__list__item__main__number">\s*(.*?)\s*</div>',
        re.S,
    )
    cards = []
    for match in pattern.finditer(page_html):
        species_name = normalize_species_name(match.group(1))
        number_text = fullwidth_to_ascii(clean_fragment(match.group(2)))
        max_match = re.search(r"(\d+(?:\.\d+)?)", number_text)
        cards.append(
            {
                "species": species_name,
                "topMax": float(max_match.group(1)) if max_match else None,
            }
        )
    return cards


def extract_text_lines(block_html):
    text = re.sub(r"<br\s*/?>", "\n", block_html)
    text = re.sub(r"</p>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text).replace("\u3000", " ")
    lines = []
    for raw_line in text.splitlines():
        cleaned = fullwidth_to_ascii(re.sub(r"\s+", " ", raw_line)).strip()
        if cleaned and cleaned != "&nbsp;":
            lines.append(cleaned)
    return lines


def parse_ichiroumaru_location_from_line(species_name, line, fallback):
    match = re.match(
        rf"^{re.escape(species_name)}\s+(.+?)\s+\d+(?:\.\d+)?\s*[〜~～\-−]\s*\d+(?:\.\d+)?\s*{MEASUREMENT_UNIT_PATTERN}",
        line,
    )
    return match.group(1).strip() if match else fallback


def parse_ichiroumaru_detail(page_html, ship_meta, source_url):
    date_match = re.search(r'<time class="result-detail__head__info__date" datetime="([^"]+)"', page_html)
    if not date_match:
        return None
    try:
        report_date = date.fromisoformat(date_match.group(1))
    except ValueError:
        return None

    content_match = re.search(r'<div class="result-detail__content">(.*?)</div>\s*<div class="result-detail__action">', page_html, re.S)
    if not content_match:
        return None
    lines = extract_text_lines(content_match.group(1))
    species_cards = parse_ichiroumaru_species_cards(page_html)
    if not species_cards:
        return None

    species = {}
    report_location = ship_meta["location"]
    for card in species_cards:
        matched_line = None
        for line in lines:
            if not line.startswith(f"{card['species']} "):
                continue
            measurement = parse_measurement(line)
            if not measurement:
                continue
            matched_line = (line, measurement)
            break

        if matched_line:
            line, measurement = matched_line
            report_location = parse_ichiroumaru_location_from_line(card["species"], line, report_location)
        elif card["topMax"] is not None:
            measurement = {
                "min": 0.0,
                "max": card["topMax"],
                "unit": "匹",
                "raw": f"0〜{int(card['topMax']) if float(card['topMax']).is_integer() else card['topMax']}匹",
            }
        else:
            continue

        species.setdefault(card["species"], {})[measurement["unit"]] = measurement

    if not species:
        return None

    return {
        "date": report_date,
        "sourceUrl": source_url,
        "location": report_location,
        "airTemp": None,
        "seaTemp": None,
        "moonAge": None,
        "species": species,
    }


def collect_ship_reports(ship_meta, training_start, today):
    if ship_meta.get("source") == "ichiroumaru":
        return collect_ichiroumaru_reports(ship_meta, training_start, today)

    index_html = fetch_text(ship_meta["catchUrl"])
    available_months = parse_available_month_codes(index_html)
    month_codes = [code for code in month_codes_between(training_start, today) if code in available_months]

    seen_ids = set()
    daily = {}
    for month_code in month_codes:
        source_url = f'{ship_meta["catchUrl"]}?dt={month_code}'
        page_html = fetch_text(ship_meta["catchUrl"], {"dt": month_code})
        for block_html, choka_id in iter_catch_blocks(page_html, ship_meta["id"]):
            if choka_id in seen_ids:
                continue
            seen_ids.add(choka_id)

            report = parse_catch_block(block_html, ship_meta, source_url)
            if not report:
                continue
            if not (training_start <= report["date"] <= today):
                continue

            key = report["date"].isoformat()
            current = daily.get(key)
            if not current:
                current = {
                    "date": report["date"],
                    "location": report["location"],
                    "airTemp": report["airTemp"],
                    "seaTemp": report["seaTemp"],
                    "moonAge": report["moonAge"],
                    "sourceUrls": [report["sourceUrl"]],
                    "tripCount": 0,
                    "species": {},
                }
                daily[key] = current
            current["tripCount"] += 1
            if report["sourceUrl"] not in current["sourceUrls"]:
                current["sourceUrls"].append(report["sourceUrl"])
            if current["airTemp"] is None and report["airTemp"] is not None:
                current["airTemp"] = report["airTemp"]
            if current["seaTemp"] is None and report["seaTemp"] is not None:
                current["seaTemp"] = report["seaTemp"]
            if current["moonAge"] is None and report["moonAge"] is not None:
                current["moonAge"] = report["moonAge"]
            if not current["location"] and report["location"]:
                current["location"] = report["location"]

            for species_name, units in report["species"].items():
                species_bucket = current["species"].setdefault(species_name, {})
                for unit, measurement in units.items():
                    unit_bucket = species_bucket.get(unit)
                    if not unit_bucket:
                        species_bucket[unit] = dict(measurement)
                    else:
                        unit_bucket["min"] = min(unit_bucket["min"], measurement["min"])
                        unit_bucket["max"] = max(unit_bucket["max"], measurement["max"])

    return [daily[key] for key in sorted(daily.keys())]


def collect_ichiroumaru_reports(ship_meta, training_start, today):
    seen_urls = set()
    daily = {}
    page_number = 1

    while True:
        if page_number == 1:
            page_url = ship_meta["catchUrl"]
        else:
            page_url = urllib.parse.urljoin(ship_meta["catchUrl"], f"index.html?page={page_number}")
        page_html = fetch_text(page_url)
        items = parse_ichiroumaru_list_page(page_html)
        if not items:
            break

        page_has_in_range = False
        oldest_date = min(item["date"] for item in items)
        for item in items:
            if item["url"] in seen_urls:
                continue
            seen_urls.add(item["url"])
            if item["date"] < training_start:
                continue
            if item["date"] > today:
                continue

            page_has_in_range = True
            report_html = fetch_text(item["url"])
            report = parse_ichiroumaru_detail(report_html, ship_meta, item["url"])
            if not report or not (training_start <= report["date"] <= today):
                continue

            key = report["date"].isoformat()
            current = daily.get(key)
            if not current:
                current = {
                    "date": report["date"],
                    "location": report["location"],
                    "airTemp": report["airTemp"],
                    "seaTemp": report["seaTemp"],
                    "moonAge": report["moonAge"],
                    "sourceUrls": [report["sourceUrl"]],
                    "tripCount": 0,
                    "species": {},
                }
                daily[key] = current

            current["tripCount"] += 1
            if report["sourceUrl"] not in current["sourceUrls"]:
                current["sourceUrls"].append(report["sourceUrl"])
            if report["location"]:
                current["location"] = report["location"]

            for species_name, units in report["species"].items():
                species_bucket = current["species"].setdefault(species_name, {})
                for unit, measurement in units.items():
                    unit_bucket = species_bucket.get(unit)
                    if not unit_bucket:
                        species_bucket[unit] = dict(measurement)
                    else:
                        unit_bucket["min"] = min(unit_bucket["min"], measurement["min"])
                        unit_bucket["max"] = max(unit_bucket["max"], measurement["max"])

        if oldest_date < training_start and not page_has_in_range:
            break
        if oldest_date < training_start:
            break
        page_number += 1

    return [daily[key] for key in sorted(daily.keys())]


def fetch_open_meteo_daily(base_url, latitude, longitude, start_date, end_date, fields):
    payload = fetch_json(
        base_url,
        {
            "latitude": latitude,
            "longitude": longitude,
            "timezone": TIMEZONE_NAME,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": ",".join(fields),
        },
    )
    daily = payload["daily"]
    output = {}
    for index, day in enumerate(daily["time"]):
        output[day] = {field: daily[field][index] for field in fields}
    return output


def combine_feature_sources(air_map, sea_map):
    combined = {}
    for day, values in air_map.items():
        combined.setdefault(day, {}).update(values)
    for day, values in sea_map.items():
        combined.setdefault(day, {}).update(values)
    return combined


def circular_distance(day_a, day_b, span=366):
    delta = abs(day_a - day_b)
    return min(delta, span - delta)


def build_climatology(feature_map):
    buckets = {}
    for iso, values in feature_map.items():
        air = values.get("temperature_2m_mean")
        sea = values.get("sea_surface_temperature_mean")
        if air is None or sea is None:
            continue
        day = date.fromisoformat(iso)
        doy = day.timetuple().tm_yday
        bucket = buckets.setdefault(doy, {"temperature_2m_mean": [], "sea_surface_temperature_mean": []})
        bucket["temperature_2m_mean"].append(air)
        bucket["sea_surface_temperature_mean"].append(sea)

    global_air = [value for bucket in buckets.values() for value in bucket["temperature_2m_mean"]]
    global_sea = [value for bucket in buckets.values() for value in bucket["sea_surface_temperature_mean"]]
    if not global_air or not global_sea:
        raise RuntimeError("Weather climatology could not be built.")

    climatology = {}
    for doy in range(1, 367):
        radius = 6
        air_values = []
        sea_values = []
        while radius <= 45 and (not air_values or not sea_values):
            air_values = []
            sea_values = []
            for other_doy, values in buckets.items():
                if circular_distance(doy, other_doy) <= radius:
                    air_values.extend(values["temperature_2m_mean"])
                    sea_values.extend(values["sea_surface_temperature_mean"])
            radius += 6
        climatology[doy] = {
            "temperature_2m_mean": sum(air_values or global_air) / len(air_values or global_air),
            "sea_surface_temperature_mean": sum(sea_values or global_sea) / len(sea_values or global_sea),
        }
    return climatology


def moon_age_for(day):
    instant = datetime.combine(day, time(hour=12), tzinfo=timezone.utc)
    delta_days = (instant - REFERENCE_NEW_MOON).total_seconds() / 86400.0
    return delta_days % SYNODIC_MONTH


def moon_phase_components(moon_age):
    angle = (moon_age / SYNODIC_MONTH) * math.tau
    return math.sin(angle), math.cos(angle)


def same_day_last_year(day):
    target_year = day.year - 1
    target_day = day.day
    while target_day > 0:
        try:
            return date(target_year, day.month, target_day)
        except ValueError:
            target_day -= 1
    raise RuntimeError(f"Could not resolve prior-year date for {day.isoformat()}.")


def resolve_prediction_feature(day, archive_map, forecast_map, climatology):
    iso = day.isoformat()
    baseline = climatology[day.timetuple().tm_yday]
    if iso in forecast_map:
        record = dict(baseline)
        record.update({key: value for key, value in forecast_map[iso].items() if value is not None})
        return record, "forecast"
    if iso in archive_map:
        record = dict(baseline)
        record.update({key: value for key, value in archive_map[iso].items() if value is not None})
        return record, "archive"
    return dict(baseline), "climatology"


def resolve_training_feature(day_record, archive_map, forecast_map, climatology):
    iso = day_record["date"].isoformat()
    baseline = climatology[day_record["date"].timetuple().tm_yday]
    archive = archive_map.get(iso, {})
    forecast = forecast_map.get(iso, {})
    air = day_record["airTemp"]
    sea = day_record["seaTemp"]
    if air is None:
        air = archive.get("temperature_2m_mean")
    if air is None:
        air = forecast.get("temperature_2m_mean", baseline["temperature_2m_mean"])
    if sea is None:
        sea = archive.get("sea_surface_temperature_mean")
    if sea is None:
        sea = forecast.get("sea_surface_temperature_mean", baseline["sea_surface_temperature_mean"])
    moon_age = day_record["moonAge"] if day_record["moonAge"] is not None else moon_age_for(day_record["date"])
    moon_sin, moon_cos = moon_phase_components(moon_age)
    return {
        "airTemp": air,
        "seaTemp": sea,
        "moonAge": moon_age,
        "moonSin": moon_sin,
        "moonCos": moon_cos,
    }


def quantile(values, q):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def clamp(value, lower, upper):
    return max(lower, min(upper, value))


def round_half(value):
    return round(value * 2) / 2.0


def round_feature_range(values, lower_pad=0.0, upper_pad=0.0):
    low = min(values) - lower_pad
    high = max(values) + upper_pad
    return round_half(low), round_half(high)


def compute_base_stats(rows):
    stats = {"means": {}, "scales": {}}
    for key in FEATURE_KEYS:
        values = [row[key] for row in rows]
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        stats["means"][key] = mean
        stats["scales"][key] = math.sqrt(variance) or 1.0
    return stats


def build_basis(raw_features, stats):
    air = (raw_features["airTemp"] - stats["means"]["airTemp"]) / stats["scales"]["airTemp"]
    sea = (raw_features["seaTemp"] - stats["means"]["seaTemp"]) / stats["scales"]["seaTemp"]
    moon_sin = (raw_features["moonSin"] - stats["means"]["moonSin"]) / stats["scales"]["moonSin"]
    moon_cos = (raw_features["moonCos"] - stats["means"]["moonCos"]) / stats["scales"]["moonCos"]
    return [
        1.0,
        air,
        sea,
        moon_sin,
        moon_cos,
        air * sea,
        air * moon_sin,
        air * moon_cos,
        sea * moon_sin,
        sea * moon_cos,
        air * air,
        sea * sea,
    ]


def solve_linear_system(matrix, vector):
    size = len(vector)
    augmented = [row[:] + [value] for row, value in zip(matrix, vector)]
    for col in range(size):
        pivot = max(range(col, size), key=lambda row: abs(augmented[row][col]))
        if abs(augmented[pivot][col]) < 1e-9:
            augmented[pivot][col] = 1e-9
        if pivot != col:
            augmented[col], augmented[pivot] = augmented[pivot], augmented[col]

        pivot_value = augmented[col][col]
        for index in range(col, size + 1):
            augmented[col][index] /= pivot_value

        for row in range(size):
            if row == col:
                continue
            factor = augmented[row][col]
            if factor == 0:
                continue
            for index in range(col, size + 1):
                augmented[row][index] -= factor * augmented[col][index]

    return [augmented[row][size] for row in range(size)]


def fit_ridge_regression(design_matrix, targets, ridge):
    feature_count = len(design_matrix[0])
    gram = [[0.0 for _ in range(feature_count)] for _ in range(feature_count)]
    rhs = [0.0 for _ in range(feature_count)]

    for features, target in zip(design_matrix, targets):
        for i in range(feature_count):
            rhs[i] += features[i] * target
            for j in range(feature_count):
                gram[i][j] += features[i] * features[j]

    for index in range(1, feature_count):
        gram[index][index] += ridge

    return solve_linear_system(gram, rhs)


def clip_probability(value):
    return round(clamp(value, 0.0, 0.995), 4)


def decode_measure(value, ceiling):
    return clamp(math.expm1(value), 0.0, ceiling)


def fit_models(rows):
    stats = compute_base_stats(rows)
    design_matrix = [build_basis(row, stats) for row in rows]
    min_targets = [math.log1p(row["catchMin"]) for row in rows]
    max_targets = [math.log1p(row["catchMax"]) for row in rows]
    count_ceiling = max(row["catchMax"] for row in rows) * 1.35 + 1.0

    return {
        "featureTerms": FEATURE_TERMS,
        "stats": {
            "means": {key: round(stats["means"][key], 6) for key in FEATURE_KEYS},
            "scales": {key: round(stats["scales"][key], 6) for key in FEATURE_KEYS},
        },
        "countCeiling": round(count_ceiling, 3),
        "models": {
            "catchMin": {
                "type": "log_measure",
                "weights": [round(value, 8) for value in fit_ridge_regression(design_matrix, min_targets, ridge=0.75)],
            },
            "catchMax": {
                "type": "log_measure",
                "weights": [round(value, 8) for value in fit_ridge_regression(design_matrix, max_targets, ridge=0.75)],
            },
        },
    }


def predict_models(raw_features, regression):
    basis = build_basis(raw_features, regression["stats"])
    min_score = sum(weight * feature for weight, feature in zip(regression["models"]["catchMin"]["weights"], basis))
    max_score = sum(weight * feature for weight, feature in zip(regression["models"]["catchMax"]["weights"], basis))

    predicted_min = decode_measure(min_score, regression["countCeiling"])
    predicted_max = max(predicted_min, decode_measure(max_score, regression["countCeiling"]))
    return {
        "predictedMin": round(predicted_min, 2),
        "predictedMax": round(predicted_max, 2),
    }


def estimate_max_sigma(rows, regression):
    residuals = [row["catchMax"] - predict_models(row, regression)["predictedMax"] for row in rows]
    squared = sum(value * value for value in residuals) / len(residuals)
    return round(max(math.sqrt(squared), 0.35), 4)


def build_xday_distribution(predictions, sigma, seed_key):
    if not predictions:
        return []

    rng = random.Random(seed_key)
    win_counts = [0 for _ in predictions]
    maxima_samples = []

    for sample_index in range(XDAY_MONTE_CARLO_SAMPLES):
        best_index = 0
        best_value = None
        for index, point in enumerate(predictions):
            sampled = rng.gauss(point["predictedMax"], sigma)
            if best_value is None or sampled > best_value:
                best_value = sampled
                best_index = index

        win_counts[best_index] += 1
        sampled_max = round(max(best_value or 0.0, 0.0), 3)
        if sample_index < SIMULATOR_MAXIMA_SAMPLES:
            maxima_samples.append(sampled_max)
        else:
            replace_index = rng.randint(0, sample_index)
            if replace_index < SIMULATOR_MAXIMA_SAMPLES:
                maxima_samples[replace_index] = sampled_max

    for point, wins in zip(predictions, win_counts):
        point["probability"] = clip_probability(wins / XDAY_MONTE_CARLO_SAMPLES)

    return sorted(maxima_samples)


def evaluate_split(rows, seed_key):
    if len(rows) < MIN_VALIDATION_ROWS:
        return None

    indices = list(range(len(rows)))
    random.Random(seed_key).shuffle(indices)
    validation_size = max(2, int(round(len(rows) * VALIDATION_RATIO)))
    validation_size = min(validation_size, len(rows) - 2)
    validation_indices = set(indices[:validation_size])
    train_rows = [row for index, row in enumerate(rows) if index not in validation_indices]
    validation_rows = [row for index, row in enumerate(rows) if index in validation_indices]
    if len(train_rows) < 2 or len(validation_rows) < 2:
        return None

    regression = fit_models(train_rows)
    min_errors = []
    max_errors = []
    for row in validation_rows:
        prediction = predict_models(row, regression)
        min_errors.append(abs(prediction["predictedMin"] - row["catchMin"]))
        max_errors.append(abs(prediction["predictedMax"] - row["catchMax"]))

    return {
        "validationRows": len(validation_rows),
        "minMae": round(sum(min_errors) / len(min_errors), 3),
        "maxMae": round(sum(max_errors) / len(max_errors), 3),
    }


def build_species_rows(daily_reports, species_name, unit, archive_map, forecast_map, climatology):
    rows = []
    for day_record in daily_reports:
        measurement = day_record["species"].get(species_name, {}).get(unit)
        if not measurement:
            continue
        features = resolve_training_feature(day_record, archive_map, forecast_map, climatology)
        rows.append(
            {
                "date": day_record["date"],
                "catchMin": measurement["min"],
                "catchMax": measurement["max"],
                "catchText": measurement["raw"],
                "airTemp": features["airTemp"],
                "seaTemp": features["seaTemp"],
                "moonAge": features["moonAge"],
                "moonSin": features["moonSin"],
                "moonCos": features["moonCos"],
            }
        )
    return rows


def species_key(ship_id, species_name):
    digest = hashlib.sha1(f"{ship_id}:{species_name}".encode("utf-8")).hexdigest()[:12]
    return digest


def build_ship_payloads(ship_meta, daily_reports, today, archive_map, forecast_map, climatology):
    species_summaries = {}
    for day_record in daily_reports:
        for species_name, units in day_record["species"].items():
            summary = species_summaries.setdefault(species_name, {"units": Counter(), "positiveDays": Counter()})
            for unit, measurement in units.items():
                summary["units"][unit] += 1
                if measurement["max"] > 0:
                    summary["positiveDays"][unit] += 1

    ship_catalog_entry = {
        "id": ship_meta["id"],
        "name": ship_meta["name"],
        "location": daily_reports[-1]["location"] if daily_reports and daily_reports[-1]["location"] else ship_meta["location"],
        "homeUrl": ship_meta["homeUrl"],
        "catchUrl": ship_meta["catchUrl"],
        "species": [],
    }

    future_start = today + timedelta(days=1)
    future_end = today + timedelta(days=FORECAST_DAYS)
    payloads = []
    for species_name, summary in sorted(
        species_summaries.items(),
        key=lambda item: (-max(item[1]["positiveDays"].values() or [0]), item[0]),
    ):
        unit, positive_days = summary["positiveDays"].most_common(1)[0] if summary["positiveDays"] else ("", 0)
        if positive_days < MIN_POSITIVE_DAYS:
            continue

        rows = build_species_rows(daily_reports, species_name, unit, archive_map, forecast_map, climatology)
        positive_rows = [row for row in rows if row["catchMax"] > 0]
        if len(positive_rows) < MIN_POSITIVE_DAYS:
            continue

        evaluation = evaluate_split(rows, f'{ship_meta["id"]}:{species_name}')
        regression = fit_models(rows)
        max_sigma = estimate_max_sigma(rows, regression)
        observed_by_date = {row["date"]: row for row in rows}

        future_predictions = []
        current_day = future_start
        while current_day <= future_end:
            resolved, source = resolve_prediction_feature(current_day, archive_map, forecast_map, climatology)
            moon_age = moon_age_for(current_day)
            moon_sin, moon_cos = moon_phase_components(moon_age)
            raw_features = {
                "airTemp": resolved["temperature_2m_mean"],
                "seaTemp": resolved["sea_surface_temperature_mean"],
                "moonAge": moon_age,
                "moonSin": moon_sin,
                "moonCos": moon_cos,
            }
            prediction = predict_models(raw_features, regression)
            prior_year_day = same_day_last_year(current_day)
            observed_row = observed_by_date.get(prior_year_day)
            future_predictions.append(
                {
                    "date": current_day.isoformat(),
                    "predictedMin": prediction["predictedMin"],
                    "predictedMax": prediction["predictedMax"],
                    "airTemp": round(raw_features["airTemp"], 2),
                    "seaTemp": round(raw_features["seaTemp"], 2),
                    "moonAge": round(raw_features["moonAge"], 2),
                    "featureSource": source,
                    "observedDate": prior_year_day.isoformat() if observed_row else None,
                    "observedMin": round(observed_row["catchMin"], 2) if observed_row else None,
                    "observedMax": round(observed_row["catchMax"], 2) if observed_row else None,
                    "observedText": observed_row["catchText"] if observed_row else None,
                }
            )
            current_day += timedelta(days=1)

        maxima_samples = build_xday_distribution(
            future_predictions,
            max_sigma,
            f'{ship_meta["id"]}:{species_name}:{today.isoformat()}',
        )
        peak_day = min(future_predictions, key=lambda item: (-item["predictedMax"], item["date"]))
        top_days = sorted(future_predictions, key=lambda item: (-item["predictedMax"], item["date"]))[:4]
        default_point = top_days[0] if top_days else future_predictions[0]

        all_air = [row["airTemp"] for row in rows] + [item["airTemp"] for item in future_predictions]
        all_sea = [row["seaTemp"] for row in rows] + [item["seaTemp"] for item in future_predictions]
        feature_ranges = {
            "airTemp": {
                "min": round_feature_range(all_air, 1.0, 1.0)[0],
                "max": round_feature_range(all_air, 1.0, 1.0)[1],
                "step": 0.1,
                "default": round(default_point["airTemp"], 1),
            },
            "seaTemp": {
                "min": round_feature_range(all_sea, 0.5, 0.5)[0],
                "max": round_feature_range(all_sea, 0.5, 0.5)[1],
                "step": 0.1,
                "default": round(default_point["seaTemp"], 1),
            },
            "moonAge": {
                "min": 0,
                "max": round(SYNODIC_MONTH, 1),
                "step": 0.1,
                "default": round(default_point["moonAge"], 1),
            },
        }

        species_id = species_key(ship_meta["id"], species_name)
        file_name = f'{ship_meta["id"]}-{species_id}.json'
        payload = {
            "generatedAt": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
            "today": today.isoformat(),
            "ship": {
                "id": ship_meta["id"],
                "name": ship_meta["name"],
                "location": ship_catalog_entry["location"],
                "homeUrl": ship_meta["homeUrl"],
                "catchUrl": ship_meta["catchUrl"],
            },
            "species": {
                "id": species_id,
                "label": species_name,
                "unit": unit,
            },
            "trainingRange": {
                "from": rows[0]["date"].isoformat(),
                "to": rows[-1]["date"].isoformat(),
            },
            "forecastRange": {
                "from": future_start.isoformat(),
                "to": future_end.isoformat(),
            },
            "tripDays": len(rows),
            "positiveDays": len(positive_rows),
            "xDayRule": "予測上限が最も高い日",
            "xDayPeak": {
                "date": peak_day["date"],
                "predictedMax": peak_day["predictedMax"],
                "probability": peak_day["probability"],
                "unit": unit,
            },
            "xDayModel": {
                "method": "monte_carlo_peak",
                "samples": XDAY_MONTE_CARLO_SAMPLES,
                "maxSigma": max_sigma,
                "maximaSamples": maxima_samples,
            },
            "featureRanges": feature_ranges,
            "evaluation": evaluation,
            "regression": regression,
            "topDays": top_days,
            "predictions": future_predictions,
        }
        payloads.append((file_name, payload))
        ship_catalog_entry["species"].append(
            {
                "id": species_id,
                "label": species_name,
                "unit": unit,
                "positiveDays": len(positive_rows),
                "tripDays": len(rows),
                "file": f"data/payloads/{file_name}",
            }
        )

    ship_catalog_entry["species"].sort(key=lambda item: (-item["positiveDays"], item["label"]))
    return ship_catalog_entry, payloads


def write_outputs(catalog, payloads):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)

    for old_file in PAYLOAD_DIR.glob("*.json"):
        old_file.unlink()

    predictions_path = DATA_DIR / "predictions.json"
    if predictions_path.exists():
        predictions_path.unlink()

    for file_name, payload in payloads:
        (PAYLOAD_DIR / file_name).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    CATALOG_PATH.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    args = parse_args()
    today = date.fromisoformat(args.today) if args.today else date.today()
    ship_ids = args.ships or DEFAULT_SHIP_IDS

    training_start = today - timedelta(days=TRAINING_DAYS - 1)
    weather_history_start = today - timedelta(days=WEATHER_HISTORY_DAYS - 1)
    archive_end = today - timedelta(days=1)
    forecast_end = today + timedelta(days=min(FORECAST_API_DAYS, FORECAST_DAYS))

    all_catalog_ships = []
    all_payloads = []

    for ship_id in ship_ids:
        ship_meta = parse_ship_meta(ship_id)
        daily_reports = collect_ship_reports(ship_meta, training_start, today)
        if len(daily_reports) < MIN_VALIDATION_ROWS:
            raise RuntimeError(f"Not enough daily reports were collected for ship {ship_id}.")

        air_archive = fetch_open_meteo_daily(
            OPEN_METEO_ARCHIVE,
            ship_meta["latitude"],
            ship_meta["longitude"],
            weather_history_start,
            archive_end,
            ["temperature_2m_mean"],
        )
        sea_archive = fetch_open_meteo_daily(
            OPEN_METEO_MARINE,
            ship_meta["latitude"],
            ship_meta["longitude"],
            weather_history_start,
            archive_end,
            ["sea_surface_temperature_mean"],
        )
        archive_map = combine_feature_sources(air_archive, sea_archive)

        air_forecast = fetch_open_meteo_daily(
            OPEN_METEO_FORECAST,
            ship_meta["latitude"],
            ship_meta["longitude"],
            today,
            forecast_end,
            ["temperature_2m_mean"],
        )
        sea_forecast = fetch_open_meteo_daily(
            OPEN_METEO_MARINE,
            ship_meta["latitude"],
            ship_meta["longitude"],
            today,
            forecast_end,
            ["sea_surface_temperature_mean"],
        )
        forecast_map = combine_feature_sources(air_forecast, sea_forecast)
        climatology = build_climatology(archive_map)

        ship_catalog_entry, ship_payloads = build_ship_payloads(
            ship_meta,
            daily_reports,
            today,
            archive_map,
            forecast_map,
            climatology,
        )
        if not ship_catalog_entry["species"]:
            raise RuntimeError(f"No qualifying species were generated for ship {ship_id}.")

        all_catalog_ships.append(ship_catalog_entry)
        all_payloads.extend(ship_payloads)

    catalog = {
        "generatedAt": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
        "today": today.isoformat(),
        "ships": sorted(all_catalog_ships, key=lambda item: item["name"]),
    }
    write_outputs(catalog, all_payloads)


if __name__ == "__main__":
    main()
