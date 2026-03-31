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

DEFAULT_SHIP_IDS = [
    "00296",
    "00297",
    "ichiroumaru",
    "00007",
    "00834",
    "00307",
    "00300",
    "00150",
    "00836",
    "00703",
    "00689",
    "01580",
]
TRAINING_DAYS = 365
FORECAST_DAYS = 365
WEATHER_HISTORY_DAYS = 365 * 3
FORECAST_API_DAYS = 14
VALIDATION_RATIO = 0.25
MIN_VALIDATION_ROWS = 8
MIN_POSITIVE_DAYS = 4
XDAY_MONTE_CARLO_SAMPLES = 4096
SIMULATOR_MAXIMA_SAMPLES = 320
NEURAL_GRAD_CLIP = 3.5
NEURAL_OUTPUT_WEIGHTS = (0.4, 0.6)
NEIGHBOR_COUNTS = (4, 6, 8, 12)
KERNEL_BANDWIDTHS = (0.7, 1.0, 1.4, 1.8)
KERNEL_PRIOR_WEIGHT = 1.0
RANDOM_FOREST_TREE_COUNT = 24
RANDOM_FOREST_MAX_DEPTH = 6
RANDOM_FOREST_MIN_LEAF = 3
RANDOM_FOREST_MAX_FEATURES = 4
RANDOM_FOREST_THRESHOLD_STEPS = 8
MIN_RANDOM_FOREST_ROWS = 18
GLOBAL_BALANCE_FIELD = "contextKey"

NEURAL_FEATURE_SETS = {
    "compact": {
        "featureKeys": ("airTemp", "seaTemp", "moonAge", "moonSin", "moonCos", "airSeaGap", "airSeaMean"),
    },
    "extended": {
        "featureKeys": (
            "airTemp",
            "seaTemp",
            "moonAge",
            "moonSin",
            "moonCos",
            "moonSin2",
            "moonCos2",
            "airSeaGap",
            "airSeaMean",
            "airSeaAbsGap",
            "moonFullness",
        ),
    },
    "rich": {
        "featureKeys": (
            "airTemp",
            "seaTemp",
            "moonAge",
            "moonSin",
            "moonCos",
            "moonSin2",
            "moonCos2",
            "moonSin3",
            "moonCos3",
            "airSeaGap",
            "airSeaMean",
            "airSeaAbsGap",
            "moonFullness",
        ),
    },
}
GLOBAL_CONTEXT_PROFILE_KEYS = (
    "contextAvgMin",
    "contextAvgMax",
    "contextPositiveRate",
    "contextAvgSpread",
    "contextTripScale",
    "speciesAvgMax",
    "speciesPositiveRate",
    "speciesTripScale",
)

FEATURE_SPECS = {
    "harmonic1": {
        "label": "月齢 1次 sin/cos",
        "featureKeys": ("airTemp", "seaTemp", "moonSin", "moonCos"),
        "basisTerms": (
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
        ),
    },
    "harmonic2": {
        "label": "月齢 1次+2次 sin/cos",
        "featureKeys": ("airTemp", "seaTemp", "moonSin", "moonCos", "moonSin2", "moonCos2"),
        "basisTerms": (
            "intercept",
            "airTemp",
            "seaTemp",
            "moonSin",
            "moonCos",
            "moonSin2",
            "moonCos2",
            "airTemp*seaTemp",
            "airTemp*moonSin",
            "airTemp*moonCos",
            "airTemp*moonSin2",
            "airTemp*moonCos2",
            "seaTemp*moonSin",
            "seaTemp*moonCos",
            "seaTemp*moonSin2",
            "seaTemp*moonCos2",
            "airTemp^2",
            "seaTemp^2",
        ),
    },
}
DEFAULT_FEATURE_SPEC = "harmonic1"
RANDOM_FOREST_FEATURE_SPEC = "harmonic2"

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


def moon_phase_components(moon_age, harmonic=1):
    angle = (moon_age / SYNODIC_MONTH) * math.tau * harmonic
    return math.sin(angle), math.cos(angle)


def build_feature_map(air_temp, sea_temp, moon_age):
    angle = (moon_age / SYNODIC_MONTH) * math.tau
    air_sea_gap = air_temp - sea_temp
    return {
        "airTemp": air_temp,
        "seaTemp": sea_temp,
        "moonAge": moon_age,
        "moonSin": math.sin(angle),
        "moonCos": math.cos(angle),
        "moonSin2": math.sin(angle * 2),
        "moonCos2": math.cos(angle * 2),
        "moonSin3": math.sin(angle * 3),
        "moonCos3": math.cos(angle * 3),
        "airSeaGap": air_sea_gap,
        "airSeaMean": (air_temp + sea_temp) * 0.5,
        "airSeaAbsGap": abs(air_sea_gap),
        "moonFullness": 0.5 * (1 - math.cos(angle)),
    }


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
    return build_feature_map(air, sea, moon_age)


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


def feature_spec(spec_key):
    return FEATURE_SPECS[spec_key]


def compute_base_stats(rows, feature_keys):
    stats = {"means": {}, "scales": {}}
    for key in feature_keys:
        values = [row.get(key, 0.0) for row in rows]
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / len(values)
        stats["means"][key] = mean
        stats["scales"][key] = math.sqrt(variance) or 1.0
    return stats


def scale_features(raw_features, stats, feature_keys):
    feature_map = build_feature_map(raw_features["airTemp"], raw_features["seaTemp"], raw_features["moonAge"])
    return {
        key: (feature_map[key] - stats["means"][key]) / stats["scales"][key]
        for key in feature_keys
    }


def evaluate_basis_term(term, scaled):
    if term == "intercept":
        return 1.0
    if term.endswith("^2"):
        feature = term[:-2]
        return scaled[feature] * scaled[feature]
    if "*" in term:
        left, right = term.split("*", 1)
        return scaled[left] * scaled[right]
    return scaled[term]


def build_basis_from_scaled(scaled, basis_terms):
    return [evaluate_basis_term(term, scaled) for term in basis_terms]


def build_basis(raw_features, stats, feature_keys, basis_terms):
    return build_basis_from_scaled(scale_features(raw_features, stats, feature_keys), basis_terms)


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


def dot(weights, values):
    return sum(weight * value for weight, value in zip(weights, values))


def weighted_average(pairs):
    total_weight = sum(weight for weight, _ in pairs)
    if total_weight <= 0:
        return 0.0
    return sum(weight * value for weight, value in pairs) / total_weight


def build_model_feature_row(raw_features, context_features=None):
    feature_row = build_feature_map(raw_features["airTemp"], raw_features["seaTemp"], raw_features["moonAge"])
    if context_features:
        feature_row.update(context_features)
    return feature_row


def compute_balanced_row_weights(rows, key_field=GLOBAL_BALANCE_FIELD):
    if not rows:
        return []
    counts = Counter(row[key_field] for row in rows)
    if not counts:
        return [1.0 for _ in rows]
    target_total = len(rows) / len(counts)
    return [target_total / max(counts[row[key_field]], 1) for row in rows]


def build_support_rows(rows, stats, min_weights, max_weights, spec_key):
    spec = feature_spec(spec_key)
    support = []
    for row in rows:
        scaled = scale_features(row, stats, spec["featureKeys"])
        basis = build_basis_from_scaled(scaled, spec["basisTerms"])
        min_baseline = dot(min_weights, basis)
        max_baseline = dot(max_weights, basis)
        support.append(
            {
                "vector": [scaled[key] for key in spec["featureKeys"]],
                "minResidual": math.log1p(row["catchMin"]) - min_baseline,
                "maxResidual": math.log1p(row["catchMax"]) - max_baseline,
            }
        )
    return support


def estimate_neighbor_residuals(scaled, feature_keys, support_rows, neighbor_count, bandwidth, prior_weight=KERNEL_PRIOR_WEIGHT):
    if not support_rows:
        return 0.0, 0.0

    vector = [scaled[key] for key in feature_keys]
    ranked = []
    for item in support_rows:
        distance_sq = sum((left - right) ** 2 for left, right in zip(vector, item["vector"]))
        weight = math.exp(-distance_sq / max(2 * bandwidth * bandwidth, 1e-9))
        ranked.append((distance_sq, weight, item))

    ranked.sort(key=lambda entry: entry[0])
    trimmed = ranked[: min(neighbor_count, len(ranked))]
    min_pairs = [(weight, item["minResidual"]) for _, weight, item in trimmed]
    max_pairs = [(weight, item["maxResidual"]) for _, weight, item in trimmed]
    total_weight = sum(weight for weight, _ in min_pairs)
    shrink = total_weight / (total_weight + prior_weight) if total_weight > 0 else 0.0
    return weighted_average(min_pairs) * shrink, weighted_average(max_pairs) * shrink


def fit_baseline_models(rows, spec_key=DEFAULT_FEATURE_SPEC):
    spec = feature_spec(spec_key)
    stats = compute_base_stats(rows, spec["featureKeys"])
    design_matrix = [build_basis(row, stats, spec["featureKeys"], spec["basisTerms"]) for row in rows]
    min_targets = [math.log1p(row["catchMin"]) for row in rows]
    max_targets = [math.log1p(row["catchMax"]) for row in rows]
    count_ceiling = max(row["catchMax"] for row in rows) * 1.35 + 1.0
    min_weights = fit_ridge_regression(design_matrix, min_targets, ridge=0.75)
    max_weights = fit_ridge_regression(design_matrix, max_targets, ridge=0.75)

    return {
        "type": "ridge_regression",
        "modelLabel": "重回帰",
        "featureSpec": {
            "key": spec_key,
            "label": spec["label"],
            "featureKeys": list(spec["featureKeys"]),
            "basisTerms": list(spec["basisTerms"]),
        },
        "stats": {
            "means": {key: round(stats["means"][key], 6) for key in spec["featureKeys"]},
            "scales": {key: round(stats["scales"][key], 6) for key in spec["featureKeys"]},
        },
        "countCeiling": round(count_ceiling, 3),
        "baseline": {
            "catchMin": {
                "type": "log_measure",
                "weights": [round(value, 8) for value in min_weights],
            },
            "catchMax": {
                "type": "log_measure",
                "weights": [round(value, 8) for value in max_weights],
            },
        },
        "_fitStats": stats,
        "_minWeights": min_weights,
        "_maxWeights": max_weights,
    }


def build_hybrid_model(rows, neighbor_count, bandwidth, spec_key=DEFAULT_FEATURE_SPEC):
    baseline = fit_baseline_models(rows, spec_key=spec_key)
    stats = baseline["_fitStats"]
    min_weights = baseline["_minWeights"]
    max_weights = baseline["_maxWeights"]
    support = build_support_rows(rows, stats, min_weights, max_weights, spec_key)
    feature_keys = baseline["featureSpec"]["featureKeys"]
    return {
        "type": "hybrid_kernel_residual",
        "modelLabel": "近傍補正つき重回帰",
        "featureSpec": baseline["featureSpec"],
        "stats": baseline["stats"],
        "countCeiling": baseline["countCeiling"],
        "baseline": baseline["baseline"],
        "neighbor": {
            "type": "gaussian_knn_residual",
            "featureKeys": feature_keys,
            "neighborCount": min(neighbor_count, len(rows)),
            "bandwidth": round(bandwidth, 4),
            "priorWeight": KERNEL_PRIOR_WEIGHT,
            "support": [
                {
                    "vector": [round(value, 6) for value in item["vector"]],
                    "minResidual": round(item["minResidual"], 8),
                    "maxResidual": round(item["maxResidual"], 8),
                }
                for item in support
            ],
        },
    }


def candidate_thresholds(values, threshold_steps=RANDOM_FOREST_THRESHOLD_STEPS):
    unique_values = sorted(set(values))
    if len(unique_values) <= 1:
        return []
    if len(unique_values) <= threshold_steps:
        return [(left + right) * 0.5 for left, right in zip(unique_values, unique_values[1:])]

    thresholds = []
    for step in range(1, threshold_steps):
        ratio = step / threshold_steps
        index = min(max(1, int(round((len(unique_values) - 1) * ratio))), len(unique_values) - 1)
        left = unique_values[index - 1]
        right = unique_values[index]
        threshold = (left + right) * 0.5
        if not thresholds or abs(thresholds[-1] - threshold) > 1e-9:
            thresholds.append(threshold)
    return thresholds


def mean_squared_error(values, weights=None):
    if not values:
        return 0.0
    if not weights:
        mean = sum(values) / len(values)
        return sum((value - mean) ** 2 for value in values)
    total_weight = sum(weights)
    if total_weight <= 0:
        return 0.0
    mean = sum(weight * value for value, weight in zip(values, weights)) / total_weight
    return sum(weight * (value - mean) ** 2 for value, weight in zip(values, weights))


def build_tree_node(samples, feature_keys, rng, depth, max_depth, min_leaf, max_features, threshold_steps):
    targets = [sample["target"] for sample in samples]
    weights = [sample["weight"] for sample in samples]
    total_weight = sum(weights)
    leaf_value = sum(sample["weight"] * sample["target"] for sample in samples) / total_weight if total_weight > 0 else 0.0
    if (
        depth >= max_depth
        or len(samples) <= min_leaf * 2
        or mean_squared_error(targets, weights) <= 1e-8
    ):
        return {"v": round(leaf_value, 8)}

    candidate_features = list(feature_keys)
    rng.shuffle(candidate_features)
    candidate_features = candidate_features[: min(max_features, len(candidate_features))]

    best_split = None
    for feature_index, feature in enumerate(feature_keys):
        if feature not in candidate_features:
            continue
        values = [sample["features"][feature_index] for sample in samples]
        for threshold in candidate_thresholds(values, threshold_steps=threshold_steps):
            left = [sample for sample in samples if sample["features"][feature_index] <= threshold]
            right = [sample for sample in samples if sample["features"][feature_index] > threshold]
            if len(left) < min_leaf or len(right) < min_leaf:
                continue
            score = mean_squared_error(
                [sample["target"] for sample in left],
                [sample["weight"] for sample in left],
            ) + mean_squared_error(
                [sample["target"] for sample in right],
                [sample["weight"] for sample in right],
            )
            if best_split is None or score < best_split["score"]:
                best_split = {
                    "featureIndex": feature_index,
                    "threshold": threshold,
                    "left": left,
                    "right": right,
                    "score": score,
                }

    if not best_split:
        return {"v": round(leaf_value, 8)}

    return {
        "f": best_split["featureIndex"],
        "t": round(best_split["threshold"], 6),
        "l": build_tree_node(
            best_split["left"],
            feature_keys,
            rng,
            depth + 1,
            max_depth,
            min_leaf,
            max_features,
            threshold_steps,
        ),
        "r": build_tree_node(
            best_split["right"],
            feature_keys,
            rng,
            depth + 1,
            max_depth,
            min_leaf,
            max_features,
            threshold_steps,
        ),
    }


def bootstrap_samples(feature_rows, targets, sample_weights, rng):
    if not sample_weights:
        sample_weights = [1.0 for _ in feature_rows]
    cumulative = []
    running_total = 0.0
    for weight in sample_weights:
        running_total += max(weight, 0.0)
        cumulative.append(running_total)
    if running_total <= 0:
        cumulative = [index + 1 for index in range(len(feature_rows))]
        running_total = len(feature_rows)

    samples = []
    for _ in range(len(feature_rows)):
        pick = rng.random() * running_total
        index = 0
        while index < len(cumulative) - 1 and cumulative[index] < pick:
            index += 1
        samples.append({"features": feature_rows[index], "target": targets[index], "weight": max(sample_weights[index], 1e-9)})
    return samples


def build_random_forest_tree(feature_rows, targets, feature_keys, config, seed_key, tree_index, sample_weights=None):
    rng = random.Random(f"{seed_key}:{tree_index}")
    samples = bootstrap_samples(feature_rows, targets, sample_weights, rng)
    return build_tree_node(
        samples,
        feature_keys,
        rng,
        depth=0,
        max_depth=config.get("max_depth", RANDOM_FOREST_MAX_DEPTH),
        min_leaf=config.get("min_leaf", RANDOM_FOREST_MIN_LEAF),
        max_features=config.get("max_features", RANDOM_FOREST_MAX_FEATURES),
        threshold_steps=config.get("threshold_steps", RANDOM_FOREST_THRESHOLD_STEPS),
    )


def train_random_forest(feature_rows, targets, feature_keys, config, seed_key, sample_weights=None):
    forest = []
    for tree_index in range(config.get("tree_count", RANDOM_FOREST_TREE_COUNT)):
        forest.append(build_random_forest_tree(feature_rows, targets, feature_keys, config, seed_key, tree_index, sample_weights))
    return forest


def predict_tree(tree, feature_row):
    node = tree
    while "v" not in node:
        node = node["l"] if feature_row[node["f"]] <= node["t"] else node["r"]
    return node["v"]


def mean_weighted_error(points, weights=None):
    if not points:
        return 0.0
    if not weights:
        return sum(weighted_error(point, point) for point in points) / len(points)
    total_weight = sum(weights)
    if total_weight <= 0:
        return 0.0
    return sum(weight * weighted_error(point, point) for point, weight in zip(points, weights)) / total_weight


def score_random_forest_validation(running_min_scores, running_max_scores, tree_count, validation_rows, count_ceiling, weights=None):
    points = []
    for index, row in enumerate(validation_rows):
        predicted_min = decode_measure(running_min_scores[index] / tree_count, count_ceiling)
        predicted_max = max(predicted_min, decode_measure(running_max_scores[index] / tree_count, count_ceiling))
        points.append(
            {
                "catchMin": row["catchMin"],
                "catchMax": row["catchMax"],
                "predictedMin": predicted_min,
                "predictedMax": predicted_max,
            }
        )
    return mean_weighted_error(points, weights=weights)


def fit_random_forest_model(rows, config, seed_key, sample_weights=None, validation_rows=None, validation_weights=None):
    feature_keys = tuple(config.get("featureKeys") or feature_spec(RANDOM_FOREST_FEATURE_SPEC)["featureKeys"])
    feature_rows = [[row.get(key, 0.0) for key in feature_keys] for row in rows]
    min_targets = [math.log1p(row["catchMin"]) for row in rows]
    max_targets = [math.log1p(row["catchMax"]) for row in rows]
    count_ceiling = max(row["catchMax"] for row in rows) * 1.35 + 1.0
    configured_tree_count = config.get("tree_count", RANDOM_FOREST_TREE_COUNT)
    selected_tree_count = configured_tree_count

    if validation_rows:
        validation_feature_rows = [[row.get(key, 0.0) for key in feature_keys] for row in validation_rows]
        min_forest = []
        max_forest = []
        running_min_scores = [0.0 for _ in validation_rows]
        running_max_scores = [0.0 for _ in validation_rows]
        best_score = None
        best_tree_count = 0
        patience = config.get("early_stopping_patience", 0)
        min_delta = config.get("early_stopping_min_delta", 0.0)

        for tree_index in range(configured_tree_count):
            min_tree = build_random_forest_tree(
                feature_rows,
                min_targets,
                feature_keys,
                config,
                f"{seed_key}:min",
                tree_index,
                sample_weights,
            )
            max_tree = build_random_forest_tree(
                feature_rows,
                max_targets,
                feature_keys,
                config,
                f"{seed_key}:max",
                tree_index,
                sample_weights,
            )
            min_forest.append(min_tree)
            max_forest.append(max_tree)
            for index, feature_row in enumerate(validation_feature_rows):
                running_min_scores[index] += predict_tree(min_tree, feature_row)
                running_max_scores[index] += predict_tree(max_tree, feature_row)

            score = score_random_forest_validation(
                running_min_scores,
                running_max_scores,
                tree_index + 1,
                validation_rows,
                count_ceiling,
                weights=validation_weights,
            )
            if best_score is None or score < best_score - min_delta:
                best_score = score
                best_tree_count = tree_index + 1
            if patience and best_tree_count and tree_index + 1 - best_tree_count >= patience:
                break

        selected_tree_count = max(best_tree_count, 1)
        min_forest = min_forest[:selected_tree_count]
        max_forest = max_forest[:selected_tree_count]
    else:
        train_config = {**config, "tree_count": config.get("selected_tree_count", configured_tree_count)}
        min_forest = train_random_forest(feature_rows, min_targets, feature_keys, train_config, f"{seed_key}:min", sample_weights)
        max_forest = train_random_forest(feature_rows, max_targets, feature_keys, train_config, f"{seed_key}:max", sample_weights)
        selected_tree_count = train_config.get("tree_count", configured_tree_count)

    return {
        "type": "random_forest",
        "modelLabel": "ランダムフォレスト",
        "featureSpec": {
            "key": config["id"],
            "label": config.get("label", "ランダムフォレスト"),
            "featureKeys": list(feature_keys),
            "basisTerms": [],
        },
        "countCeiling": round(count_ceiling, 3),
        "forest": {
            "targetType": "log_measure",
            "featureKeys": list(feature_keys),
            "treeCount": selected_tree_count,
            "catchMin": min_forest,
            "catchMax": max_forest,
        },
        "training": {
            "configuredTreeCount": configured_tree_count,
            "selectedTreeCount": selected_tree_count,
        },
    }


def split_rows_for_validation(rows, seed_key):
    if len(rows) < MIN_VALIDATION_ROWS:
        return rows, []

    indices = list(range(len(rows)))
    random.Random(seed_key).shuffle(indices)
    validation_size = max(2, int(round(len(rows) * VALIDATION_RATIO)))
    validation_size = min(validation_size, len(rows) - 2)
    validation_indices = set(indices[:validation_size])
    train_rows = [row for index, row in enumerate(rows) if index not in validation_indices]
    validation_rows = [row for index, row in enumerate(rows) if index in validation_indices]
    if len(train_rows) < 2 or len(validation_rows) < 2:
        return rows, []
    return train_rows, validation_rows


def neural_feature_keys(config):
    return tuple(config.get("featureKeys") or NEURAL_FEATURE_SETS[config["feature_set"]]["featureKeys"])


def neural_input_vector(row, stats, feature_keys):
    return [(row.get(key, 0.0) - stats["means"][key]) / stats["scales"][key] for key in feature_keys]


def neural_targets(row):
    return (
        math.log1p(row["catchMin"]),
        math.log1p(max(row["catchMax"] - row["catchMin"], 0.0)),
    )


def initialize_dense_layers(layer_sizes, rng):
    layers = []
    for input_size, output_size in zip(layer_sizes, layer_sizes[1:]):
        limit = math.sqrt(6.0 / max(input_size + output_size, 1))
        layers.append(
            {
                "weights": [
                    [rng.uniform(-limit, limit) for _ in range(input_size)]
                    for _ in range(output_size)
                ],
                "biases": [0.0 for _ in range(output_size)],
            }
        )
    return layers


def zeros_like_layers(layers):
    return [
        {
            "weights": [[0.0 for _ in row] for row in layer["weights"]],
            "biases": [0.0 for _ in layer["biases"]],
        }
        for layer in layers
    ]


def forward_dense_layers(input_vector, layers):
    activations = [input_vector]
    for layer_index, layer in enumerate(layers):
        previous = activations[-1]
        outputs = []
        for weights, bias in zip(layer["weights"], layer["biases"]):
            total = bias + sum(weight * value for weight, value in zip(weights, previous))
            if layer_index < len(layers) - 1:
                total = math.tanh(total)
            outputs.append(total)
        activations.append(outputs)
    return activations


def predict_dense_layers(input_vector, layers):
    values = input_vector
    for layer_index, layer in enumerate(layers):
        outputs = []
        for weights, bias in zip(layer["weights"], layer["biases"]):
            total = bias + sum(weight * value for weight, value in zip(weights, values))
            if layer_index < len(layers) - 1:
                total = math.tanh(total)
            outputs.append(total)
        values = outputs
    return values


def clip_gradient(value):
    return clamp(value, -NEURAL_GRAD_CLIP, NEURAL_GRAD_CLIP)


def train_neural_layers(input_rows, target_rows, config, seed_key, sample_weights=None):
    layer_sizes = [len(input_rows[0]), *config["hidden_sizes"], 2]
    rng = random.Random(seed_key)
    layers = initialize_dense_layers(layer_sizes, rng)
    grad_layers = zeros_like_layers(layers)
    momentum = zeros_like_layers(layers)
    velocity = zeros_like_layers(layers)

    beta1 = 0.9
    beta2 = 0.999
    epsilon = 1e-8
    row_weights = list(sample_weights) if sample_weights else [1.0 for _ in input_rows]
    total_weight = sum(row_weights) or 1.0
    learning_rate = config["learning_rate"]
    weight_decay = config["weight_decay"]

    for step in range(1, config["epochs"] + 1):
        for grad_layer in grad_layers:
            for weight_row in grad_layer["weights"]:
                for index in range(len(weight_row)):
                    weight_row[index] = 0.0
            for index in range(len(grad_layer["biases"])):
                grad_layer["biases"][index] = 0.0

        for inputs, targets, row_weight in zip(input_rows, target_rows, row_weights):
            activations = forward_dense_layers(inputs, layers)
            outputs = activations[-1]
            delta = [
                (outputs[index] - targets[index]) * NEURAL_OUTPUT_WEIGHTS[index] * row_weight / total_weight
                for index in range(len(outputs))
            ]

            for layer_index in range(len(layers) - 1, -1, -1):
                previous = activations[layer_index]
                current = layers[layer_index]
                current_grads = grad_layers[layer_index]

                for output_index, delta_value in enumerate(delta):
                    current_grads["biases"][output_index] += delta_value
                    for input_index, previous_value in enumerate(previous):
                        current_grads["weights"][output_index][input_index] += delta_value * previous_value

                if layer_index == 0:
                    continue

                propagated = []
                for previous_index, previous_value in enumerate(previous):
                    backward = sum(
                        current["weights"][output_index][previous_index] * delta[output_index]
                        for output_index in range(len(delta))
                    )
                    propagated.append(backward * (1.0 - previous_value * previous_value))
                delta = propagated

        for layer_index, layer in enumerate(layers):
            grad_layer = grad_layers[layer_index]
            momentum_layer = momentum[layer_index]
            velocity_layer = velocity[layer_index]
            for output_index, weight_row in enumerate(layer["weights"]):
                for input_index, weight in enumerate(weight_row):
                    gradient = clip_gradient(grad_layer["weights"][output_index][input_index] + weight_decay * weight)
                    momentum_layer["weights"][output_index][input_index] = (
                        beta1 * momentum_layer["weights"][output_index][input_index] + (1 - beta1) * gradient
                    )
                    velocity_layer["weights"][output_index][input_index] = (
                        beta2 * velocity_layer["weights"][output_index][input_index] + (1 - beta2) * gradient * gradient
                    )
                    momentum_hat = momentum_layer["weights"][output_index][input_index] / (1 - beta1**step)
                    velocity_hat = velocity_layer["weights"][output_index][input_index] / (1 - beta2**step)
                    weight_row[input_index] -= learning_rate * momentum_hat / (math.sqrt(velocity_hat) + epsilon)

                bias_gradient = clip_gradient(grad_layer["biases"][output_index])
                momentum_layer["biases"][output_index] = beta1 * momentum_layer["biases"][output_index] + (
                    1 - beta1
                ) * bias_gradient
                velocity_layer["biases"][output_index] = beta2 * velocity_layer["biases"][output_index] + (
                    1 - beta2
                ) * bias_gradient * bias_gradient
                momentum_hat = momentum_layer["biases"][output_index] / (1 - beta1**step)
                velocity_hat = velocity_layer["biases"][output_index] / (1 - beta2**step)
                layer["biases"][output_index] -= learning_rate * momentum_hat / (math.sqrt(velocity_hat) + epsilon)

    return layers


def fit_neural_model(rows, config, seed_key, sample_weights=None):
    feature_keys = neural_feature_keys(config)
    stats = compute_base_stats(rows, feature_keys)
    inputs = [neural_input_vector(row, stats, feature_keys) for row in rows]
    targets = [neural_targets(row) for row in rows]
    layers = train_neural_layers(inputs, targets, config, seed_key, sample_weights=sample_weights)
    count_ceiling = max(row["catchMax"] for row in rows) * 1.35 + 1.0

    return {
        "type": "neural_network",
        "countCeiling": round(count_ceiling, 3),
        "input": {
            "featureKeys": list(feature_keys),
            "means": {key: round(stats["means"][key], 6) for key in feature_keys},
            "scales": {key: round(stats["scales"][key], 6) for key in feature_keys},
        },
        "network": {
            "activation": "tanh",
            "hiddenSizes": list(config["hidden_sizes"]),
            "layers": [
                {
                    "weights": [[round(value, 8) for value in row] for row in layer["weights"]],
                    "biases": [round(value, 8) for value in layer["biases"]],
                }
                for layer in layers
            ],
        },
    }


def predict_shared_model_row(feature_row, model):
    if model["type"] == "neural_network":
        feature_keys = model["input"]["featureKeys"]
        input_vector = [
            (feature_row.get(key, 0.0) - model["input"]["means"][key]) / model["input"]["scales"][key]
            for key in feature_keys
        ]
        min_score, gap_score = predict_dense_layers(input_vector, model["network"]["layers"])
        predicted_min = decode_measure(min_score, model["countCeiling"])
        predicted_gap = decode_measure(gap_score, model["countCeiling"])
        predicted_max = clamp(predicted_min + predicted_gap, predicted_min, model["countCeiling"])
    elif model["type"] == "random_forest":
        feature_keys = model["forest"]["featureKeys"]
        feature_row_values = [feature_row.get(key, 0.0) for key in feature_keys]
        min_score = sum(predict_tree(tree, feature_row_values) for tree in model["forest"]["catchMin"]) / max(
            len(model["forest"]["catchMin"]),
            1,
        )
        max_score = sum(predict_tree(tree, feature_row_values) for tree in model["forest"]["catchMax"]) / max(
            len(model["forest"]["catchMax"]),
            1,
        )
        predicted_min = decode_measure(min_score, model["countCeiling"])
        predicted_max = max(predicted_min, decode_measure(max_score, model["countCeiling"]))
    else:
        raise RuntimeError(f'Unsupported shared model type: {model["type"]}')

    return {
        "predictedMin": round(predicted_min, 2),
        "predictedMax": round(predicted_max, 2),
    }


def predict_shared_model(raw_features, model, context_features=None):
    return predict_shared_model_row(build_model_feature_row(raw_features, context_features), model)


def fit_models(rows, mode="baseline", feature_spec_key=DEFAULT_FEATURE_SPEC, neighbor_count=None, bandwidth=None, seed_key="fit"):
    if mode == "hybrid" and neighbor_count and bandwidth:
        return build_hybrid_model(rows, neighbor_count, bandwidth, spec_key=feature_spec_key)
    if mode == "random_forest":
        forest_spec = feature_spec(feature_spec_key)
        return fit_random_forest_model(
            rows,
            {
                "id": f"forest:{feature_spec_key}",
                "label": forest_spec["label"],
                "featureKeys": forest_spec["featureKeys"],
            },
            seed_key=seed_key,
        )

    baseline = fit_baseline_models(rows, spec_key=feature_spec_key)
    return {
        "type": "ridge_regression",
        "modelLabel": baseline["modelLabel"],
        "featureSpec": baseline["featureSpec"],
        "stats": baseline["stats"],
        "countCeiling": baseline["countCeiling"],
        "baseline": baseline["baseline"],
        "neighbor": None,
    }


def predict_models(raw_features, regression):
    if regression["type"] == "random_forest":
        feature_keys = regression["forest"]["featureKeys"]
        feature_map = build_feature_map(raw_features["airTemp"], raw_features["seaTemp"], raw_features["moonAge"])
        feature_row = [feature_map[key] for key in feature_keys]
        min_score = sum(predict_tree(tree, feature_row) for tree in regression["forest"]["catchMin"]) / len(
            regression["forest"]["catchMin"]
        )
        max_score = sum(predict_tree(tree, feature_row) for tree in regression["forest"]["catchMax"]) / len(
            regression["forest"]["catchMax"]
        )
    else:
        feature_keys = regression["featureSpec"]["featureKeys"]
        basis_terms = regression["featureSpec"]["basisTerms"]
        scaled = scale_features(raw_features, regression["stats"], feature_keys)
        basis = build_basis_from_scaled(scaled, basis_terms)
        min_score = dot(regression["baseline"]["catchMin"]["weights"], basis)
        max_score = dot(regression["baseline"]["catchMax"]["weights"], basis)
        neighbor = regression.get("neighbor")
        if neighbor and neighbor.get("support"):
            min_residual, max_residual = estimate_neighbor_residuals(
                scaled,
                feature_keys,
                neighbor["support"],
                neighbor["neighborCount"],
                neighbor["bandwidth"],
                neighbor.get("priorWeight", KERNEL_PRIOR_WEIGHT),
            )
            min_score += min_residual
            max_score += max_residual

    predicted_min = decode_measure(min_score, regression["countCeiling"])
    predicted_max = max(predicted_min, decode_measure(max_score, regression["countCeiling"]))
    return {
        "predictedMin": round(predicted_min, 2),
        "predictedMax": round(predicted_max, 2),
    }


def estimate_max_sigma(rows, model):
    residuals = [row["catchMax"] - predict_shared_model_row(row, model)["predictedMax"] for row in rows]
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


def weighted_error(prediction, row):
    return abs(prediction["predictedMin"] - row["catchMin"]) * 0.35 + abs(prediction["predictedMax"] - row["catchMax"])


def evaluate_neural_config(train_rows, validation_rows, config, seed_key):
    model = fit_neural_model(train_rows, config, seed_key=seed_key)
    min_errors = []
    max_errors = []
    weighted_errors = []
    yy_points = []
    for row in validation_rows:
        prediction = predict_shared_model_row(row, model)
        min_errors.append(abs(prediction["predictedMin"] - row["catchMin"]))
        max_errors.append(abs(prediction["predictedMax"] - row["catchMax"]))
        weighted_errors.append(weighted_error(prediction, row))
        yy_points.append(
            {
                "date": row["date"].isoformat(),
                "actualMin": round(row["catchMin"], 2),
                "predictedMin": prediction["predictedMin"],
                "actualMax": round(row["catchMax"], 2),
                "predictedMax": prediction["predictedMax"],
            }
        )
    return {
        "score": sum(weighted_errors) / len(weighted_errors),
        "validationRows": len(validation_rows),
        "minMae": round(sum(min_errors) / len(min_errors), 3),
        "maxMae": round(sum(max_errors) / len(max_errors), 3),
        "yyPoints": yy_points,
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
                "moonSin2": features["moonSin2"],
                "moonCos2": features["moonCos2"],
                "moonSin3": features["moonSin3"],
                "moonCos3": features["moonCos3"],
                "airSeaGap": features["airSeaGap"],
                "airSeaMean": features["airSeaMean"],
                "airSeaAbsGap": features["airSeaAbsGap"],
                "moonFullness": features["moonFullness"],
            }
        )
    return rows


def species_key(ship_id, species_name):
    digest = hashlib.sha1(f"{ship_id}:{species_name}".encode("utf-8")).hexdigest()[:12]
    return digest


def species_feature_id(species_name, unit):
    return hashlib.sha1(f"{species_name}:{unit}".encode("utf-8")).hexdigest()[:10]


def summarize_species_activity(daily_reports):
    species_summaries = {}
    for day_record in daily_reports:
        for species_name, units in day_record["species"].items():
            summary = species_summaries.setdefault(species_name, {"units": Counter(), "positiveDays": Counter()})
            for unit, measurement in units.items():
                summary["units"][unit] += 1
                if measurement["max"] > 0:
                    summary["positiveDays"][unit] += 1
    return species_summaries


def average_rows(rows, field):
    return round(sum(row[field] for row in rows) / len(rows), 2)


def build_context_profile(rows):
    positive_rows = [row for row in rows if row["catchMax"] > 0]
    average_spread = sum(max(row["catchMax"] - row["catchMin"], 0.0) for row in rows) / len(rows)
    return {
        "contextAvgMin": round(sum(row["catchMin"] for row in rows) / len(rows), 4),
        "contextAvgMax": round(sum(row["catchMax"] for row in rows) / len(rows), 4),
        "contextPositiveRate": round(len(positive_rows) / len(rows), 6),
        "contextAvgSpread": round(average_spread, 4),
        "contextTripScale": round(math.log1p(len(rows)), 6),
    }


def build_species_profiles(contexts):
    buckets = {}
    for context in contexts:
        bucket = buckets.setdefault(context["speciesFeatureId"], [])
        bucket.extend(context["rows"])

    profiles = {}
    for key, rows in buckets.items():
        positive_rows = [row for row in rows if row["catchMax"] > 0]
        profiles[key] = {
            "speciesAvgMax": round(sum(row["catchMax"] for row in rows) / len(rows), 4),
            "speciesPositiveRate": round(len(positive_rows) / len(rows), 6),
            "speciesTripScale": round(math.log1p(len(rows)), 6),
        }
    return profiles


def build_global_model_space(contexts):
    ship_feature_keys = tuple(f'ship::{ship_id}' for ship_id in sorted({context["shipMeta"]["id"] for context in contexts}))
    species_feature_keys = tuple(
        f'species::{feature_id}' for feature_id in sorted({context["speciesFeatureId"] for context in contexts})
    )
    return {
        "shipFeatureKeys": ship_feature_keys,
        "speciesFeatureKeys": species_feature_keys,
    }


def build_context_feature_map(context, model_space, species_profiles):
    features = {}
    features.update(context["profile"])
    features.update(species_profiles.get(context["speciesFeatureId"], {}))
    for key in GLOBAL_CONTEXT_PROFILE_KEYS:
        features.setdefault(key, 0.0)
    for key in model_space["shipFeatureKeys"]:
        features[key] = 1.0 if key == f'ship::{context["shipMeta"]["id"]}' else 0.0
    for key in model_space["speciesFeatureKeys"]:
        features[key] = 1.0 if key == f'species::{context["speciesFeatureId"]}' else 0.0
    return features


def build_aggregate_observed_text(rows, unit):
    if not rows:
        return None
    average_min = average_rows(rows, "catchMin")
    average_max = average_rows(rows, "catchMax")
    return f'{len(rows)}船平均 {average_min:.1f}〜{average_max:.1f}{unit}'


def build_ship_ranking_entry(ship_meta, rows, unit):
    positive_rows = [row for row in rows if row["catchMax"] > 0]
    center_values = [(row["catchMin"] + row["catchMax"]) * 0.5 for row in rows]
    return {
        "shipId": ship_meta["id"],
        "shipName": ship_meta["name"],
        "location": ship_meta["location"],
        "homeUrl": ship_meta["homeUrl"],
        "catchUrl": ship_meta["catchUrl"],
        "unit": unit,
        "tripDays": len(rows),
        "positiveDays": len(positive_rows),
        "averageMin": round(sum(row["catchMin"] for row in rows) / len(rows), 2),
        "averageMax": round(sum(row["catchMax"] for row in rows) / len(rows), 2),
        "averageCenter": round(sum(center_values) / len(center_values), 2),
    }


def build_ship_species_contexts(ship_contexts):
    contexts = []
    for source_context in ship_contexts:
        ship_meta = source_context["ship_meta"]
        daily_reports = source_context["daily_reports"]
        species_summaries = summarize_species_activity(daily_reports)
        for species_name, summary in species_summaries.items():
            unit, positive_days = summary["positiveDays"].most_common(1)[0] if summary["positiveDays"] else ("", 0)
            if positive_days < MIN_POSITIVE_DAYS:
                continue
            rows = build_species_rows(
                daily_reports,
                species_name,
                unit,
                source_context["archive_map"],
                source_context["forecast_map"],
                source_context["climatology"],
            )
            positive_rows = [row for row in rows if row["catchMax"] > 0]
            if len(positive_rows) < MIN_POSITIVE_DAYS:
                continue
            contexts.append(
                {
                    "contextKey": f'{ship_meta["id"]}:{species_name}:{unit}',
                    "shipMeta": ship_meta,
                    "sourceContext": source_context,
                    "speciesName": species_name,
                    "speciesUnit": unit,
                    "speciesFeatureId": species_feature_id(species_name, unit),
                    "rows": rows,
                    "positiveRows": positive_rows,
                    "profile": build_context_profile(rows),
                }
            )

    model_space = build_global_model_space(contexts)
    species_profiles = build_species_profiles(contexts)
    for context in contexts:
        context["contextFeatures"] = build_context_feature_map(context, model_space, species_profiles)
        context["modelRows"] = [
            {
                **row,
                **context["contextFeatures"],
                "contextKey": context["contextKey"],
                "shipId": context["shipMeta"]["id"],
                "speciesFeatureId": context["speciesFeatureId"],
            }
            for row in context["rows"]
        ]
    return contexts, model_space


def build_global_model_candidates(model_space):
    profile_keys = list(GLOBAL_CONTEXT_PROFILE_KEYS)
    ship_keys = list(model_space["shipFeatureKeys"])
    species_keys = list(model_space["speciesFeatureKeys"])
    rich_context_keys = [*NEURAL_FEATURE_SETS["rich"]["featureKeys"], *profile_keys, *ship_keys, *species_keys]
    return (
        {
            "id": "balanced_context_nn_48x24",
            "modelType": "neural_network",
            "feature_set": "extended",
            "featureKeys": [*NEURAL_FEATURE_SETS["extended"]["featureKeys"], *profile_keys, *ship_keys, *species_keys],
            "hidden_sizes": (48, 24),
            "epochs": 220,
            "learning_rate": 0.011,
            "weight_decay": 0.00035,
        },
        {
            "id": "balanced_context_nn_72x36",
            "modelType": "neural_network",
            "feature_set": "rich",
            "featureKeys": rich_context_keys,
            "hidden_sizes": (72, 36),
            "epochs": 360,
            "learning_rate": 0.0085,
            "weight_decay": 0.00008,
        },
        {
            "id": "balanced_context_rf_128",
            "modelType": "random_forest",
            "label": "ランダムフォレスト",
            "featureKeys": rich_context_keys,
            "tree_count": 128,
            "max_depth": 10,
            "min_leaf": 1,
            "max_features": 16,
            "threshold_steps": 18,
            "early_stopping_patience": 18,
            "early_stopping_min_delta": 0.002,
        },
        {
            "id": "balanced_context_rf_224",
            "modelType": "random_forest",
            "label": "ランダムフォレスト",
            "featureKeys": rich_context_keys,
            "tree_count": 224,
            "max_depth": 12,
            "min_leaf": 1,
            "max_features": 24,
            "threshold_steps": 28,
            "early_stopping_patience": 28,
            "early_stopping_min_delta": 0.001,
        },
    )


def split_global_training_rows(contexts):
    split_contexts = []
    train_rows = []
    validation_rows = []
    for context in contexts:
        context_train_rows, context_validation_rows = split_rows_for_validation(context["modelRows"], context["contextKey"])
        train_rows.extend(context_train_rows)
        validation_rows.extend(context_validation_rows)
        split_contexts.append(
            {
                **context,
                "trainRows": context_train_rows,
                "validationRows": context_validation_rows,
            }
        )
    return split_contexts, train_rows, validation_rows


def build_evaluation_summary(points):
    if len(points) < 2:
        return None
    min_mae = sum(abs(point["predictedMin"] - point["actualMin"]) for point in points) / len(points)
    max_mae = sum(abs(point["predictedMax"] - point["actualMax"]) for point in points) / len(points)
    score = sum(
        abs(point["predictedMin"] - point["actualMin"]) * 0.35 + abs(point["predictedMax"] - point["actualMax"])
        for point in points
    ) / len(points)
    return {
        "validationRows": len(points),
        "minMae": round(min_mae, 3),
        "maxMae": round(max_mae, 3),
        "score": round(score, 3),
        "yyPoints": points,
    }


def summarize_context_evaluations(summaries):
    summaries = [summary for summary in summaries if summary]
    if not summaries:
        return None
    scores = [summary["score"] for summary in summaries]
    return {
        "contexts": len(summaries),
        "score": round(sum(scores) / len(scores), 3),
        "minMae": round(sum(summary["minMae"] for summary in summaries) / len(summaries), 3),
        "maxMae": round(sum(summary["maxMae"] for summary in summaries) / len(summaries), 3),
        "worstScore": round(max(scores), 3),
        "p85Score": round(quantile(scores, 0.85), 3),
    }


def fit_global_model(rows, config, seed_key, sample_weights=None, validation_rows=None, validation_weights=None):
    if config["modelType"] == "neural_network":
        return fit_neural_model(rows, config, seed_key=seed_key, sample_weights=sample_weights)
    if config["modelType"] == "random_forest":
        return fit_random_forest_model(
            rows,
            config,
            seed_key=seed_key,
            sample_weights=sample_weights,
            validation_rows=validation_rows,
            validation_weights=validation_weights,
        )
    raise RuntimeError(f'Unsupported model config type: {config["modelType"]}')


def select_global_model_config(contexts, model_space):
    candidates = build_global_model_candidates(model_space)
    split_contexts, train_rows, validation_rows = split_global_training_rows(contexts)
    if not validation_rows:
        return candidates[0]

    best_config = None
    best_summary = None
    validation_weights = compute_balanced_row_weights(validation_rows)
    for config in candidates:
        row_weights = compute_balanced_row_weights(train_rows)
        model = fit_global_model(
            train_rows,
            config,
            seed_key=f'global:{config["id"]}:selection',
            sample_weights=row_weights,
            validation_rows=validation_rows,
            validation_weights=validation_weights,
        )
        context_summaries = []
        all_points = []
        for context in split_contexts:
            points = []
            for row in context["validationRows"]:
                prediction = predict_shared_model_row(row, model)
                point = {
                    "actualMin": round(row["catchMin"], 2),
                    "predictedMin": prediction["predictedMin"],
                    "actualMax": round(row["catchMax"], 2),
                    "predictedMax": prediction["predictedMax"],
                }
                points.append(point)
                all_points.append(point)
            context_summaries.append(build_evaluation_summary(points))
        balanced_summary = summarize_context_evaluations(context_summaries)
        global_summary = build_evaluation_summary(all_points)
        if balanced_summary is None or global_summary is None:
            continue
        summary = {
            "balanced": balanced_summary,
            "global": global_summary,
        }
        if best_summary is None or (
            summary["balanced"]["score"],
            summary["balanced"]["p85Score"],
            summary["balanced"]["worstScore"],
            summary["global"]["score"],
            summary["global"]["maxMae"],
        ) < (
            best_summary["balanced"]["score"],
            best_summary["balanced"]["p85Score"],
            best_summary["balanced"]["worstScore"],
            best_summary["global"]["score"],
            best_summary["global"]["maxMae"],
        ):
            best_config = {
                **config,
                "selected_tree_count": model.get("training", {}).get("selectedTreeCount", config.get("tree_count")),
            }
            best_summary = summary
    return best_config or candidates[0]


def build_global_evaluations(contexts, config):
    split_contexts, train_rows, validation_rows = split_global_training_rows(contexts)
    if not train_rows:
        return {}, {}

    row_weights = compute_balanced_row_weights(train_rows)
    evaluation_model = fit_global_model(
        train_rows,
        config,
        seed_key=f'global:{config["id"]}:evaluation',
        sample_weights=row_weights,
        validation_rows=validation_rows,
        validation_weights=compute_balanced_row_weights(validation_rows),
    )
    ship_evaluations = {}
    aggregate_groups = {}

    for context in split_contexts:
        yy_points = []
        for row in context["validationRows"]:
            prediction = predict_shared_model_row(row, evaluation_model)
            point = {
                "date": row["date"].isoformat(),
                "actualMin": round(row["catchMin"], 2),
                "predictedMin": prediction["predictedMin"],
                "actualMax": round(row["catchMax"], 2),
                "predictedMax": prediction["predictedMax"],
            }
            yy_points.append(point)
            aggregate_groups.setdefault((context["speciesFeatureId"], row["date"]), []).append(point)
        ship_evaluations[context["contextKey"]] = build_evaluation_summary(yy_points)

    aggregate_evaluations = {}
    grouped_points_by_species = {}
    for (species_feature_id, row_date), points in aggregate_groups.items():
        grouped_points_by_species.setdefault(species_feature_id, []).append(
            {
                "date": row_date.isoformat(),
                "actualMin": round(sum(point["actualMin"] for point in points) / len(points), 2),
                "predictedMin": round(sum(point["predictedMin"] for point in points) / len(points), 2),
                "actualMax": round(sum(point["actualMax"] for point in points) / len(points), 2),
                "predictedMax": round(sum(point["predictedMax"] for point in points) / len(points), 2),
            }
        )

    for species_feature_id, points in grouped_points_by_species.items():
        points.sort(key=lambda item: item["date"])
        aggregate_evaluations[species_feature_id] = build_evaluation_summary(points)

    return ship_evaluations, aggregate_evaluations


def build_ship_payloads(ship_meta, daily_reports, ship_species_contexts, today, global_model, ship_evaluations):
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
    ship_contexts = sorted(
        [context for context in ship_species_contexts if context["shipMeta"]["id"] == ship_meta["id"]],
        key=lambda item: (-len(item["positiveRows"]), item["speciesName"]),
    )

    for context in ship_contexts:
        rows = context["rows"]
        positive_rows = context["positiveRows"]
        source_context = context["sourceContext"]
        evaluation = ship_evaluations.get(context["contextKey"])
        max_sigma = estimate_max_sigma(context["modelRows"], global_model)
        observed_by_date = {row["date"]: row for row in rows}

        future_predictions = []
        current_day = future_start
        while current_day <= future_end:
            resolved, source = resolve_prediction_feature(
                current_day,
                source_context["archive_map"],
                source_context["forecast_map"],
                source_context["climatology"],
            )
            raw_features = {
                "airTemp": resolved["temperature_2m_mean"],
                "seaTemp": resolved["sea_surface_temperature_mean"],
                "moonAge": moon_age_for(current_day),
            }
            prediction = predict_shared_model(raw_features, global_model, context["contextFeatures"])
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
            f'{ship_meta["id"]}:{context["speciesName"]}:{today.isoformat()}',
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

        species_id = species_key(ship_meta["id"], context["speciesName"])
        file_name = f'{ship_meta["id"]}-{species_id}.json'
        payload = {
            "generatedAt": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
            "today": today.isoformat(),
            "scope": {
                "mode": "ship",
                "label": "船別",
            },
            "ship": {
                "id": ship_meta["id"],
                "name": ship_meta["name"],
                "location": ship_catalog_entry["location"],
                "homeUrl": ship_meta["homeUrl"],
                "catchUrl": ship_meta["catchUrl"],
            },
            "species": {
                "id": species_id,
                "label": context["speciesName"],
                "unit": context["speciesUnit"],
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
                "unit": context["speciesUnit"],
            },
            "xDayModel": {
                "method": "monte_carlo_peak",
                "samples": XDAY_MONTE_CARLO_SAMPLES,
                "maxSigma": max_sigma,
                "maximaSamples": maxima_samples,
            },
            "featureRanges": feature_ranges,
            "evaluation": evaluation,
            "model": global_model,
            "contextFeatures": context["contextFeatures"],
            "topDays": top_days,
            "predictions": future_predictions,
        }
        payloads.append((file_name, payload))
        ship_catalog_entry["species"].append(
            {
                "id": species_id,
                "label": context["speciesName"],
                "unit": context["speciesUnit"],
                "positiveDays": len(positive_rows),
                "tripDays": len(rows),
                "file": f"data/payloads/{file_name}",
            }
        )

    ship_catalog_entry["species"].sort(key=lambda item: (-item["positiveDays"], item["label"]))
    return ship_catalog_entry, payloads


def build_aggregate_payloads(ship_species_contexts, today, global_model, aggregate_evaluations):
    future_start = today + timedelta(days=1)
    future_end = today + timedelta(days=FORECAST_DAYS)
    catalog_entries = []
    payloads = []

    grouped_contexts = {}
    for context in ship_species_contexts:
        key = (context["speciesName"], context["speciesUnit"], context["speciesFeatureId"])
        grouped_contexts.setdefault(key, []).append(context)

    for (species_name, unit, species_feature_id), contexts in sorted(
        grouped_contexts.items(),
        key=lambda item: (-sum(len(context["positiveRows"]) for context in item[1]), item[0][0]),
    ):
        rows = []
        ship_rankings = []
        observed_by_date = {}
        for context in contexts:
            ship_rankings.append(build_ship_ranking_entry(context["shipMeta"], context["rows"], unit))
            for row in context["rows"]:
                observed_by_date.setdefault(row["date"], []).append(row)
                rows.append(
                    {
                        **row,
                        "shipId": context["shipMeta"]["id"],
                        "shipName": context["shipMeta"]["name"],
                    }
                )

        positive_rows = [row for row in rows if row["catchMax"] > 0]
        if len(positive_rows) < MIN_POSITIVE_DAYS or len(rows) < MIN_VALIDATION_ROWS:
            continue
        rows.sort(key=lambda item: (item["date"], item["shipId"]))

        evaluation = aggregate_evaluations.get(species_feature_id)
        aggregate_sigma_rows = []
        for context in contexts:
            aggregate_sigma_rows.extend(context["modelRows"])
        max_sigma = estimate_max_sigma(aggregate_sigma_rows, global_model)

        future_predictions = []
        current_day = future_start
        while current_day <= future_end:
            ship_predictions = []
            current_moon_age = moon_age_for(current_day)
            source_counts = Counter()
            for context in contexts:
                source_context = context["sourceContext"]
                resolved, source = resolve_prediction_feature(
                    current_day,
                    source_context["archive_map"],
                    source_context["forecast_map"],
                    source_context["climatology"],
                )
                raw_features = {
                    "airTemp": resolved["temperature_2m_mean"],
                    "seaTemp": resolved["sea_surface_temperature_mean"],
                    "moonAge": current_moon_age,
                }
                prediction = predict_shared_model(raw_features, global_model, context["contextFeatures"])
                ship_predictions.append(
                    {
                        "prediction": prediction,
                        "airTemp": raw_features["airTemp"],
                        "seaTemp": raw_features["seaTemp"],
                    }
                )
                source_counts[source] += 1

            prior_year_day = same_day_last_year(current_day)
            observed_rows = observed_by_date.get(prior_year_day, [])
            future_predictions.append(
                {
                    "date": current_day.isoformat(),
                    "predictedMin": round(
                        sum(item["prediction"]["predictedMin"] for item in ship_predictions) / len(ship_predictions),
                        2,
                    ),
                    "predictedMax": round(
                        sum(item["prediction"]["predictedMax"] for item in ship_predictions) / len(ship_predictions),
                        2,
                    ),
                    "airTemp": round(sum(item["airTemp"] for item in ship_predictions) / len(ship_predictions), 2),
                    "seaTemp": round(sum(item["seaTemp"] for item in ship_predictions) / len(ship_predictions), 2),
                    "moonAge": round(current_moon_age, 2),
                    "featureSource": source_counts.most_common(1)[0][0],
                    "observedDate": prior_year_day.isoformat() if observed_rows else None,
                    "observedMin": average_rows(observed_rows, "catchMin") if observed_rows else None,
                    "observedMax": average_rows(observed_rows, "catchMax") if observed_rows else None,
                    "observedText": build_aggregate_observed_text(observed_rows, unit),
                    "observedShipCount": len(observed_rows) if observed_rows else 0,
                    "shipCount": len(ship_predictions),
                    "probability": 0.0,
                }
            )
            current_day += timedelta(days=1)

        maxima_samples = build_xday_distribution(
            future_predictions,
            max_sigma,
            f"aggregate:{species_name}:{today.isoformat()}",
        )
        peak_day = min(future_predictions, key=lambda item: (-item["predictedMax"], item["date"]))
        top_days = sorted(future_predictions, key=lambda item: (-item["predictedMax"], item["date"]))[:4]
        default_point = top_days[0] if top_days else future_predictions[0]
        ranking = sorted(
            ship_rankings,
            key=lambda item: (-item["averageMax"], -item["averageCenter"], -item["positiveDays"], item["shipName"]),
        )

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

        species_id = species_key("aggregate", f"{species_name}:{unit}")
        file_name = f"aggregate-{species_id}.json"
        payload = {
            "generatedAt": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
            "today": today.isoformat(),
            "scope": {
                "mode": "aggregate",
                "label": "魚種統合",
            },
            "ship": None,
            "aggregate": {
                "shipCount": len(ranking),
                "ranking": ranking,
                "modelContexts": [
                    {
                        "shipId": context["shipMeta"]["id"],
                        "contextFeatures": context["contextFeatures"],
                    }
                    for context in contexts
                ],
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
            "model": global_model,
            "topDays": top_days,
            "predictions": future_predictions,
        }
        payloads.append((file_name, payload))
        catalog_entries.append(
            {
                "id": species_id,
                "label": species_name,
                "unit": unit,
                "shipCount": len(ranking),
                "positiveDays": len(positive_rows),
                "tripDays": len(rows),
                "file": f"data/payloads/{file_name}",
            }
        )

    catalog_entries.sort(key=lambda item: (-item["shipCount"], -item["positiveDays"], item["label"]))
    return catalog_entries, payloads


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

    ship_contexts = []

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

        ship_contexts.append(
            {
                "ship_meta": ship_meta,
                "daily_reports": daily_reports,
                "archive_map": archive_map,
                "forecast_map": forecast_map,
                "climatology": climatology,
            }
        )

    ship_species_contexts, model_space = build_ship_species_contexts(ship_contexts)
    global_model_config = select_global_model_config(ship_species_contexts, model_space)
    ship_evaluations, aggregate_evaluations = build_global_evaluations(ship_species_contexts, global_model_config)
    global_training_rows = [row for context in ship_species_contexts for row in context["modelRows"]]
    global_model = fit_global_model(
        global_training_rows,
        global_model_config,
        seed_key=f'global:{global_model_config["id"]}:final',
        sample_weights=compute_balanced_row_weights(global_training_rows),
    )

    all_catalog_ships = []
    all_payloads = []
    for context in ship_contexts:
        ship_catalog_entry, ship_payloads = build_ship_payloads(
            context["ship_meta"],
            context["daily_reports"],
            ship_species_contexts,
            today,
            global_model,
            ship_evaluations,
        )
        if not ship_catalog_entry["species"]:
            raise RuntimeError(f'No qualifying species were generated for ship {context["ship_meta"]["id"]}.')

        all_catalog_ships.append(ship_catalog_entry)
        all_payloads.extend(ship_payloads)

    aggregate_catalog_entries, aggregate_payloads = build_aggregate_payloads(
        ship_species_contexts,
        today,
        global_model,
        aggregate_evaluations,
    )
    all_payloads.extend(aggregate_payloads)

    catalog = {
        "generatedAt": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
        "today": today.isoformat(),
        "ships": sorted(all_catalog_ships, key=lambda item: item["name"]),
        "aggregateSpecies": aggregate_catalog_entries,
    }
    write_outputs(catalog, all_payloads)


if __name__ == "__main__":
    main()
