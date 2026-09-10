"""Extrait la meteo courante + qualite de l'air pour un ensemble de villes
depuis l'API publique Open-Meteo (gratuite, sans cle) et ecrit un JSON brut
horodate en local. Ce fichier est ensuite pousse tel quel dans le Data Lake
(HDFS) par la tache Airflow suivante : aucune transformation ici, seulement
de la collecte.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import requests

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

WEATHER_PARAMS = "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"
AIR_QUALITY_PARAMS = "pm10,pm2_5,european_aqi"


def parse_cities(raw: str) -> list[dict]:
    """Parse la variable WEATHER_CITIES: "lat,lon,Nom;lat,lon,Nom;..." """
    cities = []
    for chunk in raw.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        lat, lon, name = chunk.split(",")
        cities.append({"name": name.strip(), "lat": float(lat), "lon": float(lon)})
    return cities


def fetch_city(session: requests.Session, city: dict) -> dict:
    weather_resp = session.get(
        WEATHER_URL,
        params={"latitude": city["lat"], "longitude": city["lon"], "current": WEATHER_PARAMS},
        timeout=15,
    )
    weather_resp.raise_for_status()

    air_resp = session.get(
        AIR_QUALITY_URL,
        params={"latitude": city["lat"], "longitude": city["lon"], "current": AIR_QUALITY_PARAMS},
        timeout=15,
    )
    air_resp.raise_for_status()

    return {
        "city": city["name"],
        "latitude": city["lat"],
        "longitude": city["lon"],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "weather": weather_resp.json().get("current", {}),
        "air_quality": air_resp.json().get("current", {}),
    }


def run(cities_raw: str, output_dir: str) -> str:
    cities = parse_cities(cities_raw)
    if not cities:
        raise ValueError("WEATHER_CITIES est vide ou mal forme")

    session = requests.Session()
    records = [fetch_city(session, city) for city in cities]

    os.makedirs(output_dir, exist_ok=True)
    filename = f"weather_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json"
    output_path = os.path.join(output_dir, filename)

    with open(output_path, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(record, ensure_ascii=False) + "\n" for record in records)

    return output_path


def main():
    parser = argparse.ArgumentParser(description="Extraction Open-Meteo vers JSON brut")
    parser.add_argument("--output-dir", default="/tmp/raw_weather")
    parser.add_argument(
        "--cities",
        default=os.environ.get("WEATHER_CITIES", ""),
        help="Format: lat,lon,Nom;lat,lon,Nom;...",
    )
    args = parser.parse_args()

    path = run(args.cities, args.output_dir)
    print(path)


if __name__ == "__main__":
    main()
    sys.exit(0)
