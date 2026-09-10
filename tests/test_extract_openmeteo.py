import json

from ingestion.extract_openmeteo import parse_cities, run


def test_parse_cities():
    raw = "48.8566,2.3522,Paris;45.7640,4.8357,Lyon"
    cities = parse_cities(raw)
    assert cities == [
        {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
        {"name": "Lyon", "lat": 45.7640, "lon": 4.8357},
    ]


def test_parse_cities_ignores_blank_segments():
    assert parse_cities("48.8566,2.3522,Paris;;") == [
        {"name": "Paris", "lat": 48.8566, "lon": 2.3522}
    ]


def test_run_writes_one_line_per_city(tmp_path, requests_mock):
    requests_mock.get(
        "https://api.open-meteo.com/v1/forecast",
        json={"current": {"temperature_2m": 21.5, "wind_speed_10m": 10.0}},
    )
    requests_mock.get(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        json={"current": {"pm2_5": 8.3, "european_aqi": 25}},
    )

    output_path = run("48.8566,2.3522,Paris;45.7640,4.8357,Lyon", str(tmp_path))

    with open(output_path, encoding="utf-8") as f:
        lines = [json.loads(line) for line in f]
    assert len(lines) == 2
    assert {record["city"] for record in lines} == {"Paris", "Lyon"}
    assert lines[0]["weather"]["temperature_2m"] == 21.5
    assert lines[0]["air_quality"]["european_aqi"] == 25
