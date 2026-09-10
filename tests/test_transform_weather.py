import json

import pytest
from pyspark.sql import SparkSession

from spark_jobs.transform_weather import transform


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-weather-transform")
        .getOrCreate()
    )
    yield session
    session.stop()


RAW_RECORDS = [
    {
        "city": "Paris",
        "latitude": 48.8566,
        "longitude": 2.3522,
        "fetched_at": "2024-01-01T10:00:00+00:00",
        "weather": {"temperature_2m": 5.0, "relative_humidity_2m": 80, "wind_speed_10m": 15.0, "weather_code": 3},
        "air_quality": {"pm10": 20.0, "pm2_5": 10.0, "european_aqi": 15},
    },
    {
        # duplicate (city, fetched_at) -> should be deduplicated
        "city": "Paris",
        "latitude": 48.8566,
        "longitude": 2.3522,
        "fetched_at": "2024-01-01T10:00:00+00:00",
        "weather": {"temperature_2m": 5.0, "relative_humidity_2m": 80, "wind_speed_10m": 15.0, "weather_code": 3},
        "air_quality": {"pm10": 20.0, "pm2_5": 10.0, "european_aqi": 15},
    },
    {
        "city": "Lyon",
        "latitude": 45.7640,
        "longitude": 4.8357,
        "fetched_at": "2024-01-01T10:05:00+00:00",
        "weather": {"temperature_2m": 8.0, "relative_humidity_2m": 60, "wind_speed_10m": 5.0, "weather_code": 1},
        "air_quality": {"pm10": 55.0, "pm2_5": 40.0, "european_aqi": 70},
    },
]


def test_transform_flattens_dedupes_and_categorizes(spark, tmp_path):
    raw_path = tmp_path / "raw.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(record) + "\n" for record in RAW_RECORDS)

    result = transform(spark, str(raw_path)).orderBy("city").collect()

    assert len(result) == 2  # duplicate row removed

    paris = next(r for r in result if r.city == "Paris")
    lyon = next(r for r in result if r.city == "Lyon")

    assert paris.temperature_c == 5.0
    assert paris.air_quality_category == "good"  # european_aqi 15 <= 20

    assert lyon.european_aqi == 70
    assert lyon.air_quality_category == "poor"  # 60 < 70 <= 80
