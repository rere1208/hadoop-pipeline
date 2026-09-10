-- Schema du Data Warehouse (execute automatiquement au premier demarrage
-- du conteneur postgres-dwh via docker-entrypoint-initdb.d).

CREATE TABLE IF NOT EXISTS staging_weather (
    city                  TEXT        NOT NULL,
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    fetched_at            TIMESTAMPTZ NOT NULL,
    temperature_c         DOUBLE PRECISION,
    humidity_pct          DOUBLE PRECISION,
    wind_speed_kmh        DOUBLE PRECISION,
    weather_code          INTEGER,
    pm10                  DOUBLE PRECISION,
    pm2_5                 DOUBLE PRECISION,
    european_aqi          DOUBLE PRECISION,
    air_quality_category  TEXT,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city, fetched_at)
);

CREATE TABLE IF NOT EXISTS dim_city (
    city       TEXT PRIMARY KEY,
    latitude   DOUBLE PRECISION NOT NULL,
    longitude  DOUBLE PRECISION NOT NULL
);
