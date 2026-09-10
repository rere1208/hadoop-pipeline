-- Vues "propres" consommables pour l'analyse (executees apres 01_create_schema.sql
-- au demarrage du conteneur postgres-dwh).

-- Lecture propre, dedupliquee, des relevés individuels.
CREATE OR REPLACE VIEW clean_weather AS
SELECT DISTINCT ON (city, fetched_at)
    city,
    latitude,
    longitude,
    fetched_at,
    date_trunc('day', fetched_at) AS reading_date,
    ROUND(temperature_c::numeric, 1)  AS temperature_c,
    ROUND(humidity_pct::numeric, 1)   AS humidity_pct,
    ROUND(wind_speed_kmh::numeric, 1) AS wind_speed_kmh,
    ROUND(pm2_5::numeric, 1)          AS pm2_5,
    ROUND(european_aqi::numeric, 1)   AS european_aqi,
    COALESCE(air_quality_category, 'unknown') AS air_quality_category
FROM staging_weather
WHERE city IS NOT NULL
  AND fetched_at IS NOT NULL
ORDER BY city, fetched_at, loaded_at DESC;

-- Agregats journaliers par ville, calcules en SQL (complement de la
-- transformation Spark qui, elle, nettoie/type les donnees brutes).
CREATE OR REPLACE VIEW daily_weather_summary AS
SELECT
    city,
    reading_date,
    COUNT(*)                          AS nb_readings,
    ROUND(AVG(temperature_c), 1)      AS avg_temperature_c,
    ROUND(MIN(temperature_c), 1)      AS min_temperature_c,
    ROUND(MAX(temperature_c), 1)      AS max_temperature_c,
    ROUND(AVG(wind_speed_kmh), 1)     AS avg_wind_speed_kmh,
    ROUND(AVG(european_aqi), 1)       AS avg_european_aqi,
    MODE() WITHIN GROUP (ORDER BY air_quality_category) AS dominant_air_quality
FROM clean_weather
GROUP BY city, reading_date
ORDER BY reading_date DESC, city;
