"""Job Spark soumis via `spark-submit --master yarn` (soumis par Airflow).

Lit le JSON brut deverse dans le Data Lake (HDFS, zone raw), aplatit les
champs imbriques renvoyes par Open-Meteo, type/nettoie les colonnes et
derive une categorie de qualite de l'air. Ecrit le resultat en Parquet dans
la zone processed du Data Lake, pret a etre charge dans PostgreSQL.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark_session(app_name: str = "weather-transform") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def aqi_category(col):
    return (
        F.when(col.isNull(), F.lit("unknown"))
        .when(col <= 20, F.lit("good"))
        .when(col <= 40, F.lit("fair"))
        .when(col <= 60, F.lit("moderate"))
        .when(col <= 80, F.lit("poor"))
        .otherwise(F.lit("very_poor"))
    )


def transform(spark: SparkSession, input_dir: str):
    raw = spark.read.json(input_dir)

    flat = raw.select(
        F.col("city"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.to_timestamp("fetched_at").alias("fetched_at"),
        F.col("weather.temperature_2m").cast("double").alias("temperature_c"),
        F.col("weather.relative_humidity_2m").cast("double").alias("humidity_pct"),
        F.col("weather.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
        F.col("weather.weather_code").cast("int").alias("weather_code"),
        F.col("air_quality.pm10").cast("double").alias("pm10"),
        F.col("air_quality.pm2_5").cast("double").alias("pm2_5"),
        F.col("air_quality.european_aqi").cast("double").alias("european_aqi"),
    )

    cleaned = (
        flat.dropna(subset=["city", "fetched_at"])
        .dropDuplicates(["city", "fetched_at"])
        .withColumn("air_quality_category", aqi_category(F.col("european_aqi")))
    )

    return cleaned


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True, help="Repertoire HDFS des JSON bruts")
    parser.add_argument("--output-dir", required=True, help="Repertoire HDFS de sortie (Parquet)")
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        cleaned = transform(spark, args.input_dir)
        cleaned.write.mode("overwrite").parquet(args.output_dir)
        print(f"Wrote {cleaned.count()} cleaned rows to {args.output_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
