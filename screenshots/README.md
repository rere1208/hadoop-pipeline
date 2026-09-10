# Captures d'écran — preuves de fonctionnement

Pipeline exécuté et vérifié de bout en bout le 10/09/2026 (voir [README.md](../README.md) racine, section 7).

- `namenode-overview.png` — NameNode UI (`:9870`), cluster actif, 1 DataNode live
- `namenode-browse-datalake.png` — Data Lake HDFS : dossiers `raw/` et `processed/`
- `yarn-applications.png` — ResourceManager UI (`:8088`), 3 jobs Spark `weather-transform` en `FINISHED` / `SUCCEEDED`
- `airflow-dag-success.png` — Graph du DAG `weather_pipeline_dag`, 5 tâches en `success`
- `airflow-dag-list.png` — Liste des DAGs Airflow, run réussi
- `postgres-query-result.png` — `SELECT * FROM daily_weather_summary;` sur le Data Warehouse PostgreSQL
