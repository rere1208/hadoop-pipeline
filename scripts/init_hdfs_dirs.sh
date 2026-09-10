#!/usr/bin/env bash
# Cree l'arborescence du Data Lake dans HDFS. A executer une fois le
# cluster demarre (docker compose up -d), avant le premier run du DAG.
#
# Usage: ./scripts/init_hdfs_dirs.sh
set -euo pipefail

docker exec namenode hdfs dfs -mkdir -p /data-lake/raw/weather
docker exec namenode hdfs dfs -mkdir -p /data-lake/processed/weather
docker exec namenode hdfs dfs -chmod -R 777 /data-lake

echo "Data Lake initialise :"
docker exec namenode hdfs dfs -ls -R /data-lake
