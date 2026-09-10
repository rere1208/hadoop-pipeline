"""Petits helpers WebHDFS partages par les taches Airflow.

On utilise le client WebHDFS (bibliotheque `hdfs`) plutot que le CLI `hdfs`
natif : cela evite d'avoir a embarquer une distribution Hadoop complete dans
l'image Airflow, tout en parlant au meme cluster HDFS (webhdfs active via
`HDFS_CONF_dfs_webhdfs_enabled=true` dans hadoop.env).
"""
import os

from hdfs import InsecureClient

DEFAULT_WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://namenode:9870")
DEFAULT_HDFS_USER = os.environ.get("HDFS_USER", "root")


def get_client() -> InsecureClient:
    return InsecureClient(DEFAULT_WEBHDFS_URL, user=DEFAULT_HDFS_USER)


def upload_file(local_path: str, hdfs_dir: str) -> str:
    client = get_client()
    client.makedirs(hdfs_dir)
    filename = os.path.basename(local_path)
    hdfs_path = f"{hdfs_dir.rstrip('/')}/{filename}"
    client.upload(hdfs_path, local_path, overwrite=True)
    return hdfs_path


def download_dir(hdfs_dir: str, local_dir: str) -> str:
    client = get_client()
    os.makedirs(local_dir, exist_ok=True)
    client.download(hdfs_dir, local_dir, overwrite=True)
    return local_dir


def list_dir(hdfs_dir: str) -> list[str]:
    client = get_client()
    return client.list(hdfs_dir)
