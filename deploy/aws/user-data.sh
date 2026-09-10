#!/usr/bin/env bash
# EC2 user-data (Ubuntu 22.04) : installe Docker + Docker Compose plugin,
# clone le repo et demarre le pipeline. A coller dans "User data" au
# lancement de l'instance, ou executer manuellement en SSH.
set -euo pipefail

REPO_URL="https://github.com/rere1208/hadoop-pipeline.git"
REPO_DIR="hadoop-pipeline"

apt-get update -y
apt-get install -y ca-certificates curl gnupg git

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

usermod -aG docker ubuntu

if [ ! -d "/home/ubuntu/${REPO_DIR}" ]; then
  sudo -u ubuntu git clone "${REPO_URL}" "/home/ubuntu/${REPO_DIR}"
fi

apt-get install -y python3

cd "/home/ubuntu/${REPO_DIR}"
if [ ! -f .env ]; then
  cp .env.example .env
  FERNET_KEY=$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
  DWH_PW=$(python3 -c "import secrets;print(secrets.token_urlsafe(16))")
  AIRFLOW_PW=$(python3 -c "import secrets;print(secrets.token_urlsafe(16))")
  ADMIN_PW=$(python3 -c "import secrets;print(secrets.token_urlsafe(12))")
  sed -i "s|^AIRFLOW__CORE__FERNET_KEY=.*|AIRFLOW__CORE__FERNET_KEY=${FERNET_KEY}|" .env
  sed -i "s|^DWH_POSTGRES_PASSWORD=.*|DWH_POSTGRES_PASSWORD=${DWH_PW}|" .env
  sed -i "s|^AIRFLOW_POSTGRES_PASSWORD=.*|AIRFLOW_POSTGRES_PASSWORD=${AIRFLOW_PW}|" .env
  sed -i "s|^AIRFLOW_ADMIN_PASSWORD=.*|AIRFLOW_ADMIN_PASSWORD=${ADMIN_PW}|" .env
  chown ubuntu:ubuntu .env
  chmod 600 .env
  echo "Identifiants generes dans .env (mot de passe Airflow admin: ${ADMIN_PW})" > /home/ubuntu/DEPLOY_CREDENTIALS.txt
  chown ubuntu:ubuntu /home/ubuntu/DEPLOY_CREDENTIALS.txt
fi

docker compose up -d
