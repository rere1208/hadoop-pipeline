# Déploiement sur AWS EC2

Le pipeline complet (HDFS, YARN, Spark client, Airflow, PostgreSQL) est
gourmand en ressources : prévoir une instance avec **au moins 16 Go de
RAM et 4 vCPU**.

## 1. Lancer l'instance

- AMI : **Ubuntu Server 22.04 LTS**
- Type d'instance recommandé : **t3.xlarge** (4 vCPU / 16 Go)
  - Si budget limité : `t3.large` (2 vCPU / 8 Go) fonctionne en réduisant
    les ressources allouées à YARN dans `hadoop.env`
    (`YARN_CONF_yarn_nodemanager_resource_memory___mb`, etc.)
- Stockage : 30 Go minimum (images Docker + données HDFS)
- Security Group (inbound) :

| Port | Usage                          | Source recommandée      |
|------|--------------------------------|--------------------------|
| 22   | SSH                            | Votre IP uniquement      |
| 8080 | Airflow UI                     | Votre IP (ou 0.0.0.0/0 pour la démo) |
| 8088 | YARN ResourceManager UI        | Votre IP                 |
| 9870 | HDFS NameNode UI                | Votre IP                 |

## 2. Bootstrap

Deux options :

**A. Via user-data (automatique au lancement)**
1. Coller le contenu de `deploy/aws/user-data.sh` (déjà pointé sur
   `github.com/rere1208/hadoop-pipeline`) dans le champ "User data" lors
   de la création de l'instance EC2.
2. Attendre ~5 min puis vérifier : `docker compose ps` en SSH.
3. Le script génère des identifiants aléatoires (Fernet key, mots de
   passe Postgres/Airflow) écrits dans `.env` et affiche le mot de passe
   admin Airflow dans `/home/ubuntu/DEPLOY_CREDENTIALS.txt`.

**B. Manuellement en SSH**
```bash
ssh ubuntu@<IP_PUBLIQUE_EC2>
sudo apt-get update -y && sudo apt-get install -y docker.io docker-compose-plugin git
git clone https://github.com/rere1208/hadoop-pipeline.git
cd hadoop-pipeline
cp .env.example .env   # editer .env pour changer les mots de passe par defaut
sudo docker compose up -d
```

## 3. Initialiser le Data Lake et vérifier

```bash
./scripts/init_hdfs_dirs.sh
```

Puis ouvrir dans un navigateur :
- `http://<IP_PUBLIQUE_EC2>:9870` — NameNode HDFS
- `http://<IP_PUBLIQUE_EC2>:8088` — YARN ResourceManager
- `http://<IP_PUBLIQUE_EC2>:8080` — Airflow (login défini dans `.env`)

## 4. Arrêt / nettoyage

```bash
docker compose down          # arrete les conteneurs
docker compose down -v       # + supprime les volumes (donnees HDFS/Postgres)
```

Penser à **arrêter ou terminer l'instance EC2** après la démonstration
pour éviter des coûts inutiles.
