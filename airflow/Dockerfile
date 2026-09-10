FROM apache/airflow:2.9.3-python3.11

USER root

# Outils reseau necessaires pour le client Spark/Hadoop
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        procps \
        curl \
        postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# JRE 17 (Eclipse Temurin) telecharge directement : la base Debian de
# l'image apache/airflow ne fournit plus de paquet openjdk (ni JDK ni
# JRE) pour les anciennes versions, donc on evite toute dependance aux
# paquets apt disponibles selon la version de Debian.
ENV JAVA_HOME=/opt/java
RUN curl -fsSL --retry 3 --retry-delay 3 \
        "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jre/hotspot/normal/eclipse" \
        -o /tmp/jre.tar.gz && \
    mkdir -p ${JAVA_HOME} && \
    tar -xzf /tmp/jre.tar.gz -C ${JAVA_HOME} --strip-components=1 && \
    rm /tmp/jre.tar.gz
ENV PATH=$JAVA_HOME/bin:$PATH

# Client Spark (build Hadoop 3, compatible avec le cluster bde2020
# hadoop3.2.1 pour les operations client de base utilisees ici) : permet
# spark-submit --master yarn sans faire tourner de cluster Spark dedie,
# en soumettant directement au ResourceManager du cluster Hadoop.
# Telecharge depuis le miroir rapide dlcdn.apache.org (une version EOL
# comme 3.2.x ne s'y trouve plus et n'est servie que par l'archive
# officielle archive.apache.org, tres lente pour de gros fichiers) : on
# reste donc sur une version 3.5.x activement mirroree.
ENV SPARK_VERSION=3.5.9
ENV SPARK_HOME=/opt/spark
RUN curl -fsSL --retry 3 --retry-delay 3 \
        https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
        -o /tmp/spark.tgz && \
    mkdir -p ${SPARK_HOME} && \
    tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1 && \
    rm /tmp/spark.tgz
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PYTHONPATH=/opt/pipeline:$PYTHONPATH
