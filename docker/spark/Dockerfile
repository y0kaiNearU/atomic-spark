FROM eclipse-temurin:21-jdk-noble

ARG PYSPARK_VERSION=4.1.4
ARG ICEBERG_VERSION=1.12.0
ARG SPARK_MINOR=4.1

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-venv curl \
 && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}" requests pytest

# Bake the Iceberg runtime into PySpark's jar dir so runs need no Ivy downloads.
RUN JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
 && curl -fsSL -o "$JARS/iceberg-spark-runtime.jar" \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MINOR}_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MINOR}_2.13-${ICEBERG_VERSION}.jar"

ENV PYSPARK_PYTHON=python
