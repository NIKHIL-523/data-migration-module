FROM eclipse-temurin:17-jre-jammy

ARG SPARK_VERSION=3.5.5
ARG SPARK_TARBALL=spark-${SPARK_VERSION}-bin-hadoop3-scala2.13.tgz
ARG APACHE_MIRROR=https://archive.apache.org/dist/spark

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl python3 python3-pip ca-certificates procps \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL "${APACHE_MIRROR}/spark-${SPARK_VERSION}/${SPARK_TARBALL}" -o /tmp/spark.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3-scala2.13 /opt/spark \
 && rm /tmp/spark.tgz \
 && mkdir -p /opt/spark/jars-extra

ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    SPARK_CLASSPATH=/opt/spark/jars-extra/*

CMD ["tail", "-f", "/dev/null"]
