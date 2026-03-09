FROM python:3.11-slim

# install java (required for spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

# spark version
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# install spark
RUN curl -L https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# install pyspark python package (light dependency wrapper)
RUN pip install --no-cache-dir pyspark

WORKDIR /app

ENTRYPOINT ["python"]