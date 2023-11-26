FROM bitnami/spark:latest

USER root

# Create the required directory and install dependencies
RUN apt-get update && \
    apt-get install -y wget

RUN mkdir -p /opt/spark/jars

ENV HADOOP_AWS_VERSION=3.2.0
ENV AWS_SDK_VERSION=1.11.375
ENV CASSANDRA_CONNECTOR_VERSION=3.1.0

RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_VERSION/aws-java-sdk-bundle-$AWS_SDK_VERSION.jar

WORKDIR /app
COPY ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

COPY ./src /app/src
