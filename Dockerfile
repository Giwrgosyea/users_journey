FROM openjdk:8-jre-slim-buster as builder

# Add Dependencies for PySpark
RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1
ENV SPARK_VERSION=3.0.2 \
HADOOP_VERSION=3.2 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz


FROM builder as apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master"

EXPOSE 8080 7077 7000

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar
# RUN wget https://repo1.maven.org/maven2/org/mongodb/bson/4.4.2/bson-4.4.2.jar
# RUN wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.4.2/mongodb-driver-sync-4.4.2.jar
# RUN wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.4.2/mongodb-driver-core-4.4.2.jar
# RUN wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar
RUN wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.0/postgresql-42.7.0.jar
RUN cp hadoop-aws-3.2.0.jar /opt/spark/jars/hadoop-aws-3.2.0.jar
RUN cp aws-java-sdk-bundle-1.11.375.jar /opt/spark/jars/aws-java-sdk-bundle-1.11.375.jar
# RUN cp bson-4.4.2.jar /opt/spark/jars/bson-4.4.2.jar
# RUN cp mongodb-driver-sync-4.4.2.jar /opt/spark/jars/mongodb-driver-sync-4.4.2.jar
# RUN cp mongodb-driver-core-4.4.2.jar /opt/spark/jars/mongodb-driver-core-4.4.2.jar
# RUN cp mongo-spark-connector_2.12-3.0.1.jar /opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar
RUN cp postgresql-42.7.0.jar /opt/spark/jars/postgresql-42.7.0.jar

COPY start-spark.sh /

CMD ["/bin/bash", "/start-spark.sh"]
