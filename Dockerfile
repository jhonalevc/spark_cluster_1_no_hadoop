FROM openjdk:18.0.2.1-slim-buster as builder

ENV DEBIAN_FRONTEND=noninteractive

# Add Dependencies for PySpark

RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy


RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_VERSION=3.5.1 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz


RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz -C /opt/ && \
    rm hadoop-3.3.6.tar.gz


ENV HADOOP_HOME=/opt/hadoop-3.3.6
ENV PATH=$PATH:$HADOOP_HOME/bin


FROM builder as apache-spark

WORKDIR /opt/spark

RUN apt-get install --fix-broken

RUN apt-get install -y python-dev

COPY requirements.txt /opt/spark/requirements.txt

RUN pip3 install --upgrade pip

RUN pip3 install -r /opt/spark/requirements.txt

RUN apt-get install -y supervisor


#Import dependencies to workWith Blob Storage

RUN wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar -O /opt/spark/jars/azure-storage-8.6.6.jar

RUN wget https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.10.0/azure-storage-blob-12.10.0.jar -O /opt/spark/jars/azure-storage-blob-12.10.0.jar

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar -O /opt/spark/jars/hadoop-azure-3.3.4.jar


RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.45.v20220203/jetty-util-9.4.45.v20220203.jar -O /opt/spark/jars/jetty-util-9.4.45.jar

RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.45.v20220203/jetty-util-ajax-9.4.45.v20220203.jar -O /opt/spark/jars/jetty-util-ajax-9.4.45.jar

RUN wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-server/9.4.45.v20220203/jetty-server-9.4.45.v20220203.jar -O /opt/spark/jars/jetty-server-9.4.45.jar

#RUN chown 1000:1000 -R /opt/spark/jars/*

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master"

EXPOSE 8080 7077 7000 8888

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG



COPY start-spark.sh /

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
