FROM openjdk:8-jdk-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    ssh \
    rsync \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/local/openjdk-8
ENV HADOOP_VERSION=3.3.4
ENV SPARK_VERSION=3.4.0
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin

# Download and install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz \
    && tar -xzf hadoop-$HADOOP_VERSION.tar.gz \
    && mv hadoop-$HADOOP_VERSION $HADOOP_HOME \
    && rm hadoop-$HADOOP_VERSION.tar.gz

# Download and install Spark
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && mv spark-$SPARK_VERSION-bin-hadoop3 $SPARK_HOME \
    && rm spark-$SPARK_VERSION-bin-hadoop3.tgz

# Download and install Pig
RUN wget https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz \
    && tar -xzf pig-0.17.0.tar.gz \
    && mv pig-0.17.0 /opt/pig \
    && rm pig-0.17.0.tar.gz

ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$PIG_HOME/bin

# Download MongoDB Hadoop Connector
RUN wget https://repo1.maven.org/maven2/org/mongodb/mongo-hadoop/mongo-hadoop-core/2.0.2/mongo-hadoop-core-2.0.2.jar \
    -O $HADOOP_HOME/share/hadoop/common/lib/mongo-hadoop-core-2.0.2.jar \
    && wget https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.12.11/mongodb-driver-3.12.11.jar \
    -O $HADOOP_HOME/share/hadoop/common/lib/mongodb-driver-3.12.11.jar

# Configure SSH
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys

# Copy configuration files
COPY config/* $HADOOP_HOME/etc/hadoop/

# Create necessary directories
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/logs

# Copy startup script
COPY start-services.sh /start-services.sh
RUN chmod +x /start-services.sh

EXPOSE 9870 8020 8080 4040 9000

CMD ["/start-services.sh"]