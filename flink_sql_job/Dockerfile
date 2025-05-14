FROM flink:1.17.2-scala_2.12

# Install Python and kafka-python
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    python3 -m pip install kafka-python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Add required Iceberg, AWS and Hadoop JARs for Iceberg+MinIO
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.17/1.4.2/iceberg-flink-runtime-1.17-1.4.2.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.2/iceberg-aws-1.4.2.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.5/hadoop-aws-3.3.5.jar /opt/flink/lib/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /opt/flink/lib/

# Set working directory for scripts
WORKDIR /opt/flink/scripts

# Copy Flink SQL job and topic-waiting script
COPY job.sql job.sql
COPY wait-for-kafka.py wait-for-kafka.py

# Copy core-site.xml with S3/MinIO config
COPY core-site.xml /opt/flink/conf/core-site.xml

# Run the Kafka-waiter and submit the Flink job
CMD ["python3", "wait-for-kafka.py"]


