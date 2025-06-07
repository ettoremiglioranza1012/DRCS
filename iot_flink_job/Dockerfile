FROM flink:1.17-scala_2.12

RUN apt-get update && apt-get install -y python3 python3-pip

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.17.0 boto3 pandas

RUN pip3 install redis kafka-python pyarrow numpy

COPY main.py /opt/main.py
COPY data_templates.py /opt/data_templates.py

COPY flink-connector-kafka-1.17.0.jar /opt/flink/lib/
COPY kafka-clients-3.3.2.jar /opt/flink/lib/

CMD ["python", "/opt/main.py"]
