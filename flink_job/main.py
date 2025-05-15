
# Utilities
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_consumer = FlinkKafkaConsumer(
        topics='sensor_meas',  # <--- Sostituisci con il nome del topic Kafka (es. "test-topic")
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'kafka:9092', 
            'group.id': 'flink-consumer-group'
        }
    )

    stream = env.add_source(kafka_consumer).name("Kafka Source")
    stream.print()  # stampa i messaggi Kafka nel log

    env.execute("Read from Kafka")

if __name__ == '__main__':
    main()

