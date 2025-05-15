
# Utilities
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Time, Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from datetime import datetime
import json


KAFKA_TOPIC = "sensor_meas"
KAFKA_SERVERS = "kafka:9092"


class JsonTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            ts = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
            return int(ts.timestamp() * 1000)
        except Exception:
            return 0


class PrintWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        window_start = context.window().start
        window_end = context.window().end
        results = []
        for element in elements:
            results.append(f"Window [{window_start} - {window_end}] | Station {key} | {element}")
        return results


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

properties = {
    'bootstrap.servers': KAFKA_SERVERS,
    'group.id': 'flink_consumer_group'
}

kafka_consumer = FlinkKafkaConsumer(
    topics=KAFKA_TOPIC,
    deserialization_schema=SimpleStringSchema(),
    properties=properties
)

watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
    .with_timestamp_assigner(JsonTimestampAssigner())

ds = env.add_source(kafka_consumer, type_info=Types.STRING()).assign_timestamps_and_watermarks(watermark_strategy)

(
    ds.key_by(lambda x: json.loads(x)["station_id"], key_type=Types.STRING())
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .process(PrintWindowFunction(), output_type=Types.STRING())
      .print()
)

env.execute("Windowed Kafka Stream")
