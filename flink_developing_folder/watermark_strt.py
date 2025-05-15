from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Time, Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.common.watermark_strategy import WatermarkStrategy

from datetime import datetime
import json

# Event time extractor
def extract_event_time(record):
    data = json.loads(record)
    ts = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S")
    return int(ts.timestamp() * 1000)  # in milliseconds

# Aggregation function
class AvgTempPerMinute(ProcessWindowFunction):

    def process(self, key, context, elements, out):
        count = 0
        total_temp = 0.0
        for record in elements:
            data = json.loads(record)
            total_temp += data["measurements"]["temperature_c"]
            count += 1
        avg_temp = total_temp / count if count > 0 else 0
        out.collect(f"Station {key} | Avg Temp: {avg_temp:.2f}°C | Count: {count}")

# Setup
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

# Dummy input or Kafka
ds = env.from_collection([
    json.dumps({
        "station_id": "S_A5-M40_074",
        "timestamp": "2025-05-15T16:15:55",
        "measurements": {"temperature_c": 35.5}
    }),
    json.dumps({
        "station_id": "S_A5-M40_074",
        "timestamp": "2025-05-15T16:16:10",
        "measurements": {"temperature_c": 36.1}
    })
], type_info=Types.STRING())

# Apply watermark strategy
watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Time.seconds(5)) \
    .with_timestamp_assigner(lambda record, ts: extract_event_time(record))

# Apply watermarks
ds = ds.assign_timestamps_and_watermarks(watermark_strategy)

# Key by station, 1-minute tumbling window, average temp
ds.key_by(lambda x: json.loads(x)["station_id"]) \
  .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
  .process(AvgTempPerMinute(), output_type=Types.STRING()) \
  .print()

env.execute("1 Minute Tumbling Window - Event Time")
