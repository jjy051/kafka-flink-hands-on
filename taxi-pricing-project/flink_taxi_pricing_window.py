import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
  StreamTableEnvironment,
  EnvironmentSettings,
  DataTypes
)
from pyflink.table.udf import udf 
from pyflink.common import Row
from pyflink.table.expressions import lit, col 
from pyflink.table.window import Slide


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

kafka_jar_path = os.path.join(
  os.path.abspath(os.path.dirname(__file__)), "../",
  "flink-sql-connector-kafka_2.11-1.14.0.jar"
)
t_env.get_config().get_configuration().set_string(
  "pipeline.jars", f"file://{kafka_jar_path}"
)

## to add watermark time, setting watermark in source_query
source_query = """
  CREATE TABLE trips (
    VendorID INT,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count INT,
    trip_distance DOUBLE,
    RatecodeID INT,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    pickup_ts AS TO_TIMESTAMP(tpep_pickup_datetime),
    WATERMARK FOR pickup_ts AS pickup_ts - INTERVAL '5' SECOND
  ) WITH (
    'connector' = 'kafka',
    'topic' = 'taxi-trips',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'taxi-group',
    'format' = 'csv',
    'scan.startup.mode' = 'latest-offset'
  )
"""
t_env.execute_sql(source_query)

# sink_query = """
#   CREATE TABLE sink (
#     w_start TIMESTAMP(3),
#     w_end TIMESTAMP(3)
#   ) WITH (
#     'connector' = 'print'
#   )
# """
# t_env.execute_sql(sink_query)

# ### 1. sql window
# windowed = t_env.sql_query("""
#     SELECT
#         HOP_START(pickup_ts, INTERVAL '5' SECONDS, INTERVAL '10' SECONDS) AS w_start,
#         HOP_END(pickup_ts, INTERVAL '5' SECONDS, INTERVAL '10' SECONDS) AS w_end
#     FROM trips
#     GROUP BY
#         HOP(pickup_ts, INTERVAL '5' SECONDS, INTERVAL '10' SECONDS)
# """
# )
# windowed.execute_insert("sink").wait()

# ### 2. table api window
# trips = t_env.from_path("trips")
# trips = trips.window(
#                 Slide.over(lit(10).seconds).every(lit(5).seconds)\
#                     .on(col("pickup_ts")).alias("w"))\
#             .group_by(col("w"))\
#             .select(col("w").start, col("w").end)
# trips.execute_insert("sink").wait()

### 3. window with udf
# check sink advised
sink_query = """
  CREATE TABLE sink (
    w_start TIMESTAMP(3),
    w_end TIMESTAMP(3),
    expected_price DOUBLE
  ) WITH (
    'connector' = 'print'
  )
"""
t_env.execute_sql(sink_query)


@udf(result_type=DataTypes.ROW([
  DataTypes.FIELD("w_start", DataTypes.TIMESTAMP(3)),
  DataTypes.FIELD("w_end", DataTypes.TIMESTAMP(3)),
  DataTypes.FIELD("expected_price", DataTypes.DOUBLE()),
]))
def calc_price(row):
  import pickle 
  import pandas as pd 
  with open("./model.pkl", "rb") as f:
    lr = pickle.load(f)
    pickup_ts, trip_distance, w_start, w_end = row
    trip_hour = pickup_ts.hour
    df = pd.DataFrame([[trip_hour, trip_distance]], columns=['trip_hour', "trip_distance"])
    prediction = lr.predict(df)
    return Row(w_start, w_end, prediction[0])

trips = t_env.from_path("trips")
### at first, should run streaming window function before applying udf function
## otherwise, it occurs an error trying windowing streaming data after mapping udf
trips = trips.window(
                Slide.over(lit(10).seconds).every(lit(5).seconds)\
                    .on(col("pickup_ts")).alias("w"))\
            .group_by(trips.pickup_ts, trips.trip_distance, col("w"))\
            .select(trips.pickup_ts, trips.trip_distance, col("w").start, col("w").end)

trips = trips.map(calc_price).alias("w_start", "w_end", "expected_price")
trips = trips.group_by(col("w_start"), col("w_end"))\
            .select(col("w_start"), col("w_end"), trips.expected_price.sum)
trips.execute_insert("sink").wait()
