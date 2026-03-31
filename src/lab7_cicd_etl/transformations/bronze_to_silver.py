import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

tfl_schema = StructType([
    StructField("vehicleId", StringType(), True),
    StructField("lineName", StringType(), True),
    StructField("destinationName", StringType(), True),
    StructField("timeToStation", IntegerType(), True),
    StructField("expectedArrival", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])

@dlt.table(name="hrynchuk_test.silver.transport_silver")
def transport_silver():
    return (
        dlt.read_stream("hrynchuk_test.bronze.transport_stream_bronze")
        .withColumn("body_str", col("value").cast("string"))
        .withColumn("data", from_json(col("body_str"), tfl_schema))
        .select("data.*", "timestamp")
    )