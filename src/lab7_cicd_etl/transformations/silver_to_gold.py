import dlt
from pyspark.sql.functions import *

# 1. Initialize the Target Dimension Table
dlt.create_streaming_table(
    name="hrynchuk_test.gold.dim_vehicles_gold",
    comment="SCD Type 2 Dimension table for Vehicles",
    table_properties={
        "quality": "gold"
    }
)

# 2. Apply SCD Type 2 Logic
dlt.apply_changes(
    target="hrynchuk_test.gold.dim_vehicles_gold",
    source="hrynchuk_test.silver.transport_silver",
    keys=["vehicleId"],
    sequence_by=col("ingestion_timestamp"),

    stored_as_scd_type="2",

    track_history_column_list=["lineName", "destinationName"]
)


# 3. Create the Fact Table
@dlt.table(name="hrynchuk_test.gold.fact_arrivals_gold")
def fact_arrivals_gold():
    return (
        dlt.read("hrynchuk_test.silver.transport_silver")
        .select("vehicleId", "timeToStation", "expectedArrival", "timestamp")
    )