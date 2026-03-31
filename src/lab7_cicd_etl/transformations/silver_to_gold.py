import dlt
from spark.sql.functions import *

@dlt.table(
    name="hrynchuk_test.gold.dim_vehicles",
    comment="Dimension table for vehicles with history"
)
def dim_vehicles():
    return (
        dlt.read("hrynchuk_test.silver.transport_stream_silver")
        .select("vehicleId", "lineName", "destinationName", "__start_at", "__end_at", "__is_current")
    )

@dlt.table(
    name="hrynchuk_test.gold.fact_transport_events",
    comment="Fact table containing all arrival events"
)
def fact_transport_events():
    return (
        dlt.read("hrynchuk_test.silver.transport_stream_silver")
        .select(
            "vehicleId",
            "expectedArrival",
            "timeToStation",
            col("timestamp").alias("event_time")
        )
    )

@dlt.table(
    name="hrynchuk_test.gold.agg_line_stats",
    comment="Summary of average arrival times per line"
)
def agg_line_stats():
    return (
        dlt.read("hrynchuk_test.gold.fact_transport_events")
        .groupBy("lineName")
        .agg(
            avg("timeToStation").alias("avg_time_to_station"),
            count("vehicleId").alias("total_arrivals_tracked")
        )
    )