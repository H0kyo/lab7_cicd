from pyspark.sql.functions import *
import dlt

dlt.create_streaming_table(
    name="hrynchuk_test.silver.transport_stream_silver",
    comment="Silver table with SCD Type 2 history",
    table_properties={
        "quality": "silver"
    }
)

dlt.apply_changes(
    target = "transport_stream_silver",
    source = "hrynchuk_test.bronze.transport_stream_bronze",
    keys = ["key"],
    sequence_by = col("ingestion_timestamp"),
    ignore_null_updates = False,
    apply_as_deletes = expr("operation = 'DELETE'"),
    stored_as_scd_type = "2",
    track_history_column_list = ["lineName", "expectedArrival", "destinationName"]
)