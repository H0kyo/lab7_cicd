from pyspark.sql.functions import *
import dlt

EH_NAMESPACE = "evhgigdata"
EH_NAME = 'hrynchuk-hub'
EH_CONN_STR = dbutils.secrets.get(scope="kvbddev-scope", key="eh-conn-str-hrynchuk")

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : "evhgigdata.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 20000,
  "maxOffsetsPerTrigger"     : 1000,
  "failOnDataLoss"           : "false",
  "startingOffsets"          : "latest"
}


@dlt.table(
    name = "hrynchuk_test.bronze.transport_stream_bronze",
    comment = "Raw data from streaming data volume",
    spark_conf = {"spark.sql.caseSensitive": "true"}
)
def transport_stream_bronze():
    raw_df = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()

    return raw_df
