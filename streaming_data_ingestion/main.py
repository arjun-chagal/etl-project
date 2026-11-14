
from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

builder = (
    SparkSession.builder.appName("etl-project-local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.master", "local[*]")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()






#stream customers;

schema= StructType([StructField("customer_id", IntegerType(), nullable=False),
                    StructField("customer_name", StringType()), StructField("date_of_birth", DateType()),
                    StructField("telephone"), StructField("email", StringType()), StructField("member_since",
                    DateType()), StructField("created_timestamp", TimestampType()) ])

customers_bronze_stream = (spark.readStream.
                           schema("schema").format("json")
                           .option("checkpointOption", "../data/streamed-data/checkpoint-data/customers")
                           .load("../data/landing-data/customers"))



(customers_bronze_stream.writeStream.
 format("delta").trigger(availableNow= True).outputMode("append").
 option("checkpointLocation", "../data/streamed-data/checkpoint-data/customers").
 save("../data/streamed-data/customers-streamed-bronze"))









spark.stop()















