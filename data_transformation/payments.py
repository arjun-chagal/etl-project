from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, DateType, TimestampType, StringType

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




payments_bronze = spark.read.format("delta").load("../data/raw-DFs/payments")

#adding columns

"""
THIS IS WRONG:
WHY: withColumnRenamed fails silently. If _c0 doesn't exist, it will NOT throw an error. This will undermine subsequent steps (applying schema).



payments_bronze = payments_bronze.withColumnRenamed("_c0", "payment_id")
payments_bronze = payments_bronze.withColumnRenamed("_c1", "customer_id")
payments_bronze = payments_bronze.withColumnRenamed("_c2", "payment_date")
payments_bronze = payments_bronze.withColumnRenamed("_c3", "payment_status")
payments_bronze = payments_bronze.withColumnRenamed("_c4", "payment_method")

payments_schema = StructType([StructField("payment_id", IntegerType(), nullable=False),
                              StructField("customer_id", IntegerType(), nullable=False),
                              StructField("payment_date", TimestampType(), nullable=False),
                              StructField("payment_status", IntegerType(), nullable=False),
                              StructField("payment_method", StringType(), nullable=False)])

payments_bronze = spark.createDataFrame(payments_bronze.rdd, schema=payments_schema)
"""

payments_bronze.toDF()

payments_bronze.show()
payments_bronze.printSchema()











spark.stop()