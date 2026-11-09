import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession


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

def loading_customers_bronze():
    customers_1df = spark.read.format("json").load("../data/landing-data/customers")
    customers_1df.write.format("delta").mode("append").save("../data/raw-DFs/customers")

def loading_addresses_bronze():
    addresses_raw_data = spark.read.format("csv").option("delimiter", "\t").load("../data/landing-data/addresses")
    addresses_raw_data.write.mode("append").format("delta").save("../data/raw-DFs/addresses")

def loading_memberships_bronze():
    memberships_raw_df = spark.read.format("binaryFile").\
                          option("recursiveFileLookUp", True).option("pathGlobFilter", "*.png").\
                          load("../data/landing-data/memberships/*")
    memberships_raw_df.write.format("delta").mode("append").save("../data/raw-DFs/memberships")
"""
def loading_payments_bronze():
    payments_raw_df = spark.read.format("csv").load("../data/landing-data/payments")
    payments_raw_df.write.format("delta").mode("append").save("../data/raw-DFs/payments")

def loading_orders_bronze():
    orders_raw_df = spark.read.format("json").load("../data/landing-data/orders")
    orders_raw_df.write.format("delta").mode("append").save("../data/raw-DFs/orders")

"""

loading_memberships_bronze()

"""
loading_customers()
loading_addresses()
loading_payments()
loading_orders_bronze()
"""

spark.stop()
