
from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip
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

"""
transforming data in silver layer:

removing null, keeping the latest data (deduplication)


"""





"""
print("\n loading bronze addresses \n")
(spark.read.format("delta").option("header", True).load("../data/raw-DFs/addresses")).show()

print("\n loading bronze customers \n")
(spark.read.format("delta").load("../data/raw-DFs/customers")).show()
"""
"""
print("\n loading bronze memberships \n")
(spark.read.format("delta").load("../data/raw-DFs/memberships")).show()

print("\n loading bronze orders \n")
(spark.read.format("delta").load("../data/raw-DFs/orders")).show()

print("\n loading bronze payments \n")
(spark.read.format("delta").load("../data/raw-DFs/payments")).show()
"""

spark.stop()