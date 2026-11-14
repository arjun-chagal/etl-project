
from pyspark.sql import SparkSession, DataFrame, Window

from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, TimestampType, DateType

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

changing data to the right datatype
"""


#customers

customers_bronze = spark.read.format("delta").load("../data/raw-DFs/customers")
customers_bronze = customers_bronze.dropDuplicates()

def latest_duplicate_customer_id(df: DataFrame):
    customers_bronze_1 = df.dropna(subset=["customer_id"])
    df_dup_customer_id = customers_bronze_1.groupBy(col("customer_id")).count().filter(col("count") >= 2)
    df_dup_customer_id = customers_bronze.join(df_dup_customer_id, "customer_id", "inner")
    # Avoid: df1.join(df2, df1.id == df2.id) -> Creates duplicate 'id' columns
    # Do this instead:
    #joined_df = df1.join(df2, "id", "inner")
    window = Window.partitionBy(df_dup_customer_id.customer_id).orderBy(df_dup_customer_id.created_timestamp.desc())
    df_row_num = df_dup_customer_id.withColumn("row_number",row_number().over(window))
    df_row_num = df_row_num.filter(col("row_number") == 1)

    return df_row_num



customers_latest_duplicates = latest_duplicate_customer_id(customers_bronze)


"""
This is bringing non-key, unwanted columns from latest_duplicate_custer_id DF. 

customers_unique = customers_bronze.join(customers_latest_duplicates, ["customer_id", "created_timestamp"], "left")
"""
customers_unique = customers_bronze.join(customers_latest_duplicates.select(col("customer_id"), col("created_timestamp")), ["customer_id", "created_timestamp"], "left")


#casting columns


customers_unique = customers_unique.withColumn("customer_id", col("customer_id").cast(IntegerType()))
customers_unique = customers_unique.withColumn("created_timestamp", col("created_timestamp").cast(TimestampType()))
customers_unique = customers_unique.withColumn("date_of_birth", col("date_of_birth").cast(DateType()))
customers_silver = customers_unique.withColumn("member_since", col("member_since").cast(DateType()))


#writing customer data to silver

customers_silver.write.format("delta").mode("append").save("../data/transformed-data(silver)/customers")


spark.stop()