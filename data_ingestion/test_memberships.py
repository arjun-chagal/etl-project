from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("x").config("spark.ui.enabled","false").config("spark.driver.host","127.0.0.1").getOrCreate()
for label, opts, path in [
  ("current: .png + /*", {"recursiveFileLookUp":"True","pathGlobFilter":".png"}, "../data/landing-data/memberships/*"),
  ("fix1: *.png + /*", {"recursiveFileLookUp":"True","pathGlobFilter":"*.png"}, "../data/landing-data/memberships/*"),
  ("fix2: no glob filter", {"recursiveFileLookUp":"True"}, "../data/landing-data/memberships/*"),
  ("fix3: *.png + folder", {"recursiveFileLookUp":"True","pathGlobFilter":"*.png"}, "../data/landing-data/memberships"),
]:
    df = spark.read.format("binaryFile")
    for k,v in opts.items(): df=df.option(k,v)
    df=df.load(path)
    print(label, "count=", df.count(), flush=True)
    df.select("path","length").show(3, truncate=False)
spark.stop()
print("DONE")
