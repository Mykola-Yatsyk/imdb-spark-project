from core import *

TOP = 100

def task5(number):
    printIntro(number)
    df = getDataFrame("title.akas", t.StructType([
        t.StructField("titleId", t.StringType(), False),
        t.StructField("ordering", t.IntegerType(), False),
        t.StructField("title", t.StringType(), False),
        t.StructField("region", t.StringType(), False),
        t.StructField("language", t.StringType(), False),
        t.StructField("types", t.StringType(), False),
        t.StructField("attributes", t.StringType(), False),
        t.StructField("isOriginalTitle", t.IntegerType(), False)]))

    df = df.select("title", "region")
    df = df.groupBy("region").count()
    df = df.orderBy(f.col("count")).limit(TOP)
    print("Top 100 of them from the region with the smallest count")
    df.show(TOP)
    df = df.orderBy(f.col("count").desc()).limit(TOP)
    print("Top 100 of them from the region with the biggest count")
    df.show(TOP)