from core import *

def task1(number):
    printIntro(number)
    df = getDataFrame("title.akas",t.StructType([
        t.StructField("titleId", t.StringType(), False),
        t.StructField("ordering", t.IntegerType(), False),
        t.StructField("title", t.StringType(), False),
        t.StructField("region", t.StringType(), False),
        t.StructField("language", t.StringType(), False),
        t.StructField("types", t.StringType(), False),
        t.StructField("attributes", t.StringType(), False),
        t.StructField("isOriginalTitle", t.IntegerType(), False)]))

    df = df.select("title", "region").where(f.col("region") == "UA")
    df.show()