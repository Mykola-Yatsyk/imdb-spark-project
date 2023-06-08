from core import *

def task3(number):
    printIntro(number)
    df = getDataFrame("title.basics", t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("titleType", t.StringType(), False),
        t.StructField("primaryTitle", t.StringType(), False),
        t.StructField("originalTitle", t.StringType(), False),
        t.StructField("isAdult", t.IntegerType(), False),
        t.StructField("startYear", t.IntegerType(), False),
        t.StructField("endYear", t.IntegerType(), False),
        t.StructField("runtimeMinutes", t.IntegerType(), False),
        t.StructField("genres", t.StringType(), False)]))

    df = df.select("primaryTitle", "titleType", "runtimeMinutes")
    df = df.na.fill(0)
    df = df.where((f.col("runtimeMinutes") > 120) & (f.col("titleType") == "movie"))
    df.show()