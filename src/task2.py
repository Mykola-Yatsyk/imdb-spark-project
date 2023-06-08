from core import *

def task2(number):
    printIntro(number)
    df = getDataFrame("name.basics", t.StructType([
        t.StructField("nconst", t.StringType(), False),
        t.StructField("primaryName", t.StringType(), False),
        t.StructField("birthYear", t.IntegerType(), False),
        t.StructField("deathYear", t.IntegerType(), False),
        t.StructField("primaryProfession", t.StringType(), False),
        t.StructField("knownForTitles", t.StringType(), False)]))

    df = df.select("primaryName", "birthYear")
    df = df.na.fill(0)
    df = df.where((f.col("birthYear") >= 1800) & (f.col("birthYear") < 1900))
    df.show()