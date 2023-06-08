from core import *

TOP = 10
COUNT = 5

def task7(number):
    printIntro(number)
    
    films = getDataFrame("title.basics", t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("titleType", t.StringType(), False),
        t.StructField("primaryTitle", t.StringType(), False),
        t.StructField("originalTitle", t.StringType(), False),
        t.StructField("isAdult", t.IntegerType(), False),
        t.StructField("startYear", t.IntegerType(), False),
        t.StructField("endYear", t.IntegerType(), False),
        t.StructField("runtimeMinutes", t.IntegerType(), False),
        t.StructField("genres", t.StringType(), False)]))

    ratings = getDataFrame("title.ratings", t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("averageRating", t.DoubleType(), False),
        t.StructField("numVotes", t.IntegerType(), False)]))

    films = films.na.fill(0).filter((f.col("startYear") != 0) & (f.col("endYear") != 0))
    films = films.withColumn("decade", ((films.endYear - films.startYear) / 10 + 1).cast("int"))
    films = films.join(ratings, films.tconst == ratings.tconst, "inner")
    films = films.select("primaryTitle", "decade", "averageRating")
    films = films.withColumn("top", f.row_number().over(Window.partitionBy("decade").orderBy(f.col("averageRating").desc())))
    films = films.filter(f.col("top") <= TOP)
    films.show(TOP * COUNT)