from core import *

TOP = 50

def task6(number):
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

    episodes = getDataFrame("title.episode", t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("parentTconst", t.StringType(), False),
        t.StructField("seasonNumber", t.IntegerType(), False),
        t.StructField("episodeNumber", t.IntegerType(), False)]))

    films = films.select("tconst", "primaryTitle")
    episodes = episodes.select("tconst", "episodeNumber")
    films = films.join(episodes, films.tconst == episodes.tconst, "inner")
    films = films.drop("tconst").orderBy(f.col("episodeNumber").desc()).limit(TOP)
    films.show(TOP)