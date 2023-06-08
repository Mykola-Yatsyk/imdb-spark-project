from core import *

def task4(number):
    printIntro(number)

    actors = getDataFrame("name.basics", t.StructType([
        t.StructField("nconst", t.StringType(), False),
        t.StructField("primaryName", t.StringType(), False),
        t.StructField("birthYear", t.IntegerType(), False),
        t.StructField("deathYear", t.IntegerType(), False),
        t.StructField("primaryProfession", t.StringType(), False),
        t.StructField("knownForTitles", t.StringType(), False)]))

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

    actors_films = getDataFrame("title.principals", t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("ordering", t.IntegerType(), False),
        t.StructField("nconst", t.StringType(), False),
        t.StructField("category", t.StringType(), False),
        t.StructField("job", t.StringType(), False),
        t.StructField("characters", t.StringType(), False)]))

    actors = actors.select("primaryName", "nconst")
    actors_films = actors_films.drop("ordering", "job")
    films = films.select("tconst", "primaryTitle")
    actors = actors.join(actors_films, actors.nconst == actors_films.nconst, "inner")
    actors = actors.filter(actors.category == "actor")
    actors = actors.join(films, actors.tconst == films.tconst, "inner")
    actors = actors.drop("nconst", "tconst")
    actors.show()