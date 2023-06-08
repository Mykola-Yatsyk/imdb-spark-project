import findspark
import pyspark.sql.types as t
import pyspark.sql.functions as f

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window

findspark.init()

PATH = "../imdb-data/"
TYPE = ".tsv.gz"
SPARK_SESSION = (SparkSession.builder
                             .master("local")
                             .appName("Solution tasks by Grid Dynamics")
                             .config(conf=SparkConf())
                             .getOrCreate())

def printIntro(number):
    print("====================> Task "+str(number)+" <====================")

def getDataFrame(fileName, struct):
    return SPARK_SESSION.read.csv(PATH+fileName+TYPE, schema=struct, header=True, sep="\t", encoding="CP1251")

def saveDataFrame(path, df):
    df.write.csv(path, header=True)