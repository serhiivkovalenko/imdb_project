from pyspark import SparkConf
from pyspark.sql import SparkSession
from task1 import task1


def main():
    spark_session = (SparkSession.builder
                                 .master("local")
                                 .appName("IMDB")
                                 .config(conf=SparkConf())
                                 .getOrCreate())
    task1(spark_session)


if __name__ == "__main__":
    main()
