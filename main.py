from pyspark import SparkConf
from pyspark.sql import SparkSession
from task1 import task1
from task2 import task2
from task3 import task3


def main():
    spark_session = (SparkSession.builder
                                 .master("local")
                                 .appName("IMDB")
                                 .config(conf=SparkConf())
                                 .getOrCreate())
    # task1(spark_session)
    # task2(spark_session)
    print("Task3")
    task3(spark_session)


if __name__ == "__main__":
    main()
