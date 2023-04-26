from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t


def main():
    spark_session = (SparkSession.builder
                                 .master("local")
                                 .appName("IMDB")
                                 .config(conf=SparkConf())
                                 .getOrCreate())

    data = [('a', 1), ('b', 2)]
    schema = t.StructType([
        t.StructField("name", t.StringType(), True),
        t.StructField("age", t.IntegerType(), True)
    ])

    df = spark_session.createDataFrame(data, schema)
    df.show()

    # movies_df = spark_session.read.csv(path)


if __name__ == "__main__":
    main()
