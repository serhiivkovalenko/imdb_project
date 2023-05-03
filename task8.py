from read_write import write_data, read_data
from settings import TITLE_BASICS
from settings import TITLE_RATINGS
from pyspark.sql.functions import col, asc, max, desc, expr, explode, substring, split
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def task8(spark_session):
    """
        Get 10 titles of the most popular movies/series etc. by each genre.
        Получите 10 наименований самых популярных фильмов/сериалов и т. д. в каждом жанре.
    """

    df = read_data(spark_session, TITLE_BASICS)
    df1 = read_data(spark_session, TITLE_RATINGS)
    # df.printSchema()
    df = df.select("tconst", explode(split(df.genres, ",")).alias("genre"), "originalTitle")

    df = df.join(df1, df.tconst == df1.tconst, "inner")\
        .select("genre",  "originalTitle", "AverageRating")\
        .groupBy("genre", "originalTitle").agg(max("AverageRating").alias("averageRating")) \
        .orderBy(col("genre"), col("averageRating").desc())

    df.printSchema()

    windowSpec = Window.partitionBy("genre").orderBy("genre")
    df = df.withColumn("row_number", row_number().over(windowSpec)).orderBy("genre", col("AverageRating").desc())
    # df.show(30)
    df = df.select("genre",  "originalTitle", "AverageRating")\
         .where(df.row_number <= 10)

    df.show()
    write_data(df, "output/task8")
    return None




