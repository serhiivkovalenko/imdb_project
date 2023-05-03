from read_write import write_data, read_data
from settings import TITLE_BASICS
from settings import TITLE_RATINGS
from pyspark.sql.functions import col, asc, count, desc, expr, explode, substring, floor, to_str
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def task7(spark_session):
    """
        Get 10 titles of the most popular movies/series etc. by each decade.

    """

    df = read_data(spark_session, TITLE_BASICS)
    df1 = read_data(spark_session, TITLE_RATINGS)
    spark = SparkSession.builder.appName("create_empty_dataframe").getOrCreate()

    # Создаем схему данных
    schema = StructType([
        StructField("year", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("averageRating", IntegerType(), True)
    ])

    # Создаем пустой DataFrame
    empty_df = spark.createDataFrame([], schema)
    # Показать схему DataFrame
    # empty_df.printSchema()

    for x in range(187, 203):
        s = to_str(x)
        df2 = df.join(df1, df.tconst == df1.tconst, "inner")\
              .where((floor(df.startYear.cast(IntegerType())/10) <= x) &\
                     (floor(df.endYear.cast(IntegerType())/10) >= x) | \
                     (floor(df.startYear.cast(IntegerType()) / 10) == x) & \
                     (df.endYear == "\\N")) \
                      .select(expr(s), "originalTitle", "averageRating")\
              .orderBy(col("averageRating").desc())\
              .limit(50)
        # df2.show(10)
        empty_df = empty_df.union(df2)

     # df = df.select("tconst", explode(substring("startYear", 1, 3),substring("endYear", 1, 3)).alias('year'), "originalTitle")\
    #     .join(df1, df.tconst == df1.tconst, "inner")\
    #     .select(substring("startYear", 1, 3).alias('year'), "originalTitle", "averageRating")\
    #     .groupBy("year", "originalTitle").agg(max("averageRating").alias("averageRating"))

    empty_df.show()
    write_data(empty_df, "output/task7")
    return None



