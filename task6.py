from read_write import write_data, read_data
from settings import TITLE_BASICS
from settings import TITLE_EPISODE
from pyspark.sql.functions import col, asc, count, desc
from pyspark.sql.types import IntegerType


def task6(spark_session):
    """
        Get information about how many episodes in each TV Series. Get the top
        50 of them starting from the TV Series with the biggest quantity of
        episodes.

    """

    df = read_data(spark_session, TITLE_BASICS)
    df1 = read_data(spark_session, TITLE_EPISODE)

    df = df.join(df1, df.tconst == df1.parentTconst, "inner")\
        .select("originalTitle", "titleType").where((col("titleType") == "tvSeries")) \
        .groupBy(df.originalTitle).agg(count("originalTitle").alias("count_Series"))\
        .orderBy(col("count_Series").desc()).limit(50)

    df.show()
    write_data(df, "output/task6")
    return None



