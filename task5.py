from read_write import write_data, read_data
from settings import TITLE_AKAS
from settings import TITLE_BASICS
from pyspark.sql.functions import col, asc, count, desc
from pyspark.sql.types import IntegerType


def task5(spark_session):
    """
        Get information about how many adult movies/series etc. there are per
        region. Get the top 100 of them from the region with the biggest count to
        the region with the smallest one.
    """

    df = read_data(spark_session, TITLE_AKAS)
    df1 = read_data(spark_session, TITLE_BASICS)
    df = df.join(df1, df.titleId == df1.tconst, "inner")\
        .select("region", "title", "isAdult").where((col("isAdult") == 1) & (col("region") != "\\N"))\
        .groupBy(df.region).agg(count("region").alias("count_reg")).orderBy(col("count_reg").desc()).limit(100)

    # df.groupBy(df.region).agg(count("region").alias("count_reg")).orderBy(col("count_reg").desc()).limit(100)
    # select("primaryName", "characters")

    df.show()
    write_data(df, "output/task5")
    return None



