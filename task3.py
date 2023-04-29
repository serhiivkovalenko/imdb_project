from read_write import write_data, read_data
from settings import TITLE_BASICS
from pyspark.sql.functions import col, asc
from pyspark.sql.types import IntegerType


def task3(spark_session):
    """
        Get titles of all movies that last more than 2 hours
    """
    df = read_data(spark_session, TITLE_BASICS)
    df = df.select("primaryTitle", "titleType", "runtimeMinutes")\
           .where((col("runtimeMinutes").cast(IntegerType()) > 120)
                  & (col("titleType") == "movie"))
    # df = df.withColumn("birthYear", df["birthYear"].cast(IntegerType()))
    # born_in_1800 = df.filter((col("birthYear") > 1800) & (col("birthYear") <= 1900)).select("primaryName")
    df.show()
    write_data(df, "task3.csv")
    return None



