from read_write import write_data, read_data
from settings import TITLE_PRINCIPALS
from settings import NAMES_BASICS
from pyspark.sql.functions import col, asc
from pyspark.sql.types import IntegerType


def task4(spark_session):
    """
        Get names of people, corresponding movies/series and characters they
        played in those films.
    """

    df = read_data(spark_session, NAMES_BASICS)
    df1 = read_data(spark_session, TITLE_PRINCIPALS)
    # df = df.join(df, df.nconst == df1.nconst, "inner").select("primaryName", "characters")
    df = df.join(df1, df.nconst == df1.nconst, "inner").select("primaryName", "characters").distinct()

    # df = df.withColumn("birthYear", df["birthYear"].cast(IntegerType()))
    # born_in_1800 = df.filter((col("birthYear") > 1800) & (col("birthYear") <= 1900)).select("primaryName")
    df.show()
    write_data(df, "output/task4")
    return None



