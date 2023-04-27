from read_write import write_data, read_data
from settings import NAMES_BASICS
from pyspark.sql.functions import col, asc


def task2(spark_session):
    """
        Get the list of people’s names, who were born in the 19th century
    """
    df = read_data(spark_session, NAMES_BASICS)
    df = df.select("primaryName", "birthYear").where((col("birthYear").between(1801, 1900)))
    # df = df.withColumn("birthYear", df["birthYear"].cast(IntegerType()))
    # born_in_1800 = df.filter((col("birthYear") > 1800) & (col("birthYear") <= 1900)).select("primaryName")
    df.show()
    write_data(df, "task2.csv")
    return None



