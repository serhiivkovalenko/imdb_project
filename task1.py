from read_write import write_data, read_data
from settings import TITLE_AKAS
from pyspark.sql.functions import col, asc


def task1(spark_session):
    """
    Get all titles of series/movies etc. that are available in Ukrainian.
    """
    df = read_data(spark_session, TITLE_AKAS)
    df_filter = df.select("title").where((col("region") == "UA")).orderBy("title")
    # df_filter.show()
    write_data(df_filter, "task1.csv")
    return None



