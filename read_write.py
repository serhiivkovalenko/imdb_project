from pathlib import Path


def read_data(spark_session, file_path):

    return (spark_session.read.csv(file_path,
                                   header=True,
                                   sep='\t'))


def write_data(df, file_path):
    folder = Path('output')
    folder.mkdir(parents=True, exist_ok=True)
    file_name = Path(folder, file_path)
    df.toPandas().to_csv(file_name, index=False)
    # df.write.format("csv").mode('overwrite').save('output/task1')
