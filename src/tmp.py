import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(data: dir, path: str):
    df = pd.DataFrame(data,
                      index=list('abc'))
    # create a pyarrow table object
    table = pa.Table.from_pandas(df)
    # write the table object to a single paquet file. In practice, a Parquet dataset may consist
    # of many files in many directories.
    # Note the write_table() method has a number of options, such as:
    # version: the Parquet format version to use, whether '1.0' for compatibility with older readers, or '2.0' to
    #          unlock more recent features.
    # data_page_size: to control the approximate size of encoded data pages within a column chunk. This currently
    #                 defaults to 1MB
    # flavor: to set compatibility options particular to a Parquet consumer like 'spark' for Apache Spark.
    # For the full option list, please visit below page
    # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html#pyarrow.parquet.write_table
    pq.write_table(table, path)


def read_parquet_as_table(path: str):
    # We can read a single parquet file to a parquet table object with read_table
    # note here we read only column 1 and 3.
    #
    table = pq.read_table(path, columns=['one', 'three'])
    # the print only shows the
    print(table)


def read_parquet_as_pandas_df(path: str):
    df1 = pq.read_pandas(path, columns=['one', 'two']).to_pandas()
    print(df1.head())


# we can convert a pandas dataframe to a arrow table directly
def convert_pandas_df_to_arrow_tab(data: dict, path: str):
    df = pd.DataFrame(data, index=list('abc'))
    print(f"df with index: {df.head()}")
    # as the index also take disk space, if the index is just row number, we can
    # omit the index by using preserve_index=False
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)
    df_back = pq.read_table(path).to_pandas()
    print(f"df without index: {df_back.head()}")


def finer_grained_reading(path):
    # we have seen how to use read_table to read a parquet file
    # in fact, it calls pyarrow.parquet.ParquetFile class to read parquet files.
    # This class has other features:
    # - get metadata
    # - get schema
    # - read data by individual row groups
    parquet_file = pq.ParquetFile(path)

    meta_data = parquet_file.metadata
    schema = parquet_file.schema
    total_row_groups = parquet_file.num_row_groups
    print(f"metadata: {meta_data}")
    print(f"schema: {schema}")
    print(f"total_row_groups: {total_row_groups}")

    # As we explained before, parquet file split data into multiple row groups the read_table()
    # method read all of the row groups and concatenate them into a single table.
    # With ParquetFile class, we can read individual groups with read_row_group:
    table_row_group0 = parquet_file.read_row_group(0)
    print(f"group 0: {table_row_group0}")


def finer_grained_writing(data: dict, path: str):
    # We can write a parquet file with multiple row groups
    df = pd.DataFrame(data, index=list('abc'))
    table = pa.Table.from_pandas(df, preserve_index=False)
    # write the arrow table into a parquet file with 3 row groups
    writer = pq.ParquetWriter(path, table.schema)
    for i in range(3):
        writer.write_table(table)
    writer.close()

    # check the new parquet file's row groups number
    parquet_file = pq.ParquetFile(path)
    total_row_groups = parquet_file.num_row_groups
    print(f"total row groups: {total_row_groups}")


def get_metadata(path: str):
    parquet_file = pq.ParquetFile(path)
    # you have two possible way to get the metadata, they both return an FileMetaData object
    # solution1:
    metadata1 = parquet_file.metadata

    # solution2:
    metadata2 = pq.read_metadata(path)

    # The FileMetaData object allows to inspect
    # - the metadata of the file
    # - the metadata of row groups of the file
    # - the metadata of the columns of a row group of the file

    # get the metadata of the parquet file
    print(f"General metadata: {metadata1} \n")

    # get the metadata of the row group 0
    print(f"Metadata of row group 0: {metadata1.row_group(0)}\n")

    # get the metadata of the column 0 of row group 0
    print(f"Metadata of column 0 of row group 0: {metadata1.row_group(0).column(0)}")


def main():
    file_path = 'test.parquet'
    no_index_file_path = 'test1.parquet'
    multiple_row_groups_file_path = 'test2.parquet'
    data = {'one': [-1, np.nan, 2.5],
            'two': ['foo', 'bar', 'baz'],
            'three': [True, False, True]}
    # write data to parquet file
    # write_parquet(data, file_path)

    # read parquet file and return a pyarrow table
    # read_parquet_as_table(file_path)

    # read parquet file and return a pandas df
    # read_parquet_as_pandas_df(file_path)

    # convert df to parquet without index
    # convert_pandas_df_to_arrow_tab(data, no_index_file_path)

    # read parquet file by row groups
    # finer_grained_reading(file_path)

    # write parquet file with multiple row groups
    # finer_grained_writing(data, multiple_row_groups_file_path)
    get_metadata(file_path)


if __name__ == "__main__":
    main()
