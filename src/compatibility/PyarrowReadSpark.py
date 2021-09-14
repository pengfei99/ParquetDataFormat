import pyarrow.parquet as pq
import s3fs
from pyarrow import fs


def read_s3(endpoint: str, bucket_name, path):
    url = f"https://{endpoint}"
    fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': url})
    file_uri = f"{bucket_name}/{path}"
    str_info = fs.info(file_uri)
    print(str_info)
    dataset = pq.ParquetDataset(file_uri, filesystem=fs)
    table = dataset.read()
    df = table.to_pandas()
    print(f"shape of the data set: {df.shape}")


# The S3 connector of pyarrow does not work
def read_parquet_from_s3(access_key: str, secret_key: str, session_token: str, endpoint: str, bucket: str, path: str):
    s3 = fs.S3FileSystem(access_key=access_key, secret_key=secret_key, session_token=session_token,
                         endpoint_override=endpoint, region="us-east-1")
    table = pq.read_table(f"s3://{bucket}/{path}")
    df = table.to_pandas()
    print(f"shape of the data set: {df.shape}")
    print(f"df without index:\n {df.head()}")


# we can read partitioned parquet file by using
# - ParquetDataset
# - read_table
# use dataset
def read_partitioned_parquet_with_dataset(path: str):
    dataset = pq.ParquetDataset(path)
    table = dataset.read()
    df = table.to_pandas()
    print(f"shape of the data set: {df.shape}")
    print(f"df without index:\n {df.head()}")
    return table


def read_partitioned_parquet_with_read_table(path: str):
    table = pq.read_table(path)
    df = table.to_pandas()
    print(f"df without index:\n {df.head()}")
    print(f"shape of the data set: {df.shape}")
    return table

# This function writes the input table to a fs, the partition cols will split the data into small parquet files.
def write_parquet_as_partitioned_dataset(table, partition_cols, endpoint, bucket_name, path):
    url = f"https://{endpoint}"
    fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': url})
    file_uri = f"{bucket_name}/{path}"
    pq.write_to_dataset(table, root_path=file_uri, partition_cols=partition_cols, filesystem=fs)


def main():
    bucket = "pengfei"
    path = "diffusion/data_format/netflix.parquet"
    # access_key = "changeMe"
    # secret_key = "changeMe"
    # token = "changeMe"
    endpoint = "minio.lab.sspcloud.fr"
    # read_parquet_from_s3(access_key, secret_key, token, endpoint, bucket, path)

    # benchmark a remote read parquet from s3
    # t1 = time.time()
    # read_s3(endpoint, bucket, path)
    # t2 = time.time()
    # print(f"Time spent: {t2 - t1}")

    local_path = "/home/pliu/data_set/arrow/netflix"
    # read_partitioned_parquet_with_dataset(local_path)
    # read_partitioned_parquet_with_read_table(local_path)

    # write parquet to s3 as partitioned
    arrow_output_path = "diffusion/data_format/arrow_netflix.parquet"
    table = read_partitioned_parquet_with_read_table(local_path)
    partition_cols = ['rating', 'date']
    write_parquet_as_partitioned_dataset(table, partition_cols, endpoint, bucket, arrow_output_path)


if __name__ == "__main__":
    main()
