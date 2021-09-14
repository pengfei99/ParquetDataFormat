from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.master("local[*]").appName("SparkReadPyarrow").config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.1").getOrCreate()
    path = "s3a://pengfei/diffusion/data_format/netflix.parquet"
    df = spark.read.parquet(path)
    df.show()


if __name__ == "__main__":
    main()
