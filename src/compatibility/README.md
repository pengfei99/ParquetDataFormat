# Compatibility 
The parquet compatibility issues are caused by different framework (e.g. spark, hive, pandas, etc.) does not
fully implement the parquet standard


# 1. Compression

# 2. Data type
## 2.1 Unsigned int
Spark(version 2.*) does not support unsigned int as parquet column type. But pyarrow does. So if you write parquet with
unsigned int with pyarrow, spark can't read it. You will get 
```text
org.apache.spark.sql.AnalysisException: Parquet type not supported: INT32 (UINT_16);
```
For more detail, please check 
- [Spark can't read a parquet file created with pyarrow](https://github.com/apache/arrow/issues/1470)
- [parquet-compatibility-with-dask-pandas-and-pyspark](https://stackoverflow.com/questions/59948321/parquet-compatibility-with-dask-pandas-and-pyspark)

This issue has been fixed in Spark 3

## 2.2 Timestamp 

# 3. Metadata 

## 3.1 Version indicateur
PyArrow uses footer metadata to determine the format version of parquet file, while parquet-mr lib 
(which is used by spark) determines version on the page level by page header type. Moreover, in ParquetFileWriter 
parquet-mr hardcodes version in footer to '1'. 

This should be corrected in spark3. 

For more details, please check [Parquet files v2.0 created by spark can't be read by pyarrow](https://issues.apache.org/jira/browse/ARROW-6057)
