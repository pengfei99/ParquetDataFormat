# Parquet as long term storage data format

## 1. Which one to choose?
Normally, when we evaluate a data format, we use the following basic properties. 

- Human Readable
- Compressable
- Splittable
- Complex data structure
- Schema evolution
- Columnar(for better compression and operation performence)
- Framework supportable

|Property |CSV |Json|Parquet|Avro|ORC|
|---------|----|----|-------|----|---|
|Human Readable|YES|YES|NO|NO|NO|
|Compressable|YES|YES|YES|YES|YES|
|Splittable|YES*|YES*|YES|YES|YES|
|Complex data structure|NO|YES|YES|YES|YES|
|Schema evolution|NO|NO|YES|YES|YES|
|Columnar|NO|NO|YES|NO|YES|

Note:

1. CSV is splittable when it is a raw, uncompressed file or using a splittable compression format such as BZIP2 or LZO (note: LZO needs to be indexed to be splittable!)
2. JSON has the same conditions about splittability when compressed as CSV with one extra difference. When “wholeFile” option is set to true in Spark(re: SPARK-18352), JSON is NOT splittable.

### 1.2. Operation latency evaluation for all above data formats 

We have benchmarked all above data formats for the common data operations latency such as:
- read/write
- get basic stats (min, max, avg, count)
- Random data lookup
- Filtering/GroupBy(column-wise)
- Distinct(row-wise)

For more details about the benchmark, [data_format_overview](https://github.com/pengfei99/data_format_and_optimization/blob/main/notebooks/data_format_overview.ipynb)

After the above analysis, we can say that Orc and Parquet are the best data formats for OLAP applications. They both support various compression algorithms which reduce significantly disk usage. They are both very efficient on columnar-oriented data analysis operations. 

**Parquet has better support on nested data types than Orc**. Orc loses compression ratio and analysis performance when data contains complex nested data types.

**Orc supports data update and ACID (atomicity, consistency, isolation, durability). Parquet does not**, so if you want to update a Parquet file, you need to create a new one based on the old one.

**Parquet has better interoperability** compare to Orc. Because almost all data analysis tools and framework supports parquet. Orc is only supported by Spark, Hive, Impala, MapReduce.

### 1.3 Long-term storage

Parquet is designed for long-term storage and archival purposes, meaning if you write a file today, you can expect that any system that says they can “read Parquet” will be able to read the file in 5 years or 10 years.