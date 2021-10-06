# ParquetPyArrow


# 1. Parquet file format Introduction

Parquet was launched and developed by Cloudera and Twitter to serve as a column-based storage format in 2013. It's 
optimized for work with multi-column datasets. You can visit their official site https://parquet.apache.org/

**Parquet files are binary files that contain metadata about their content**. It means without reading/parsing the 
content of the file(s), we can just rely on the metadata to determine column names, compression/encodings, data types 
and even some basic statistics. **The column metadata for a Parquet file is stored at the end of the file, which 
allows for fast, one-pass writing.**

**Parquet is optimized for the Write Once Read Many (WORM) paradigm. It’s slow to write, but incredibly fast to read, 
especially when you’re only accessing a subset of the total columns. Parquet is a good choice for read-heavy workloads. 
For use cases requiring operating on entire rows of data, a format like CSV or AVRO should be used.**

## 1.1 Compression 

Because data is stored by columns, and the compression algorithms work on each column chunk. Each column chunk has 
a unique data type(e.g. int, string, etc.) and low-value entropy. This makes the compression ratio very high 
(compression algorithms perform better on data with low information entropy which is usually contained in columns).

Parquet supports the following compression algorithms:
- UNCOMPRESSED = 0;
- SNAPPY = 1;
- GZIP = 2;
- LZO = 3;
- BROTLI = 4; (Added in 2.4)
- LZ4 = 5;    (Added in 2.4)
- ZSTD = 6;   (Added in 2.4)

Snappy, Gzip, and LZO are the most commonly used ones and are supported by all implementations. LZ4 and ZSTD yield 
better results than the former three but are a rather new addition to the format, so they are not supported by 
all the tools.

## 1.2 Columnar format advantages
- Fast read, because columnar format (e.g. **predicate pushdown** and **projection pushdown**) can reduce disk I/O.
- Schema travels with the data so data is self-describing;
- Parquet are just files, it can be stored in any file system, such as GlusterFs or on top of NFS. It is easy to move, back up, and replicates
- Columnar format provides a very good compression ratio(up to 75% when used even with the compression formats like snappy).
- Low column-oriented operation (e.g. aggregations) latency when they are required on certain columns over a huge set of data;
- Parquet can be read and write using Avro API and Avro Schema(which gives the idea to store all raw data in Avro format but all processed data in Parquet);

## 1.3 Columnar format disadvantages

- The column-based design makes you think about the schema and data types;
- Does not support data modification and schematic evolution. Of course, Some implementation such as Spark knows how to merge the scheme, 
  if you change it over time (you need to specify a special option when reading). But to change something in an 
  already existing file, you can do nothing other than overwriting, except that you can add a new column.
  
## 1.4 Supported tools

Parquet is a language-neutral data format. It does not bind with any data processing framework. Many languages and 
frameworks support Parquet.
Some OLAP tools example:
- Hive
- Impala
- Pig
- Presto
- Drill
- Tajo
- HAWQ
- IBM Big SQL
- ETC.

Some data analysis framework:
- Arrow(python, R, java, etc)
- Pandas

Some distributed calculation framework example:
- MapReduce
- Spark
- Cascading
- Crunch
- Scalding
- Kite

Data Serialization Formats:
- Avro
- Thrift
- Protocol Buffer
- POJOs

# 2. Architecture of Parquet data format


![parquet_format_overview](https://raw.githubusercontent.com/pengfei99/ParquetPyArrow/main/img/parquet_format.gif)
![parquet_format_breakdown](https://raw.githubusercontent.com/pengfei99/ParquetPyArrow/main/img/breakdown-parquet-file-format.jpg)

The above figure shows the three basic components of a parquet file:
- **Header**: It contains a Magic Number, which indicates the file is in parquet format.
- **Data block**: Normally, a data block(hdfs block) contains only one **row group**. The size of **row group** is 
  the size of data block. In hdfs, it's an hdfs block. This ensures that one-row group will get one mapper to work with.
    - **Row Groups**: It splits data by rows. If a file of 900 rows contains 3 row group, each row group has 300 rows. 
                      A row groups has a list of column chunks
        - **Column Chunks**: A column chunk only contains data of one column. Each column chunk can have its own compression codec.
            * **Page**: There are three types of pages(e.g. DATA_PAGE, INDEX_PAGE, DICTIONARY_PAGE)
                * **DATA_PAGE**: It stores the value of current row group and column chunk.
                * **DICTIONARY_PAGE**: It stores the value encoding dictionary. A column chunk can't have more than one DICTIONARY_PAGE.
                * **INDEX_PAGE**: It stores the index of the current column chunk in the row group.
* **Footer**: It contains the file metadata which includes the
    * Version of the format,
    * Data schema,
    * any extra key-value pairs,
    * Metadata for columns in the file. The column metadata would be type, path, encoding, number of values, compressed size, etc.
    * A 4-byte field encoding the length of the footer metadata
    * A 4-byte magic number (PAR1)

Question: Why file meta is in the footer not in the header?
Answer: Metadata is written after the data is written, to allow for single-pass writing. Since the metadata stored in the footer, while reading a parquet file, an initial seek will be performed to read the footer metadata length and then a backward seek will be performed to read the footer metadata.

# 3. Parquet file interoperability

There are three layers:
* storage format: This layer describes how data is stored that is defined by parquet.
* object model converters: This layer is responsible for converting other object models(e.g. avro, thrift) to parquet 
  inner object model or vise versa. In parquet-mr project，it has many modules which support many object 
  models(See the figure above). Parquet uses the striping and assembly algorithm to encode its inner object model.
* object models: We can think object models define how data is represented in memory. For example, the parquet-pig 
  project aims to convert **Pig Tuple**(object model in memory) to the parquet object model and vise versa. Avro, 
  Thrift, Protocol Buffers, Hive SerDe, Pig Tuple, Spark SQL InternalRow are all object models. Parquet also provides 
  a package "org.apache.parquet.example" which demonstrates how to convert the java object model to Parquet.

![parquet_convert](https://raw.githubusercontent.com/pengfei99/ParquetPyArrow/main/img/parquet-convert.jpg)

Note that Avro, Thrift, Protocol Buffer also provide their own storage format, but Parquet does not use them，
it uses its own parquet storage format. So even if you have Avro object model in your project, when we need to write 
to disk, the parquet-mr will convert them to Parquet storage format.

# Parquet data nested encoding schemes

To encode nested columns, Parquet uses the **Dremel encoding** with **definition and repetition levels**. 

**Definition levels**: specify how many optional fields in the path for the column are defined. 
**Repetition levels**: specify at what repeated field in the path has the value repeated. 

The max definition and repetition levels can be computed from the schema (i.e. how much nesting there is). This 
defines the maximum number of bits required to store the levels (levels are defined for all values in the column).



Two encodings for the levels are supported BITPACKED and RLE. Only RLE is now used as it supersedes BITPACKED.
Details about encoding can be found here:
https://www.waitingforcode.com/apache-parquet/encodings-apache-parquet/read
## Spark parquet 
Spark implement paruqet reader and writer. It uses following default value to 

### Data organization:
- Row group (default 128)
  - Column chunks
    - Pages(default 1MB)
      - Metadata
          - min
          - max
          - count
    - repetition level
    - definition level
    - encoded value (actual data)

### Supported Compression algo 

- uncompressed 
  
- snappy, 

- gzip, 
- lzo, 
  
- brotli, 
  
- lz4, 
  
- zstd. 
  
Note that 'zstd' requires 'ZStandardCodec' to be installed before Hadoop 2.9.0, 'brotli' requires 'BrotliCodec' 
to be installed. 

Benchmark time, space.

### Supported encoding schemes

- Plain: - it's available for all types supported by Parquet. It encodes the values back to back and is used as a 
  last resort when there is no more efficient encoding for given data. The plain encoding always reserves the same 
  amount of place for given type. For instance, an int 32 bits will be always stored in 4 bytes. 
  

- RLE_dictionary: This encoding uses three techniques to encode (RLE,bit-packing,dictionary-compression). 
  RLE is an acronym from **Run-Length Encoding**. This encoding is wonderful when your column has many repeated value.
  For example a column that is categorical or binary. 
  

To demonstrate how RLE_dict encoding works, suppose we encode a column that stores country names. 

With plain encoding, we just put these string one by one. 

With RLE_dict encoding, 
1. We first build a dictionary(key:index, value: country names). 
   
2. Raw data becomes a dictionary+ data encoded by dictionary.

3. Then we apply RLE and bit-packing on the encoded data to gain more space.

Check the below figure.

![RLE_dict_encoding](https://raw.githubusercontent.com/pengfei99/ParquetPyArrow/main/img/parquet_page_encoding.PNG)


** The size of dictionary is defined with parquet.dictionary.page.size**. If the size of the generated dictionary is 
bigger than the defined size. The page will fallback to plain encoding automatically.

To avoid this happens, you can:
- increase parquet.dictionary.page.size: allows you to have bigger dictionary to host more distinct values.
- decrease row-group size: allow you to have less distinct values in one page.



# 4. Projection Pushdown

**Projection Pushdown** stands for when your query selects certain columns, it does not need to read the entire data, 
and then filter the column. The parquet format allows the query to push down the selected column name, and only read the required column.


For example, below spark sql query will push the column projection to parquet file. As a result, only the data of 
the two column will be send back to spark, not the entire data frame.

```python

data.select(col('title'),col('author')).explain()

```

This allows the minimization of data transfer between the file system and the Spark engine by eliminating unnecessary fields from the table scanning process. As all values of the same column are organized in the same column chunks, this can avoid random access on disk(compare to row-based format).
 
# 5. Predicate Pushdown with Partition Pruning

The partition pruning technique allows optimizing performance when reading directories and files from the 
corresponding file system so that only the desired files in the specified partition can be read. It will address 
to shift the filtering of data as close to the source as possible to prevent keeping unnecessary data into memory 
with the aim of reducing disk I/O.

Below, it can be observed that the partition filter push down, which is ‘library.books.title’ = ‘THE HOST’ filter 
is pushed down into the parquet file scan. This operation enables the minimization of the files and scanning of data.

```python
parquet_df.filter(col("rating")>"3").explain()
```

As in each page, we have the min, max. So for each group, we know the min, max too. For example, with below row groups
- Row_group_0: min 0, max2
- Row_group_1: min 1, max4
- Row_group_2: min 2, max5
spark will skip row_group_0, and only read data from 1, 2. This reduce I/O.



Optimization tips:

1. It does not work well on unsorted data, because, it can have all possible values in all row groups. So spark has to read
them all. Solution, sort the column that you will use predicate, so each row group will have more narrow min, max ranges.
   
2. Use the type of the column in your predicates. For example, if your column has long type, you predicate should be long
type too. If you use int, the auto conversion of type can ruin the pushdown completely.
   
3. If your predicate is equality test (e.g. rating==3). Use **parquet dictionary filtering (parquet.filter.dictionary.enabled)**
Dictionary contains all unique values of the column chunk, which is stored at the beginning of the column chunk. Use this option
   can avoid the reading of the whole column chunk. If there is no unique value of 3 of column rating. Then we can skip
   the row group.
   
4. Materialize your predicate to physical parquet folders. For example, if you partitioned your parque file with column
"rating", then you will have sub-directory such as rating=1,rating=2, etc. When you do filter, spark will only
   read the parquet file in specific directory. 
   But be careful with partitioning of a column, if it has too many distinct value, you may have thousands of small files.
   This will actually decrease the performance. We will talk about it later in section optimal parquet file size
   

## Optimize parquet file size

### Avoid too many small files.

Because reading a file causes overhead:
- Reserve/setup internal data structure
- instantiate parquet object reader
- Fetch file
- Parse parquet metadata

These overheads add up and can impact dramatically the parquet performance,

### Avoid fi






## Accelerating ORC and Parquet Reads of s3 

https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.5/bk_cloud-data-access/content/s3-orc-parquet-reads.html

## Parquet loses compression ratio after repartitioning

Context: I have a data set imported by using Sqoop ImportTool as-parquet-file using codec snappy. I just read the origin parquet file and repartition the data frame and generate a new parquet file. The size of the new parquet file went up to 200GB. But the origin was 80GB.

The data processing logic
```scala
val productDF = spark.read.parquet("/tmp/sample1")

productDF
.repartition(100)
.write.mode(org.apache.spark.sql.SaveMode.Overwrite)
.option("compression", "snappy")
.parquet("/tmp/sample2")
```



=== Cause: ===

This happened because when you call repartition(n) on a data frame **you are doing a round-robin partitioning**. Any data locality that existed before the repartitioning is gone. So the compression codecs can't compress data very well, and the run length and dictionary encoders don't really have much to work with.

=== Solution: ===

so when you do your repartition, you need to repartition your data frame base on a good column that preserves data locality.

```text
repartition (n, col)
```


Also, if you are optimizing your sqooped tables for downstream jobs you can sortWithinPartition for faster scans.
```scala
df.repartition(100, $"userId").sortWithinPartitions("userId").write.parquet(...)
```

