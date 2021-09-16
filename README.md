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




===== 4. Projection Pushdown =====

**Projection Pushdown** stands for when your query selects certain columns, it does not need to read the entire data, and then filter the column. The parquet format allows the query to push down the selected column name, and only read the required column.


For example, below spark sql query will push the column projection to parquet file. As a result, only the data of the two column will be send back to spark, not the entire data frame.
<code>
data.select(col('title'),col('author')).explain()
</code>

This allows the minimization of data transfer between the file system and the Spark engine by eliminating unnecessary fields from the table scanning process. As all values of the same column are organized in the same column chunks, this can avoid random access on disk(compare to row-based format).

===== 5. Predicate Pushdown with Partition Pruning =====

The partition pruning technique allows optimizing performance when reading directories and files from the corresponding file system so that only the desired files in the specified partition can be read. It will address to shift the filtering of data as close to the source as possible to prevent keeping unnecessary data into memory with the aim of reducing disk I/O.
Below, it can be observed that the partition filter push down, which is ‘library.books.title’ = ‘THE HOST’ filter is pushed down into the parquet file scan. This operation enables the minimization of the files and scanning of data.

<code>
parquet_df.filter(col("rating")=="5").explain()
</code>





==== Accelerating ORC and Parquet Reads of s3 ====

https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.5/bk_cloud-data-access/content/s3-orc-parquet-reads.html

==== Parquet loses compression ratio after repartitioning ====

Context: I have a data set imported by using Sqoop ImportTool as-parquet-file using codec snappy. I just read the origin parquet file and repartition the data frame and generate a new parquet file. The size of the new parquet file went up to 200GB. But the origin was 80GB.

The data processing logic
<code>
val productDF = spark.read.parquet("/tmp/sample1")

productDF
.repartition(100)
.write.mode(org.apache.spark.sql.SaveMode.Overwrite)
.option("compression", "snappy")
.parquet("/tmp/sample2")

</code>

=== Cause: ===

This happened because when you call repartition(n) on a data frame **you are doing a round-robin partitioning**. Any data locality that existed before the repartitioning is gone. So the compression codecs can't compress data very well, and the run length and dictionary encoders don't really have much to work with.

=== Solution: ===

so when you do your repartition, you need to repartition your data frame base on a good column that preserves data locality.

<code>
repartition (n, col)
</code>

Also, if you are optimizing your sqooped tables for downstream jobs you can sortWithinPartition for faster scans.
<code>
df.repartition(100, $"userId").sortWithinPartitions("userId").write.parquet(...)
</code>


====== Appendix: Parquet tools ======

The "parquet tools" is a part of the parquet project. It comes in form of a jar file. You can use it to explore the parquet file by using a command line.

See these pages for more details about Parquet Tools:
* https://github.com/apache/parquet-mr/tree/master/parquet-tools-deprecated
* https://mvnrepository.com/artifact/org.apache.parquet/parquet-tools


===== 2. Usage =====

* (hadoop): hadoop jar parquet-tools-*.jar COMMAND [GENERIC-OPTIONS] [COMMAND-OPTIONS] PARUQET-FILE-PATH
* (local): java -jar parquet-tools-*.jar COMMAND [GENERIC-OPTIONS] [COMMAND-OPTIONS] PARUQET-FILE-PATH

**Note that the local mode does not work for version 1.11.1.**

Available command:
* cat :  Prints out content for a given parquet file.
* head : Prints out the first n records for a given parquet file (default: 5).
* schema : Prints out the schema for a given parquet file.
* meta : Prints out metadata for a given parquet file.
* dump : Prints out row groups and metadata for a given parquet file.
* merge : Merges multiple Parquet files into one Parquet file.

Available GENERIC-OPTIONS:
* --debug     |  Enable debug output.
* -h,--help   |  Show this help string.
* --no-color  |  Disable color output even if supported.

===== 3. Command example =====

==== 3.1 Cat ====
It prints out content for a given parquet file
<code>
# cat a file in hdfs, -j means output in json format
hadoop jar parquet-tools.jar cat -j hdfs://pengfei.org:9000/toto.parquet

# cat a flie in local fs
hadoop jar parquet-tools.jar cat --json file:///home/pliu/data_set/toto.parquet
</code>

==== 3.2 Head ====

Prints out the first n records(default: 5) for a given parquet file

<code>
# you can change the default value of n by using -n
hadoop jar parquet-tools.jar head -n 10 file:///home/pliu/data_set/toto.parquet
</code>

==== 3.3 schema ====
Prints the schema of parquet file
<code>
# you can get more details by using -d, it will print the meta data
hadoop jar parquet-tools.jar schema -d file:///home/pliu/data_set/toto.parquet
</code>

==== 3.4 meta ====
Prints the meta data of parquet file
<code>
hadoop jar parquet-tools.jar meta file:///home/pliu/data_set/toto.parquet
</code>

==== 3.5 dump ====
Prints out row groups and metadata for a given parquet file. You have the following option to choose
* -c,--column <arg>  |  Dump only the given column, can be specified more than once.
* -d,--disable-data  |  Do not dump column data.
* -m,--disable-meta  |  Do not dump row group and page metadata.
* -n,--disable-crop  |  Do not crop the output based on console width.
  <code>
  hadoop jar parquet-tools.jar dump file:///home/pliu/data_set/toto.parquet
  </code>

==== 3.6 merge ====
Merges multiple Parquet files into one Parquet file.

<code>
hadoop jar parquet-tools.jar merge file:///home/pliu/data_set/toto.parquet file:///home/pliu/data_set/titi.parquet
</code>


===== 4. python version parquet tools =====

The python version does not work well.
<code>
# install the tool via pypi
pip install parquet-tools

# get help
parquet-tools --help

# show the details of the parquet file
parquet-tools show test.parquet
parquet-tools show s3://bucket-name/prefix/*

# 
parquet-tools inspect /path/to/parquet
</code>
