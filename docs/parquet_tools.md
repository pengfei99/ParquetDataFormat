# Parquet tools

The "parquet tools" is a part of the parquet project. It comes in form of a jar file. You can use it to explore the 
parquet file by using a command line.

See these pages for more details about Parquet Tools:
- https://github.com/apache/parquet-mr/tree/master/parquet-tools-deprecated
- https://mvnrepository.com/artifact/org.apache.parquet/parquet-tools


## 1. Usage

```shell
(hadoop): hadoop jar parquet-tools-*.jar COMMAND [GENERIC-OPTIONS] [COMMAND-OPTIONS] PARUQET-FILE-PATH
(local): java -jar parquet-tools-*.jar COMMAND [GENERIC-OPTIONS] [COMMAND-OPTIONS] PARUQET-FILE-PATH
```

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

## 2. Command example 

### 2.1 Cat
It prints out content for a given parquet file:
```shell
# cat a file in hdfs, -j means output in json format
hadoop jar parquet-tools.jar cat -j hdfs://pengfei.org:9000/toto.parquet

# cat a file in local fs
hadoop jar parquet-tools.jar cat --json file:///home/pliu/data_set/toto.parquet

```


### 2.2 Head

Prints out the first n records(default: 5) for a given parquet file

```shell
# you can change the default value of n by using -n
hadoop jar parquet-tools.jar head -n 10 file:///home/pliu/data_set/toto.parquet

```

### 2.3 schema

Prints the schema of parquet file
```shell
# you can get more details by using -d, it will print the meta data
hadoop jar parquet-tools.jar schema -d file:///home/pliu/data_set/toto.parquet
```

### 2.4 meta
Prints the meta data of parquet file
```shell
hadoop jar parquet-tools.jar meta file:///home/pliu/data_set/toto.parquet
```


### 2.5 dump
Prints out row groups and metadata for a given parquet file. You have the following option to choose
* -c,--column <arg>  |  Dump only the given column, can be specified more than once.
* -d,--disable-data  |  Do not dump column data.
* -m,--disable-meta  |  Do not dump row group and page metadata.
* -n,--disable-crop  |  Do not crop the output based on console width.

```shell
hadoop jar parquet-tools.jar dump file:///home/pliu/data_set/toto.parquet
```

### 2.6 merge
Merges multiple Parquet files into one Parquet file.

```shell
hadoop jar parquet-tools.jar merge file:///home/pliu/data_set/toto.parquet file:///home/pliu/data_set/titi.parquet
```


## 3. python version parquet tools

The python version does not work well.
```shell
# install the tool via pypi
pip install parquet-tools

# get help
parquet-tools --help

# show the details of the parquet file
parquet-tools show test.parquet
parquet-tools show s3://bucket-name/prefix/*

# 
parquet-tools inspect /path/to/parquet
```
