#!/bin/bash
minio_path=s3/pengfei/diffusion/data_format/ny_taxis/parquet/raw_2011_2012/

for i in 01 02 03 04 05 06 07 08 09 10 11 12
do
   mc cp 2012_${i}_data.parquet ${minio_path}
done
