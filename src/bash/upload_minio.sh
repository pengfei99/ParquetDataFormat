#!/bin/bash
minio_path=s3/pengfei/diffusion/data_format/ny_taxis/

for i in 01 02 03
do
   echo "mc cp 2009_${i}_data.parquet ${minio_path}"
done
