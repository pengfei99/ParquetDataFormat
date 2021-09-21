#!/bin/bash

bucket_name=ursa-labs-taxi-data

for i in 01 02 03 04 05 06 07 08 09 10 11 12
do
  aws s3api get-object --bucket ${bucket_name} --key 2010/${i}/data.parquet 2010_${i}_data.parquet  --no-sign-request
done

