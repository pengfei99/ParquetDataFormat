#!/bin/bash

bucket_name=ursa-labs-taxi-data

for i in 01 02 03
do
  echo "aws s3api get-object --bucket ${bucket_name} --key 2009/${i}/data.parquet 2009_${i}_data.parquet  --no-sign-request"
done

