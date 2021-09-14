#!/bin/bash

export AWS_ACCESS_KEY_ID=changeMe
export AWS_SECRET_ACCESS_KEY=changeMe
export AWS_SESSION_TOKEN=changeMe
export AWS_DEFAULT_REGION=us-east-1

python src/compatibility/PyarrowReadSpark.py
# python src/compatibility/SparkReadPyarrow.py