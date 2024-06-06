#!/bin/bash

mkdir data
parquet_url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
wget "$parquet_url" -O data/yellow_tripdata_2022-01.parquet
