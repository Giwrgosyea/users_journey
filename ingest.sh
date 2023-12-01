#!/bin/bash

s3cmd --config minio.s3cfg mb s3://data 
s3cmd --config minio.s3cfg put data/dataset2.csv s3://data 
s3cmd --config minio.s3cfg put data/dataset1.csv s3://data  