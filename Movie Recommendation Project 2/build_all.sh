#!/bin/bash
docker build -t sophiezh/data-processing:v1 -f data_processing/Dockerfile.kafka data_processing/
docker build -t sophiezh/backend:v1 -f inference_service/Dockerfile.backend inference_service/
docker build -t sophiezh/svd-server:v1 -f model_training/ABTESTING/Dockerfile.ABtestserver model_training/ABTESTING/
docker build -t sophiezh/knn-server:v1 -f model_training/KNN_Server/Dockerfile.KNNserver model_training/KNN_Server/
docker build -t sophiezh/abtest-server:v1 -f model_training/SVDServer/Dockerfile.SVDserver model_training/SVDServer/


docker push sophiezh/data-processing:v1
docker push sophiezh/backend:v1
docker push sophiezh/svd-server:v1
docker push sophiezh/knn-server:v1
docker push sophiezh/abtest-server:v1

