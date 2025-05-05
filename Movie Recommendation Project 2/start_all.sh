#!/bin/bash
# ssh -L 0.0.0.0:9092:localhost:9092 tunnel@128.2.204.215 -NT
docker run -d --add-host host.docker.internal:128.2.205.110 sophiezh/data-processing:v1

minikube kubectl -- apply -f inference_service/loadbalancer-deployment.yaml
minikube kubectl -- apply -f inference_service/loadbalancer-service.yaml
minikube kubectl -- apply -f model_training/Server_deploy/backend-deployment-1.yaml
minikube kubectl -- apply -f model_training/Server_deploy/backend-deployment-2.yaml
minikube kubectl -- apply -f model_training/Server_deploy/backend-deployment-3.yaml
minikube kubectl -- apply -f model_training/Server_deploy/backend-service-1.yaml
minikube kubectl -- apply -f model_training/Server_deploy/backend-service-2.yaml
minikube kubectl -- apply -f model_training/Server_deploy/backend-service-3.yaml