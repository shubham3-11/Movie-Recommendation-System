# remember to establish ssh conne
cd data_processing
./startKafkaFetch.sh
cd .. && cd model_training/Server
./startMLServer.sh
cd .. && cd .. && cd inference_service
./startServer.sh