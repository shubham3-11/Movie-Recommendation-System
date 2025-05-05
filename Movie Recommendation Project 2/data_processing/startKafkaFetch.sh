#!/bin/bash
# ssh -L 0.0.0.0:9092:localhost:9092 tunnel@128.2.204.215 -NT
pkill -f kafka_consumer.py          # keep only one kafka consumer program running on bg
exec python3 kafka_consumer.py   # run kafka consumer program in bg no hang state