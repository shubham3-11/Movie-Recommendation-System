#!/bin/bash
pkill -f SVD_model_app.py                     # keep only one server program running on bg
exec python3 SVD_model_app.py              # run server program in bg no hang state