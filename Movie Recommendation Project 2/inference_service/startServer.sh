#!/bin/bash
pkill -f backend_app.py                     # keep only one server program running on bg
exec python3 backend_app.py             # run server program in bg no hang state