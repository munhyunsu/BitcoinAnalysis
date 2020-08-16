#!/bin/bash

python3 python3 main_db_builder.py --debug --type index --resume
python3 python3 main_db_builder.py --debug --type core --index dbv3-index.db --resume

