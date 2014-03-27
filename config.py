""" TODO This file serves as an example of """
import logging

DB_CONFIG = {
    "profiler_database": "test",
    "oplog_server": {
        "host": "localhost",
        "port": 27017,
    },
    "profiler_server": {
        "host": "localhost",
        "port": 27017,
    },
    "oplog_output_file": "./oplog_output_file",
    "profiler_output_file": "./profiler_output"
}

APP_CONFIG = {
    "logging_level": logging.DEBUG
}
