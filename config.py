""" TODO This file serves as an example of """
import logging

DB_CONFIG = {
    # Indicates which database to record.
    "target_database": "test",
    # TODO(kailiu) not implemented yet
    # Indicates which collections to record. If user wants to capture all the
    # collections' activities, leave this field to be `None` (but we'll always
    # skip collection `system.profile`, even if it has been explicit
    # specified).
    "collections": None,
    "oplog_server": {
        "host": "localhost",
        "port": 27017,
    },
    "profiler_server": {
        "host": "localhost",
        "port": 27017,
    },
    "oplog_output_file": "./oplog_output_file",
    "profiler_output_file": "./profiler_output",
    "duration_secs": 30
}

APP_CONFIG = {
    "logging_level": logging.DEBUG
}
