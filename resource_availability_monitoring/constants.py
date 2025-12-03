"""Constants used throughout the resource availability monitoring package."""

# Job status codes
JOB_STATUS_COMPLETED = 1
JOB_STATUS_SUBMITTED = 2

# Default values
DEFAULT_CONFIG_FILE = "config.yml"
DEFAULT_INFLUX_CONFIG_FILE = "influx_parameters.yml"
DEFAULT_JOB_DB_FILE = "jobs.sqlite3"
DEFAULT_LOG_FILE = "resource-monitoring.log"

# Database
DB_RETENTION_DAYS = 7

# Job event log parameters
JOB_EVENT_LOG_STOP_AFTER = 300

# Scheduler parameters
SCHEDULER_SLEEP_INTERVAL = 10
