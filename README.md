# Resource Availability Monitoring (ram) tool

This lightweight tool can be used to monitor the availability of resources and services via HTCondor. User-defined jobs are submitted in regular intervals, and their results are collected and written to an influxdb database. The service is designed to run as a systemd service, and can be configured extensively.

## Installation

The tool is installed via pip:

```bash
pip install resource-availability-monitoring
```

Afterwards, the service can be started via

```bash
ram-cli
```

## Configuration

Per default, ram comes without a configuration, however, a default configuration can be generated via

```bash
ram-cli --initialize --configdir /path/to/your/configdir
```

### Job Configuration
The configuration has to be adjusted to the user's needs. The main configuration contains a list of defined testjobs. Each testjob configuration contains the following fields:

```yaml
jobs:
  - name: "default" # Name of the testjob
    parameters:
      enabled: true # The testjob will only be executed if enabled is set to true
      description: "Default Test" # Description of the testjob
      site: "Default" # The name of the site to be monitored
      interval: 1200 # after each interval, a new job is submitted (in seconds)
      timeout: 1200 # maximum time the testjob has to finish (in seconds)
      job:
        executable: "default.sh" # The executable to be run, has to be located in <configdir>/<name of job>/
        AccountingGroup: "group" # The accounting group to be used by HTCondor
        arguments: "" # Arguments to be passed to the executable
        output_file: "job_result.yaml" # The file to which the result of the job is written
        output: "default.out" # The file to which the stdout of the job is written
        error: "default.err" # The file to which the stderr of the job is written
        log: "default.log" # The file to which the HTCondor of the job is written
        universe: "vanilla" # The universe to be used by HTCondor
        docker_image: "" # The docker image to be used by HTCondor (only if universe is set to docker)
      requirements: '' # Requirements to be passed to HTCondor
        cpu: 1 # The number of CPUs to be used by the job
        memory: 1000 # The amount of memory to be used by the job (in MB)
        disk: 100000 # The amount of disk space to be used by the job (in KB)
        gpu: 0 # The number of GPUs to be used by the job
        requirements: '' # Additional requirements to be passed to HTCondor, e.g. "OpSysMajorVer == 7"
```

In addition, the job executable has to be located in `<configdir>/<name of job>/`. The executable has to be a shell script, and has to return a yaml file with the following structure:

```yaml
tests:
  - test: "default_test"
    passed: True
    message: "default_test passed"
```

A testjob can contain multiple tests, and each test has to contain the fields `test`, `passed`, and `message`. A testjob is considered to have passed if all tests have passed and the job has finished successfully. Within the shell script, the tests can be implemented as needed, and the results have to be written to the yaml file.


### InfluxDB Configuration

The Influxdb parameters are stored in a separate configuration file, and contain the following fields:

```yaml
url: ""
token: ""
bucket: ""
org: ""
```

Set all parameters to the correct values to enable the writing of the results to the Influxdb database. If you do not want to use an Influxdb database, run the service with the `--no-influxdb` flag. This will disable the writing of the results to the Influxdb database.

## Usage

All command line options can be displayed via

```bash
ram-cli -h
usage: __main__.py [-h] --workdir WORKDIR --configdir CONFIGDIR
                   [--config-file CONFIG_FILE]
                   [--influxdb-config-file INFLUXDB_CONFIG_FILE]
                   [--job-db-file JOB_DB_FILE] [--log-file LOG_FILE]
                   [--initialize] [--check] [--no-influxdb]

optional arguments:
  -h, --help            show this help message and exit
  --workdir WORKDIR     Directory to store job results, job logs and job
                        database
  --configdir CONFIGDIR
                        Directory to store configuration files and job scripts
  --config-file CONFIG_FILE
                        Path to the configuration file for the jobs, default
                        is <configdir>/config.yml
  --influxdb-config-file INFLUXDB_CONFIG_FILE
                        Path to the InfluxDB configuration file, default is
                        <configdir>/influx_parameters.yml
  --job-db-file JOB_DB_FILE
                        Path to the job database file, default is
                        <workdir>/jobs.sqlite3
  --log-file LOG_FILE   Path to the log file, default is <workdir>/remote-
                        testsuite.log
  --initialize          Initialize the tool with default configuration
  --check               Check if the given configuration is valid and exit
  --no-influxdb         Do not write to InfluxDB, only run the jobs
```

After the configuration has been adjusted, the configuration and Influxdb parameters can be tested via

```bash
ram-cli --configdir /path/to/your/configdir --workdir /path/to/your/workdir --check
```

Recommended arguments are:

```bash
ram-cli --configdir /path/to/your/configdir --workdir /path/to/your/workdir
```

### Systemd Service

To run the service as a systemd service, some best practices should be followed. The service should be run as a dedicated user, and the configuration and workdir should be owned by this user. After the user is created, setup a python venv, where the package is installed:
```bash
python3 -m venv /path/to/your/venv
source /path/to/your/venv/bin/activate
pip3 install resource-availability-monitoring
```

The service file should be located in `/etc/systemd/system/`, and should contain the following content and be named `resource-availability-monitoring.service`:

```bash
[Unit]
Description=Resource Availability Monitoring Service
After=network.target
Wants=network-online.target
After=network-online.target

[Install]
WantedBy=multi-user.target

[Service]
Type=simple
User=ram
Group=ram
LimitNOFILE=65536
WorkingDirectory=/path/to/your/workdir
ExecStart=/path/to/your/venv/bin/python3 -m resource_availability_monitoring --configdir /path/to/your/configdir --workdir /path/to/your/workdir
Restart=on-failure
RestartSec=300s
```

After the service file has been created, the service can be started via

```bash
systemctl start resource-availability-monitoring
```
