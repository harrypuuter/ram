import logging
import yaml
from .job_factory import JobFactory
from .job_scheduler import JobScheduler
from .job_database import JobDatabase
from .influx_db_writer import InfluxDBWriter
import argparse
import htcondor
import shutil

# The line `from pathlib import Path` is importing the `Path` class from the `pathlib` module. The
# `Path` class provides an object-oriented interface for working with file and directory paths. It
# allows you to manipulate paths in a platform-independent way, regardless of the operating system.
from pathlib import Path

log = logging.getLogger(__name__)


def parse_args():
    """
    The `parse_args` function is used to parse command line arguments and return the parsed arguments.

    Returns:
      The function `parse_args()` returns the parsed command-line arguments as an `argparse.Namespace`
    object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--workdir",
        type=str,
        default=Path("."),
        help="Directory to store job results, job logs and job database",
    )
    parser.add_argument(
        "--configdir",
        type=str,
        default=Path("job_configuration"),
        help="Directory to store configuration files and job scripts",
    )
    args, unknown = parser.parse_known_args()
    parser.add_argument(
        "--config-file",
        type=str,
        default=Path(args.configdir).absolute() / "config.yml",
        help="Path to the configuration file for the jobs, default is configdir/config.yml",
    )
    parser.add_argument(
        "--influxdb-config-file",
        type=str,
        default=Path(args.configdir).absolute() / "influx_parameters.yml",
        help="Path to the InfluxDB configuration file, default is configdir/influx_parameters.yml",
    )
    parser.add_argument(
        "--job-db-file",
        type=str,
        default=Path(args.workdir).absolute() / "jobs.sqlite3",
        help="Path to the job database file, default is workdir/jobs.sqlite3",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=Path(args.workdir).absolute() / "remote-testsuite.log",
        help="Path to the log file, default is workdir/remote-testsuite.log",
    )

    parser.add_argument(
        "--initialize",
        action="store_true",
        default=False,
        help="Initialize the tool with default configuration",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        default=False,
        help="Check if the given configuration is valid and exit",
    )
    parser.add_argument(
        "--no-influxdb",
        action="store_true",
        default=False,
        help="Do not write to InfluxDB, only run the jobs",
    )
    return parser.parse_args(unknown)


def load_yaml(config_file):
    """
    The function `load_yaml` reads a YAML file and returns its contents as a Python dictionary.

    Args:
      config_file: The `config_file` parameter is a string that represents the file path of the YAML
    configuration file that you want to load.

    Returns:
      the `config` object, which is the result of loading the YAML file.
    """
    with open(config_file) as f:
        config = yaml.safe_load(f)
    return config


def setup_logging(log_file):
    """
    The function `setup_logging` sets up logging in Python, with logs being written to both the console
    and a specified log file.

    Args:
      log_file: The `log_file` parameter is the path to the log file where the log messages will be
    written.

    Returns:
      the logger object.
    """
    logger = logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file),
        ],
    )
    return logger


def get_enabled_job_config(config):
    """
    The function `get_enabled_job_config` filters out disabled jobs from a given configuration and
    returns the updated configuration.

    Args:
      config: The `config` parameter is a dictionary that contains information about jobs. Each job is
    represented as a dictionary within the "jobs" key of the `config` dictionary. Each job dictionary
    contains a "parameters" key, which is also a dictionary. The "parameters" dictionary contains a
    key-value pair

    Returns:
      an updated configuration dictionary that only includes the jobs that have the "enabled" parameter
    set to True.
    """
    updated_config = {"jobs": []}
    for job in config["jobs"]:
        if job["parameters"]["enabled"]:
            updated_config["jobs"].append(job)
    if len(updated_config["jobs"]) == 0:
        raise ValueError("No jobs enabled in config")
    return updated_config


def calculate_number_of_required_workers(config):
    nworkers = sum(
        [job["parameters"]["timeout"] / job["parameters"]["interval"] for job in config]
    )
    return int(nworkers + 1)


def check_config(config_file):
    """
    The function `check_config` loads a YAML configuration file, retrieves the enabled job
    configurations, logs the enabled jobs and the maximum number of required workers, and returns the
    enabled job configurations.

    Args:
      config_file: The `config_file` parameter is the path to a YAML configuration file.

    Returns:
      the enabled job configuration, which is a list of dictionaries containing information about the
    enabled jobs.
    """
    config = load_yaml(config_file)
    enabled_job_config = get_enabled_job_config(config)["jobs"]
    testjobs = [enabled_job_config[x]["name"] for x in range(len(enabled_job_config))]
    log.info("Enabled jobs: %s", testjobs)
    log.info(
        "Maximum number of required workers: %s",
        calculate_number_of_required_workers(enabled_job_config),
    )
    return enabled_job_config


def load_config_and_schedule_jobs(config_file, factory):
    """
    The function loads a configuration file, checks the configuration, creates a job scheduler,
    schedules jobs based on the configuration, and runs the scheduler.

    Args:
      config_file: The `config_file` parameter is the file path or name of the configuration file that
    contains the job configurations. This file is used to determine which jobs should be enabled and
    their respective parameters.
      factory: The "factory" parameter is an object or function that is responsible for creating and
    managing the jobs. It could be a class that implements the necessary methods for creating and
    running jobs, or it could be a function that takes in the job configuration and returns a job
    object. The specific implementation of the factory
    """
    enabled_job_config = check_config(config_file)
    # start a job scheduler
    scheduler = JobScheduler(
        factory, num_workers=calculate_number_of_required_workers(enabled_job_config)
    )
    for index, job in enumerate(enabled_job_config):
        job_config = enabled_job_config[index]["parameters"]
        job_name = enabled_job_config[index]["name"]
        interval = job_config["interval"]
        scheduler.schedule_job(job_config, job_name, interval)
    scheduler.run()


def initialize_configs(configdir):
    """
    The function initializes a configuration directory by copying a default configuration and prompting
    the user to edit the configuration files.

    Args:
      configdir: The `configdir` parameter is the directory where the configuration files will be
    initialized.
    """
    # if the configdir exists, and it is not empty, exit
    configdir = Path(configdir).absolute()
    if Path(configdir).exists() and len(list(Path(configdir).iterdir())) > 0:
        log.error(
            "Config directory %s already exists and is not empty. Exiting.", configdir
        )
        exit(1)
    log.info(
        "Initializing tool for the first time, setting up a default config in %s",
        configdir,
    )
    Path(configdir).mkdir(parents=True, exist_ok=True)
    default_config = Path(__file__).parent / "default_configuration"
    shutil.copytree(default_config, configdir, dirs_exist_ok=True)
    log.info("Default configuration copied to %s", configdir)
    log.info("Please edit the configuration files to enable jobs and set parameters")
    exit(0)


def main_cli():
    """
    The `main_cli()` function is the main entry point for a resource-monitoring tool that initializes
    configurations, checks for relevant files, creates necessary directories, sets up an InfluxDB
    writer, creates a HTCondor Schedd object, initializes a job database, creates a job factory, and
    then checks the configuration and schedules jobs.
    """
    # This is the main entry point for the resource-monitoring tool
    args = parse_args()
    setup_logging(args.log_file)
    if args.initialize:
        initialize_configs(args.configdir)
    # check if relevant config files exist
    for relevant_file in [args.config_file, args.influxdb_config_file]:
        if not Path(relevant_file).absolute().exists():
            log.error("Not able to find %s. Exiting.", Path(relevant_file).absolute())
            exit(1)
    Path(args.workdir).mkdir(parents=True, exist_ok=True)
    if not args.no_influxdb:
        influx_writer = InfluxDBWriter(load_yaml(args.influxdb_config_file))
    else:
        influx_writer = None
    htcondor_schedd = htcondor.Schedd()
    database = JobDatabase(args.job_db_file)
    factory = JobFactory(
        database, influx_writer, htcondor_schedd, args.configdir, args.workdir
    )
    if args.check:
        check_config(args.config_file)
        log.info("Configuration check successful. Exiting.")
        exit(0)
    factory.pickup_jobs()
    load_config_and_schedule_jobs(args.config_file, factory)


# The `if __name__ == "__main__":` block is a common Python idiom that allows a module to be run as a
# standalone script or imported as a module.
if __name__ == "__main__":
    main_cli()
