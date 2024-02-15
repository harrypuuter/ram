import logging
import yaml
from .JobFactory import JobFactory
from .JobScheduler import JobScheduler
from .JobDatabase import JobDatabase
import argparse
import htcondor
import shutil
from pathlib import Path

log = logging.getLogger(__name__)


def parse_args():
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
        default=Path(args.configdir) / "config.yml",
        help="Path to the configuration file for the jobs, default is configdir/config.yml",
    )
    parser.add_argument(
        "--influxdb-config-file",
        type=str,
        default=Path(args.configdir) / "influx_parameters.yml",
        help="Path to the InfluxDB configuration file, default is configdir/influx_parameters.yml",
    )
    parser.add_argument(
        "--job-db-file",
        type=str,
        default=Path(args.workdir) / "jobs.sqlite3",
        help="Path to the job database file, default is workdir/jobs.sqlite3",
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
    return parser.parse_args()


def load_influxdb_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
    return config


def setup_logging(workdir):
    logger = logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(Path(workdir) / "remote-testsuite.log"),
        ],
    )
    return logger


def get_enabled_job_config(config):
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
    with open(config_file) as f:
        config = yaml.safe_load(f)
    enabled_job_config = get_enabled_job_config(config)["jobs"]
    testjobs = [enabled_job_config[x]["name"] for x in range(len(enabled_job_config))]
    log.info("Enabled jobs: {}".format(testjobs))
    log.info(
        "Maximum number of required workers: {}".format(
            calculate_number_of_required_workers(enabled_job_config)
        )
    )
    return enabled_job_config


def load_config_and_schedule_jobs(config_file, factory):
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
    # if the configdir exists, and it is not empty, exit
    configdir = Path(configdir).absolute()
    if Path(configdir).exists() and len(list(Path(configdir).iterdir())) > 0:
        log.error(
            f"Config directory {configdir} already exists and is not empty. Exiting."
        )
        exit(1)
    log.info(
        f"Initializing tool for the first time, setting up a default config in {configdir}"
    )
    Path(configdir).mkdir(parents=True, exist_ok=True)
    default_config = Path(__file__).parent / "default_configuration"
    shutil.copytree(default_config, configdir, dirs_exist_ok=True)
    log.info(f"Default configuration copied to {configdir}")
    log.info("Please edit the configuration files to enable jobs and set parameters")
    exit(0)


def main_cli():
    # This is the main entry point for the resource-monitoring tool
    args = parse_args()
    setup_logging(args.workdir)
    if args.initialize:
        initialize_configs(args.configdir)
    # check if relevant config files exist
    for relevant_file in [args.config_file, args.influxdb_config_file, args.workdir]:
        if not relevant_file.exists():
            log.error("Not able to find {}. Exiting.".format(relevant_file.absolute()))
            exit(1)
    Path(args.workdir).mkdir(parents=True, exist_ok=True)
    influx_parameters = load_influxdb_config(args.influxdb_config_file)
    htcondor_schedd = htcondor.Schedd()
    database = JobDatabase(args.job_db_file)
    factory = JobFactory(
        database, influx_parameters, htcondor_schedd, args.configdir, args.workdir
    )
    if args.check:
        check_config(args.config_file)
        log.info("Configuration check successful. Exiting.")
        exit(0)
    factory.pickup_jobs()
    load_config_and_schedule_jobs(args.config_file, factory)


if __name__ == "__main__":
    main_cli()
