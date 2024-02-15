import logging
import yaml
from JobFactory import JobFactory
from JobScheduler import JobScheduler
from JobDatabase import JobDatabase
import argparse
import htcondor

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--config-file", type=str, default="config.yml")
    parser.add_argument(
        "--influxdb-config-file", type=str, default="influx_parameters.yml"
    )
    parser.add_argument("--job-db-file", type=str, default="jobs.sqlite3")
    return parser.parse_args()


def load_influxdb_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
    return config


def setup_logging():
    logger = logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("remote-testsuite.log")],
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


def load_config_and_schedule_jobs(config_file, factory):
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


if __name__ == "__main__":
    args = parse_args()
    setup_logging()
    influx_parameters = load_influxdb_config(args.influxdb_config_file)
    htcondor_schedd = htcondor.Schedd()
    database = JobDatabase(args.job_db_file)
    factory = JobFactory(database, influx_parameters, htcondor_schedd)
    factory.pickup_jobs()

    load_config_and_schedule_jobs(args.config_file, factory)
