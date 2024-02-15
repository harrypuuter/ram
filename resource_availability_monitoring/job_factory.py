from .condor_job import HTCondorJob
import time
import logging
import threading

log = logging.getLogger(__name__)


class JobFactory(object):
    def __init__(self, database, influx_writer, htcondor_schedd, configdir, workdir):
        self.database = database
        self.influx_writer = influx_writer
        self.htcondor_schedd = htcondor_schedd
        self.configdir = configdir
        self.workdir = workdir
        self.unfinished_jobs = []

    def is_first_run(self):
        return self.database.get_number_of_jobs() == 0

    def report(self):
        log.debug("JobFactory report:")
        log.debug(
            "Number of jobs in database: {}".format(self.database.get_number_of_jobs())
        )
        log.debug("Unfinished jobs: {}".format(len(self.unfinished_jobs)))
        for job in self.unfinished_jobs:
            job.report()

    def run_job(self, job_config, job_name):
        log.info("Running job {}".format(job_name))
        # create the job object
        job = HTCondorJob(
            job_config,
            job_name,
            time.time(),
            self.htcondor_schedd,
            self.configdir,
            self.workdir,
        )
        self.unfinished_jobs.append(job)
        # run the job
        job.submit_job()
        self.database.add_job(job, 2)  # 2 is the status for "Submitted"
        job.wait_for_job()
        self.collect_job_results(job)
        self.database.update_job_status(job, 1)  # 1 is the status for "Completed"
        self.unfinished_jobs.remove(job)

    def collect_job_results(self, job):
        # parse the job's output
        job.parse_job_output()
        # get job results from history
        job.get_condor_history_details()
        test_total_duration = time.time() - job.submission_time
        log.info("Job runtime: {}".format(job.runtime))
        results = self.construct_results(job, test_total_duration)
        if job.has_passed():
            log.info("Job {} has passed".format(job.job_name))
            job.cleanup_outputs()
        # write the test results to influxdb
        if self.influx_writer:
            self.influx_writer.write_to_influxdb(
                "testresults",
                results,
            )

    def pickup_jobs(self):
        log.warning("Picking up jobs")
        self.database.dump_database()
        unfinished_jobs = self.database.get_unfinished_jobs()
        if len(unfinished_jobs) == 0:
            log.info("No jobs to be picked up found in the database")
            return
        # for each job in the database, start a new thread to pick it up
        log.info("Picking up {} jobs".format(len(unfinished_jobs)))
        for job in unfinished_jobs:
            thread = threading.Thread(target=self.pickup_job, args=(job,))
            thread.start()
        return

    def pickup_job(self, job):
        self.unfinished_jobs.append(job)
        if job.is_job_still_running():
            job.wait_for_job()
        else:
            job.finished_during_pickup()
        self.collect_job_results(job)
        self.database.update_job_status(job, 1)  # 1 is the status for "Completed"

    def construct_results(self, job, test_total_duration):
        testresult = {
            "name": f"{job.job_name}",
            "cluster-id": f"{job.cluster_id}",
            "passed": job.has_passed(),
            "message": job.message(),
            "runtime": job.runtime,
            "cpu_efficiency": job.cpu_efficiency,
            "testtime": test_total_duration,
            "site": job.site,
            "submission_time": job.submission_time,
        }
        return testresult
