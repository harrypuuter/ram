import htcondor
import yaml
import time
import os
import logging
from pathlib import Path

log = logging.getLogger(__name__)


class HTCondorJob(object):
    def __init__(
        self, job_config, job_name, submission_time, schedd, configdir, workdir
    ):
        self.job_config = job_config
        self.job_name = job_name
        self.submission_dict = {}
        self.job_data_folder = Path(configdir) / self.job_name
        self.logs_folder = Path(workdir) / "logs" / self.job_name
        self.results_folder = Path(workdir) / "results" / self.job_name
        self.result_file_path = str(
            self.results_folder
            / f'id_$(Cluster)-$(Process)-{self.job_config["job"]["output_file"]}'
        )
        self.logs_folder.mkdir(parents=True, exist_ok=True)
        self.results_folder.mkdir(parents=True, exist_ok=True)
        # now parse the job config and set defaults
        self.parse_job_config()
        self.timeout = self.job_config["timeout"]
        self.site = self.job_config["site"]
        self.submission_time = submission_time
        self.cluster_id = -1
        self.runtime = -1
        self.cpu_efficiency = -1
        self.job_output = None
        self.last_event_type = None
        self.succeeded = False
        self.done_before_timeout = False
        self.schedd = schedd

    def __getstate__(self):
        state = self.__dict__.copy()
        # Don't pickle the schedd
        del state["schedd"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Add schedd back since it doesn't exist in the pickle
        self.schedd = htcondor.Schedd()

    def jobid(self):
        return f"{self.job_name}_{self.cluster_id}"

    def parse_job_config(self):
        self.submission_dict["executable"] = (
            self.job_data_folder / self.job_config["job"]["executable"]
        )
        self.submission_dict["arguments"] = self.job_config["job"]["arguments"]
        self.submission_dict["AccountingGroup"] = "sitetest"
        self.submission_dict["universe"] = self.job_config["job"]["universe"]
        self.submission_dict["should_transfer_files"] = "YES"
        self.submission_dict["when_to_transfer_output"] = "ON_EXIT_OR_EVICT"
        if "input_files" in self.job_config["job"].keys():
            self.submission_dict["transfer_input_files"] = " ".join(
                self.job_data_folder / self.job_config["job"]["input_files"]
            )
        self.submission_dict["transfer_output_files"] = self.job_config["job"][
            "output_file"
        ]
        remap_string = (
            f'"{self.job_config["job"]["output_file"]} = {self.result_file_path}"'
        )
        self.submission_dict["transfer_output_remaps"] = remap_string
        self.submission_dict["output"] = str(
            self.logs_folder / f"$(Cluster)_{self.job_config['job']['output']}"
        )
        self.submission_dict["error"] = str(
            self.logs_folder / f"$(Cluster)_{self.job_config['job']['error']}"
        )
        self.submission_dict["log"] = str(
            self.logs_folder / self.job_config["job"]["log"]
        )
        self.submission_dict["request_cpus"] = self.job_config["requirements"]["cpu"]
        self.submission_dict["request_memory"] = self.job_config["requirements"][
            "memory"
        ]
        self.submission_dict["request_disk"] = self.job_config["requirements"]["disk"]
        self.submission_dict["request_gpus"] = self.job_config["requirements"]["gpu"]
        self.submission_dict["requirements"] = self.job_config["requirements"][
            "requirements"
        ]

    def submit_job(self):
        sub_obj = htcondor.Submit(self.submission_dict)
        sub_result = self.schedd.submit(sub_obj)
        self.cluster_id = sub_result.cluster()
        log.info(
            f"Submitted job with cluster id: {self.cluster_id} and a timeout of {self.timeout} seconds"
        )

    def wait_for_job(self):
        jel = htcondor.JobEventLog(self.submission_dict["log"])
        true_timeout = self.submission_time + self.timeout
        while time.time() < true_timeout and not self.done_before_timeout:
            for event in jel.events(stop_after=300):
                if event.cluster != self.cluster_id or event.proc != 0:
                    continue
                self.last_event_type = event.type
                if event.type == htcondor.JobEventType.JOB_TERMINATED:
                    if event["TerminatedNormally"]:
                        log.info(
                            f"Job {self.cluster_id} terminated normally with return value {event['ReturnValue']}."
                        )
                        self.done_before_timeout = True
                        self.succeeded = True
                        return
                    else:
                        log.info(
                            f"Job {self.cluster_id} terminated on signal {event['TerminatedBySignal']}."
                        )
                        self.done_before_timeout = True
                        return

                elif event.type in {
                    htcondor.JobEventType.JOB_ABORTED,
                    htcondor.JobEventType.JOB_HELD,
                    htcondor.JobEventType.CLUSTER_REMOVE,
                }:
                    log.info(f"Job {self.cluster_id} aborted, held, or removed.")
                    self.done_before_timeout = True
                    return

                # We expect to see the first three events in this list, and allow
                # don't consider the others to be terminal.
                elif event.type not in {
                    htcondor.JobEventType.SUBMIT,
                    htcondor.JobEventType.EXECUTE,
                    htcondor.JobEventType.IMAGE_SIZE,
                    htcondor.JobEventType.JOB_EVICTED,
                    htcondor.JobEventType.JOB_SUSPENDED,
                    htcondor.JobEventType.JOB_UNSUSPENDED,
                    htcondor.JobEventType.FILE_TRANSFER,
                }:
                    log.info(
                        f"Job {self.cluster_id} had unexpected event: {event.type}!"
                    )
                    self.done_before_timeout = True
                    return
        else:
            log.info(
                f"Timed out waiting for job {self.cluster_id} to finish! (Timeout {self.timeout} seconds)"
            )
            # remove the job from the queue
            self.schedd.act(
                htcondor.JobAction.Remove, f"ClusterId == {self.cluster_id}"
            )
            self.done_before_timeout = False

    def report(self):
        log.debug(f"    Job: {self.job_name}")
        log.debug(f"    Cluster ID: {self.cluster_id}")
        log.debug(f"    Status: {self.last_event_type}")
        try:
            current_htcondor_status = self.schedd.query(
                constraint=f"ClusterId == {self.cluster_id}",
                projection=["ClusterId", "JobStatus"],
                limit=1,
            )
        except Exception as e:
            log.info(f"Could not get job status: {e}")
            return
        if len(current_htcondor_status) == 0:
            log.debug("     HTCondor Job status: None")
        else:
            log.debug(
                f"    HTCondor Job status: {current_htcondor_status[0]['JobStatus']}"
            )

    def is_job_still_running(self):
        # check if the job is still running on HTCondor
        try:
            jobresult = self.schedd.query(
                constraint=f"ClusterId == {self.cluster_id}",
                projection=["ClusterId", "JobStatus"],
                limit=1,
            )
        except Exception as e:
            log.info(
                f"Could not get job status: {e}, assuming job {self.cluster_id} is still running."
            )
            return True
        if len(jobresult) == 0:
            log.info(f"Job {self.cluster_id} not found on HTCondor")
            return False
        if len(jobresult) > 1:
            raise ValueError(
                "More than one job found on HTCondor this should not happen!"
            )
        if jobresult[0]["JobStatus"] == 2:
            log.info(f"Job {self.cluster_id} still running on HTCondor")
            return True
        else:
            log.info(f"Job {self.cluster_id} running on HTCondor")
            return False

    def finished_during_pickup(self):
        try:
            for job in self.schedd.history(
                constraint=f"ClusterId == {self.cluster_id}",
                projection=["ClusterId", "LastJobStatus"],
                match=1,
            ):
                log.info(f"Job {self.cluster_id} found in HTCondor history")
                log.info(f"Job {self.cluster_id} status: {job['LastJobStatus']}")
                if job["LastJobStatus"] == 4:
                    log.info(f"Job {self.cluster_id} finished successfully")
                    self.done_before_timeout = True
                    self.succeeded = True
                else:
                    log.info(f"Job {self.cluster_id} did not finish successfully")
                    self.done_before_timeout = True
                    self.succeeded = False
        except Exception as e:
            log.info(f"Could not get job status: {e}")
            self.succeeded = False

    def parse_job_output(self):
        """Each test job should retun a yaml file with the following format:

        tests:
        - name: test1
            passed: True
            message: "Test 1 passed"
        - name: test2
            passed: False
            message: "Test 2 failed"
        """
        # check if the results file exists
        resultfiles = os.listdir(self.results_folder)
        for resultfile in resultfiles:
            if str(self.cluster_id) in resultfile:
                with open(self.results_folder / resultfile) as f:
                    self.job_output = yaml.safe_load(f)
        if self.job_output is None:
            log.info(
                f"Could not find results file for {self.cluster_id} in {self.results_folder}"
            )

    def has_passed(self):
        """Check if all tests in the job output have passed"""
        if self.job_output and self.done_before_timeout and self.succeeded:
            return all(test["passed"] for test in self.job_output["tests"])
        else:
            return False

    def message(self):
        message = ""
        if not self.has_passed():
            message = "Job failed"
            if not self.done_before_timeout:
                message += " - Job timed out before finishing"
            if not self.succeeded:
                message += f" - Job did not succeed on HTCondor (last event type: {self.last_event_type})"
            if not self.job_output:
                message += " - Job did not produce any output"
            if self.job_output and not all(
                test["passed"] for test in self.job_output["tests"]
            ):
                failed_tests = [
                    test["message"]
                    for test in self.job_output["tests"]
                    if not test["passed"]
                ]
                message += f" - Tests failed: {failed_tests}"
        else:
            message = "Job succeeded"
        return message

    def get_condor_history_details(self):
        # get runtime and CPU efficiency
        history_parameters = [
            "JobStatus",
            "RemoteWallClockTime",
            "RemoteUserCpu",
            "RemoteSysCpu",
        ]
        self.history_results = {}
        try:
            for job in self.schedd.history(
                constraint=f"ClusterId == {self.cluster_id}",
                projection=history_parameters,
                match=1,
            ):
                self.history_results = job
                self.runtime = self.history_results["RemoteWallClockTime"]
                try:
                    self.cpu_efficiency = (
                        self.history_results["RemoteUserCpu"]
                        + self.history_results["RemoteSysCpu"]
                    ) / self.runtime
                except ZeroDivisionError:
                    self.cpu_efficiency = 0
        except Exception as e:
            log.info(f"Could not get job history: {e}")

    def cleanup_outputs(self):
        # clean up the results and logs folders
        deletions = [
            self.logs_folder / logfile
            for logfile in os.listdir(self.logs_folder)
            if str(self.cluster_id) in logfile
        ]
        deletions += [
            self.results_folder / resultfile
            for resultfile in os.listdir(self.results_folder)
            if str(self.cluster_id) in resultfile
        ]
        for file in deletions:
            os.remove(file)
        log.info(f"Cleaned up {len(deletions)} files for job {self.cluster_id}")
