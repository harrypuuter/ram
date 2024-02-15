import schedule
import threading
import queue
import time
import logging

log = logging.getLogger(__name__)


class JobWorker(object):
    def __init__(self, job_queue):
        self.job_queue = job_queue

    def run(self):
        while True:
            job = self.job_queue.get()
            job()
            self.job_queue.task_done()


class JobScheduler(object):
    def __init__(self, job_factory, num_workers=1):
        self.num_workers = num_workers
        self.job_queue = queue.Queue()
        self.job_factory = job_factory
        self.scheduler = schedule.Scheduler()

    def schedule_job(self, job_config, job_name, interval):
        log.info(
            "Scheduling job {} with interval {} seconds".format(job_name, interval)
        )
        self.scheduler.every(interval).seconds.do(
            self.job_queue.put, (lambda: self.job_factory.run_job(job_config, job_name))
        )

    def run(self):
        log.info("Starting {} workers".format(self.num_workers))
        for i in range(self.num_workers):
            thread = threading.Thread(target=JobWorker(self.job_queue).run)
            thread.start()
        while True:
            if self.job_factory.is_first_run():
                log.info("First run, running all jobs immediately once")
                self.scheduler.run_all()
            else:
                log.debug("First run complete, running jobs on schedule")
                self.job_factory.report()
                self.scheduler.run_pending()
            time.sleep(10)
