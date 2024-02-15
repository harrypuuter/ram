import sqlite3
from .HTCondorJob import HTCondorJob
import pickle
import logging
import copy
from pathlib import Path

log = logging.getLogger(__name__)


# The JobDatabase class is used to manage a SQLite database for storing job information, including
# initialization and cleanup of old jobs.
class JobDatabase(object):
    def __init__(self, sqlite_file):
        """
        The `__init__` function initializes an object with a given SQLite file, and then calls the
        `initialize` and `cleanup_old_jobs` methods.

        Args:
          sqlite_file: The `sqlite_file` parameter is the file path or name of the SQLite database file that
        will be used for storing data.
        """
        self.sqlite_file = Path(sqlite_file).absolute()
        self.initialize()
        self.cleanup_old_jobs(retention_days=7)

    def initialize(self):
        """
        The `initialize` function checks if a SQLite database file exists, and if not, creates a new
        database with a table called "jobs".
        """
        if not self.sqlite_file.exists():
            log.info("Creating new database at {}".format(self.sqlite_file))
            conn = sqlite3.connect(self.sqlite_file)
            c = conn.cursor()
            c.execute(
                """CREATE TABLE IF NOT EXISTS jobs
                         (jobid text, status integer, submissiontime timestamp, object blob)"""
            )
            conn.commit()
            conn.close()
        else:
            log.debug("Database already exists at {}".format(self.sqlite_file))
            # self.dump_database()

    def dump_database(self):
        """
        The function `dump_database` logs a debug message, connects to a SQLite database, executes a SELECT
        query on the "jobs" table, logs the fetched results, and then closes the database connection.
        """
        log.debug("Dumping db:")
        conn = sqlite3.connect(self.sqlite_file)
        c = conn.cursor()
        c.execute("SELECT * FROM jobs")
        log.debug(c.fetchall())
        conn.close()

    def add_job(self, job: HTCondorJob, status):
        """
        The `add_job` function adds a job object to a SQLite database, storing its job ID, status,
        submission time, and a pickled version of the job object.

        Args:
          job (HTCondorJob): The `job` parameter is an instance of the `HTCondorJob` class. It represents a
        job that is being added to the database.
          status: The "status" parameter is a variable that represents the current status of the job. It
        could be a string or any other data type that represents the job's status, such as "running",
        "completed", "failed", etc.
        """
        conn = sqlite3.connect(self.sqlite_file)
        # convert the job object to a blob
        log.debug("Adding job to database: {}".format(job.jobid()))
        job_pickle = pickle.dumps(job)
        c = conn.cursor()
        c.execute(
            "INSERT INTO jobs VALUES (?, ?, ?, ?)",
            (
                job.jobid(),
                status,
                job.submission_time,
                job_pickle,
            ),
        )
        conn.commit()
        log.debug("Job added to database: {}".format(job.jobid()))
        conn.close()

    def update_job_status(self, job: HTCondorJob, status):
        """
        The function updates the status of a job in an SQLite database.

        Args:
          job (HTCondorJob): HTCondorJob - An object representing a job in HTCondor.
          status: The `status` parameter is the new status that you want to update for the given job. It
        represents the current state or progress of the job.
        """
        conn = sqlite3.connect(self.sqlite_file)
        c = conn.cursor()
        c.execute("UPDATE jobs SET status = ? WHERE jobid = ?", (status, job.jobid()))
        conn.commit()
        log.debug(f"Updated job status of {job.jobid()} to {status}")
        conn.close()

    def are_jobs_unfinished(self):
        """
        The function checks if there are any unfinished jobs in a SQLite database.

        Returns:
          a boolean value. If there are any jobs in the database with a status 1, it will
        return True. Otherwise, it will return False.
        """
        conn = sqlite3.connect(self.sqlite_file)
        c = conn.cursor()
        c.execute("SELECT * FROM jobs WHERE status == 2")
        jobs = c.fetchall()
        conn.close()
        if len(jobs) > 0:
            return True
        else:
            return False

    def get_unfinished_jobs(self):
        """
        The function retrieves unfinished jobs from a SQLite database and converts them into HTCondorJob
        objects.

        Returns:
          a list of HTCondorJob objects that represent unfinished jobs in the database.
        """
        conn = sqlite3.connect(self.sqlite_file)
        c = conn.cursor()
        c.execute("SELECT * FROM jobs WHERE status == 2")
        jobs = c.fetchall()
        conn.close()
        job_objects = []

        # need to convert the blob back to a HTCondorJob object
        for job in jobs:
            job_object = pickle.loads(job[3])
            job_objects.append(copy.deepcopy(job_object))
        return job_objects

    def get_number_of_jobs(self):
        """
        The function `get_number_of_jobs` retrieves the total number of jobs from a SQLite database.

        Returns:
          the total number of jobs in the database.
        """
        conn = sqlite3.connect(self.sqlite_file)
        c = conn.cursor()
        c.execute("SELECT * FROM jobs WHERE status >= 0 ")
        count = len(c.fetchall())
        conn.close()
        log.debug("Total number of jobs in database: {}".format(count))
        return count

    def cleanup_old_jobs(self, retention_days):
        """
        The `cleanup_old_jobs` function deletes records from a SQLite database table based on a specified
        retention period.

        Args:
          retention_days: The `retention_days` parameter specifies the number of days for which the jobs
        should be retained in the database. Any jobs that were submitted before `retention_days` days ago
        will be deleted from the `jobs` table.
        """
        conn = sqlite3.connect(self.sqlite_file)
        c = conn.cursor()
        c.execute(
            f"DELETE FROM jobs WHERE strftime('%Y-%m-%d', submissiontime) < strftime('%Y-%m-%d', datetime('now', '-{retention_days} day'))"
        )
        conn.commit()
        conn.close()
