from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
import datetime
import os

log = logging.getLogger(__name__)


class InfluxDBWriter(object):
    def __init__(self, configuration) -> None:
        self.url = configuration["url"]
        self.token = configuration["token"]
        self.org = configuration["org"]
        self.bucket = configuration["bucket"]
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            log.info("Connected to InfluxDB")
        except Exception as e:
            raise Exception("Failed to connect to InfluxDB: {}".format(e))

    def write_to_influxdb(self, measurement, data_dict):
        log.info("Writing data to influxdb: {}".format(data_dict))
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        submission_time = datetime.datetime.fromtimestamp(
            data_dict["submission_time"], datetime.timezone.utc
        )
        hostname = os.uname().nodename
        data_point = (
            Point(measurement)
            .tag("test_name", data_dict["name"])
            .field("cluster-id", data_dict["cluster-id"])
            .field("result", int(data_dict["passed"]))
            .field("message", data_dict["message"])
            .field("runtime", int(data_dict["runtime"]))
            .field("cpu_efficiency", float(data_dict["cpu_efficiency"]))
            .field("testtime", int(data_dict["testtime"]))
            .field("site", data_dict["site"])
            .field("hostname", hostname)
            .time(
                submission_time.isoformat()
            )  # the submission time of the job from the data_dict
        )

        write_api.write(self.bucket, self.org, data_point)
        log.info("Data written to influxdb")
