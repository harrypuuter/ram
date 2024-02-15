from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
import datetime
import os

log = logging.getLogger(__name__)


def write_to_influxdb(url, token, org, bucket, measurement, data_dict):
    log.info("Writing data to influxdb: {}".format(data_dict))
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
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

    write_api.write(bucket, org, data_point)
    log.info("Data written to influxdb")


if __name__ == "__main__":
    data = {"field1": "value1", "field2": "value2"}
    write_to_influxdb(
        "http://localhost:8086",
        "my-token",
        "my-org",
        "my-bucket",
        "mymeasurement",
        data,
    )
