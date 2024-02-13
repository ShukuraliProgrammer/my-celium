from datetime import timedelta
from google.cloud import bigquery
from google import auth
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def get_time_partitionning_type(time_delta: timedelta):

    if time_delta <= timedelta(hours=1):
        return bigquery.table.TimePartitioningType.HOUR
    elif time_delta <= timedelta(days=1):
        return bigquery.table.TimePartitioningType.DAY
    elif time_delta <= timedelta(days=30):
        return bigquery.table.TimePartitioningType.MONTH
    else:
        return bigquery.table.TimePartitioningType.YEAR

def complete():
    credentials, project = auth.default()

    logging.info(f"credentials: {credentials}")
    logging.info(f"project: {project}")


if __name__ == "__main__":

    test_gcp_auth()