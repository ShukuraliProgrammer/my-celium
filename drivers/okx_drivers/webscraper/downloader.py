import logging
import os
import sys
import time
import zipfile
from typing import List

import requests

logging.basicConfig(level=logging.INFO)

import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OKXWebscaper:
    def __init__(self, data_type: str, period: str, resume_path: Path = None):

        self.resume_path = resume_path
        self.data_type = data_type
        self.period = period

        assert self.data_type in self.available_data_type
        assert self.period in self.available_periods

        if resume_path:
            self.data_folder_path = self.resume_path
        else:
            self.cwd_path = Path.cwd()
            self.data_folder_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            self.data_folder_name = (
                f"{self.data_folder_name}_{self.data_type}_{self.period}"
            )
            self.data_folder_path = self.cwd_path / self.data_folder_name
            self.data_folder_path.mkdir(parents=True, exist_ok=True)

        self.all_dates = pd.date_range(start=date(2021, 10, 1), end=date.today())
        self.all_months_str = list(set(self.all_dates.strftime("%Y%m")))

        self.all_dates_str = list(set(self.all_dates.strftime("%Y-%m-%d")))
        self.all_dates_str.sort()

        self.PAUSE_TIME = 0.5

    def validate_zip_file(self, zip_file: str) -> bool:
        """Validate zip file."""
        the_zip_file = zipfile.ZipFile(zip_file)
        ret = the_zip_file.testzip()
        return ret is None

    def download(self, url: str, output_file: str) -> bool:
        """Download a zip file, skip if it exists."""
        assert output_file.endswith(".zip")
        if os.path.exists(output_file) and self.validate_zip_file(output_file):
            logging.info(f"Skipped {url}")
            return True
        logging.info(f"Downloading {url}")
        resp = requests.get(url, stream=True)
        with open(output_file, "wb") as f_out:
            for chunk in resp.iter_content(chunk_size=4096):
                if chunk:  # filter out keep-alive new chunks
                    f_out.write(chunk)
        return True

    def list_files(self, month: int) -> List[str]:
        """List files in a month."""
        url = f"https://www.okx.com/priapi/v5/broker/public/orderRecord?t={int(time.time() * 1000)}&path=cdn/okex/traderecords/{self.data_type}/monthly/{month}"
        obj = requests.get(url).json()
        if obj["code"] != "0":
            raise ValueError(obj)
        return [x["fileName"] for x in obj["data"]]

    def okx_download(self) -> bool:
        """Download OKX data"""

        for date_str in self.all_dates_str:
            year_month = "".join(date_str.split("-")[:2])
            file_name_future = f"allfuture-aggtrades-{date_str}.zip"
            file_name_spot = f"allspot-aggtrades-{date_str}.zip"
            file_name_swap = f"allswap-aggtrades-{date_str}.zip"
            url = f"https://static.okx.com/cdn/okex/traderecords/{self.data_type}/monthly/{year_month}/{file_name_swap}"
            output_file = self.data_folder_path.resolve() / file_name_swap
            downloaded = self.download(url, str(output_file))
            if downloaded:
                logger.info(f"SUCESS: downloaded {file_name_swap}")
            else:
                logger.error(f"ERROR: downloaded {file_name_swap}")

        return True

    @property
    def available_data_type(self):
        return ["aggtrades", "swaprate", "trades"]

    @property
    def available_periods(self):
        return ["daily", "monthly"]


if __name__ == "__main__":
    resume_path = Path("./2023-01-19_10-15-01")
    okx = OKXWebscaper(data_type="aggtrades", period="monthly")
    okx.okx_download()
