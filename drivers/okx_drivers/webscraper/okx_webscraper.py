from datetime import datetime
from pathlib import Path
import logging
from time import sleep

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains

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

        chromeOptions = webdriver.ChromeOptions()
        prefs = {"download.default_directory": str(self.data_folder_path.resolve())}
        chromeOptions.add_experimental_option("prefs", prefs)

        self.driver = webdriver.Chrome(
            ChromeDriverManager().install(), chrome_options=chromeOptions
        )

        self.driver.get("https://www.okx.com/data-download")

        self.driver.set_window_position(-1000, 0)
        self.driver.maximize_window()

        self.actions = ActionChains(self.driver)

        self.PAUSE_TIME = 0.5

    @property
    def available_data_type(self):
        return ["aggtrades", "swaprate", "trades"]

    @property
    def available_periods(self):
        return ["daily", "monthly"]

    def get_navigation(self):
        elements_list = self.driver.find_element(by=By.CLASS_NAME, value="folder-path")
        elements = elements_list.find_elements(by=By.CLASS_NAME, value="floder-name")
        return elements

    def get_all_items(self):
        elements_list = WebDriverWait(self.driver, 20).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "folder-list"))
        )
        elements_items = elements_list.find_elements(by=By.TAG_NAME, value="li")
        return elements_items

    def get_item_in_folder_list(self, item_name: str):

        elements = self.get_all_items()
        matches = list(filter(lambda x: item_name in x.text, elements))

        if len(matches) == 0:
            raise ValueError(f"Couldn't find {item_name}")
        elif len(matches) == 1:
            return matches[0]
        elif len(matches) > 1:
            raise ValueError(f"Found {len(matches)} matches for {item_name}")

    def click_on_item_in_list(self, item_name: str):
        item = self.get_item_in_folder_list(item_name=item_name)
        self.click_when_visible(item)

    def click_when_visible(self, item):
        try:
            logger.info(f"Navigating to {item.text}")
            WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable(item)
            ).click()
            sleep(self.PAUSE_TIME)
        except Exception as e:
            logger.error(e)

    def scroll_to_bottom(self):
        # Get scroll height
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        while True:
            # Scroll down to bottom
            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);"
            )

            # Wait to load page
            sleep(self.PAUSE_TIME)

            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def go_back_one_step(self):
        nav_menu = self.get_navigation()
        self.click_when_visible(nav_menu[-2])

    def download_swaprates(self):

        self.click_on_item_in_list(item_name=self.data_type)

        # best to daily since monthly has missing data (26th, 27th and 28th of February 2022)
        self.click_on_item_in_list(item_name=self.period)

        self.scroll_to_bottom()

        if self.resume_path:
            available_dates = set([item.text for item in self.get_all_items()])
            p = self.resume_path.glob("*.zip")
            existing_dates = set(["".join(x.stem.split("-")[-3:]) for x in p])
            all_periods = list(available_dates - existing_dates)
        else:
            all_periods = [item.text for item in self.get_all_items()]

        for period in all_periods:

            try:
                element = self.get_item_in_folder_list(item_name=period)
                self.actions.move_to_element(element).perform()
            except Exception as e:
                logger.error(e)

            try:
                self.click_on_item_in_list(item_name=period)
            except Exception as e:
                logger.error(e)

            self.scroll_to_bottom()

            try:
                all_files = self.get_all_items()
            except Exception as e:
                logger.error(e)
                self.go_back_one_step()
                self.scroll_to_bottom()
                continue

            if len(all_files) == 0:
                self.go_back_one_step()
                self.scroll_to_bottom()
                continue

            for file in all_files:
                file.click()
                sleep(0.1)

            self.go_back_one_step()

            self.scroll_to_bottom()

        self.driver.close()


if __name__ == "__main__":
    resume_path = Path("./2023-01-19_10-15-01")
    okx = OKXWebscaper(data_type="aggtrades", period="monthly")
    okx.download_swaprates()
