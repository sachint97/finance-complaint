import os,sys,requests,json,uuid,re,time
import pandas as pd
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logger
from dataclasses import dataclass
from typing import List
from datetime import datetime
from finance_complaint.entities.config_entities import DataIngestionConfig

@dataclass
class DownloadUrl:
    url:str
    file_path:str
    n_retry:str

class DataIngestion:

    def __init__(self, data_ingestion_config:DataIngestionConfig, n_retry:int=5):
        """
        data_ingestion_config: Data Ingestion config
        n_retry: Number of retry filed should be tried to download in case of failure encountered
        n_month_interval: n month data will be downloded
        """
        try:
            logger.info(f"{'>>' * 20}Starting data ingestion.{'<<' * 20}")
            self.data_ingestion_config = data_ingestion_config
            self.failed_download_urls: List[DownloadUrl]
            self.n_retry = n_retry

        except Exception as e:
            raise FinanceException(e, sys)

    def get_required_intervals(self):
        from_date = datetime.strptime(self.data_ingestion_config.from_date, "%Y-%m-%d")
        to_date = datetime.strptime(self.data_ingestion_config.to_date,"%Y-%m-%d")
        n_diff_days = (to_date - from_date).days
        freq=None
        if n_diff_days > 365:
            freq = "Y"
        elif n_diff_days > 30:
            freq = "M"
        elif n_diff_days > 7:
            freq = "W"
        logger.info(f"{n_diff_days} hence freq : {freq}")
        if freq is None:
            intervals = pd.date_range(start=from_date,
                                      end=to_date,
                                      periods=2)
        else:
            intervals = pd.date_range(start=from_date,
                                      end=to_date,
                                      freq=freq)
        logger.info(f"Prepared intervals : {intervals}")
        if self.data_ingestion_config.to_date not in intervals:
            intervals.append(self.data_ingestion_config.to_date)
        return intervals

    def download_data(self, download_url:DownloadUrl):
        logger.info(f"Starting download operation : {download_url}")
        download_dir = os.path.dirname(download_url.file_path)

        # Creating download directory
        os.makedirs(download_dir, exist_ok=True)

        # Download data
        response = requests.get(download_url.url, params={'User-agent': f'your bot {uuid.uuid4()}'})

        try:
            logger.info(f"Started writing downloaded data into json file: {download_url.file_path}")
            # saving downloaded data into hard disk
            with open(download_url.file_path,"w") as file_obj:
                finance_complaint_data = list(map(lambda x : x["_source"],
                                                    filter(lambda x : "_source" in x.keys(),
                                                            response.json())))
                json.dump(finance_complaint_data, file_obj)
            logger.info(f"Downloaded data has been written into file: {download_url.file_path}")
        except Exception as e:
            logger.info("Failed to download data hence retry again.")
            # removing failed file
            if os.path.exists(download_url.file_path):
                os.remove(download_url.file_path)
            self.retry_download_data(response, download_url)

    def retry_download_data(self, data, download_url: DownloadUrl):
        """
        This function help to avoid failure as it help to download failed file again

        data:failed response
        download_url: DownloadUrl
        """
        try:
            # if retry still possible try else return response
            if download_url.n_retry ==0:
                self.failed_download_urls.append(download_url)
                logger.info(f"Unable to download file {download_url.url}")
                return

            # to handle throatling requestion can be solve if we wait for some seconds
            content = data.cotent.decode("utf-8")
            wait_second = re.findall(r'\d+', content)

            if len(wait_second) > 0:
                time.sleep(int(wait_second[0]) + 2)

            # Writing response to understand why request was failed
            failed_file_path = os.path.join(self.data_ingestion_config.failed_dir,
                                            os.path.basename(download_url.file_path))

            os.makedirs(self.data_ingestion_config.failed_dir, exist_ok=True)
            with open(failed_file_path, "wb") as file_obj:
                file_obj.write(data.content)

            # calling download function again to retry
            download_url = DownloadUrl(download_url.url, download_url.file_path,
                                       download_url.n_retry - 1)

            self.download_data(download_url=download_url)

        except Exception as e:
            raise FinanceException(e, sys)

    def download_files(self, n_days_interval_url:int=None):
        """
        downloads data for given set of intervals
        """
        try:
            required_interval = self.get_required_intervals()
            logger.info("Started downloading files.")
            for index in range(1, len(required_interval)):
                from_date, to_date = required_interval[index - 1], required_interval[index]
                logger.debug(f"Generating data download url between {from_date} and {to_date}")
                datasource_url : str = self.data_ingestion_config.data_source_url
                url = datasource_url.replace("<todate>",
                                              to_date).replace("<fromdate>",from_date)
                logger.debug(f"Url: {url}")
                file_name = f"{self.data_ingestion_config.file_name}_{from_date}_{to_date}"
                file_path = os.path.join(self.data_ingestion_config.download_dir,file_name)
                download_url = DownloadUrl(url=url, file_path=file_path, n_retry=self.n_retry)
                self.download_data(download_url)
            logger.info(f"File download completed")
        except Exception as e:
            raise FinanceException(e, sys)

    def convert_files_to_parquet(self) -> str:
        pass


    def write_metadata(self, file_path: str) -> None:
        pass

    def initiate_data_ingestion(self):
        pass






