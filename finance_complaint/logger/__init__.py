import logging
from datetime import date
import os

def get_log_file_name():
    return str(date.today())+".log"

LOG_FILE_NAME=get_log_file_name()
LOG_DIR = "logs"

os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE_PATH= os.path.join(LOG_DIR,LOG_FILE_NAME)

logging.basicConfig(filename=LOG_FILE_PATH,
                    filemode="a", # a for appending to existing file , w for overwriting
                    format="%(asctime)s - %(name)s -%(levelname)s - %(filename)s - %(funcName)s - %(lineno)d - %(message)s")

logger = logging.getLogger("FinanceProjectLogger")
logger.setLevel(logging.DEBUG)
