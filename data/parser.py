from constants import *
from helpers import *
from logger import configure_logging
import time

class DataBatch:
    pass

class DataParser:
    def __init__(self):
        self.sources = [TIDEPOOL_FOLDER, BITESNAP_FOLDER, FITBIT_FOLDER]
        self.logger = configure_logging()

        create_folders()

    def process_data(self):
        tic = time.time()
        #  process code

        toc = time.time()
        self.logger.info("Parsing time elapsed: ", toc - tic)
    
if __name__ == '__main__':
    pass
