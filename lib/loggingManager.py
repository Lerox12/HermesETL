#
# Author: Lerox12
#

import logging
import sys

# Create logger
logger = logging.getLogger('simple_example')
logger.setLevel(logging.DEBUG)

# Create file handler and set level to debug
fh = logging.FileHandler('logs/myapp.log')
fh.setLevel(logging.INFO)

# Create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')

# Add formatter to ch and fh
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# Add ch and fh to logger
logger.addHandler(ch)
logger.addHandler(fh)