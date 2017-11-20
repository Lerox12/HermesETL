#
# Author: Lerox12
#

from configparser import ConfigParser
from lib.loggingManager import logger

class DictionaryConfig(object):

    def __init__(self, filename=None, section=None):
        logger.error('Dict filename: %s' % filename)
        self.filename = filename
        self.options = {}
        self.config = ConfigParser()
        self.config.read(self.filename)
        for section in self.config.sections():
            self.options[section] = self.getOptions(section)
        logger.debug('Options in chosen dictionary:')
        logger.debug(self.options)

    def getOptions(self, section):
        """
        Gets options for each section in dictionary format
        """
        options = self.config.options(section)
        params = {}
        for option in options:
            try:
                params[option] = self.config.get(section, option)
            except Exception:
                params[option] = None
        return params

    def __getitem__(self, name):
        return self.options[name]

    def __repr__(self):
        return self.options

    def __str__(self):
        return str(self.options)