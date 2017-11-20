#
# Author: Lerox12
#

import csv
import codecs

from lib.loggingManager import logger

class DataGetter:

    def __init__(self, inputPath):
        self.inputPath = inputPath

    def getDataFromCSV(self):
        """
        Method that gets data from a CSV. It creates
        a list of dictionaries. Each key of the
        dictionaries has the name of the sourceCSV column.

        :list return: data list
        """        
        result = []
        with codecs.open(self.inputPath, 'rb', encoding='latin-1') as csvfile:
            reader = csv.reader(csvfile, delimiter = ';', quotechar='"')
            for count, row in enumerate(reader):
                logger.debug(row)
                auxList = {}
                if count == 0:
                    headers = row
                else:
                    for i,v in enumerate(row):
                        try:
                            auxList[headers[i]] = v.strip()
                        except Exception:
                            auxList[headers[i]] = ''
                    result.append(auxList)
        logger.info('Successful importation from CSV file')
        logger.debug(result)
        return result
