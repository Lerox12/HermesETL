#
# Author: Lerox12
#

"""
This manager connects all modules
and gives an user menu.
All code wrote in Python3.
"""

import time
import subprocess
import os

from lib.loggingManager import logger
from lib.config import DictionaryConfig
from modules.gettermodule.dataGetter import DataGetter
from modules.parsermodule.dataParser import DataParser
from modules.exportmodule.dbsubmodule.dbExport import DBExport

class MainManager:
    """
    Class with all menu options.
    """

    def __init__(self):
        self.exportOptions = ['Database', 'JSON', 'CSV']
        self.inputPath = ''
        self.dataDict = ''
        self.dbDict = ''

    def inputPathMenu(self):
        """
        Menu which gets input data path.
        """
        while True:
            subprocess.call('clear')
            availableData = subprocess.getoutput('ls data').split()
            print('Available data stored:')
            for num, data in enumerate(availableData):
                print('%s. %s' % (num, data.strip()))
            inputPath = input('Please, insert input path or choose one stored files: ')
            try:
                inputPath = int(inputPath)
            except ValueError:
                logger.debug('Input path is not int: %s' % inputPath)                   
            if isinstance(inputPath, int) and inputPath < len(availableData):
                self.inputPath = os.path.abspath('data/%s' % availableData[inputPath])
                break
            elif os.path.isfile(inputPath):
                self.inputPath = os.path.abspath('data/%s' % inputPath)
                break
            else:
                input('File does not exist. Press enter to continue: ')        
        logger.debug('Chosen input file %s' % inputPath)

    def dataDictionaryMenu(self):
        """
        Menu which gets data dictionary path.
        """
        while True:
            subprocess.call('clear')
            availableInputDict = subprocess.getoutput('ls dictionaries/inputDict').split()
            print('Available data dictionaries stored:')
            for num, data in enumerate(availableInputDict):
                print('%s. %s' % (num, data.strip()))
            dataDict = input('Please, insert data dictionary or choose one stored files: ')
            try:
                dataDict = int(dataDict)
            except ValueError:
                logger.debug('Input path is not int: %s' % dataDict)            
            if isinstance(dataDict, int) and dataDict < len(availableInputDict):
                self.dataDict = os.path.abspath('dictionaries/inputDict/%s' % availableInputDict[dataDict])
                break
            elif os.path.isfile(dataDict):
                self.dataDict = os.path.abspath('dictionaries/inputDict/%s' % dataDict)
                break
            else:
                input('File does not exist. Press enter to continue: ')
        logger.debug('Chosen data dict file %s' % dataDict)
        logger.debug('Start getting data')
        dataType = self.inputPath.strip('.')[-1]
        if dataType.lower() == 'csv':
            self.getDataFromCSV()        

    def exportOptionsMenu(self, parsedData):
        """
        Menu which allows to chose export options.
        """
        while True:
            subprocess.call('clear')
            print('Available export options:')
            for num, exOpt in enumerate(self.exportOptions):
                print('%s. %s' % (num, exOpt.strip()))
            exportOption = input('Please, choose one option: ')
            try:
                exportOption = int(exportOption)
            except ValueError:
                logger.debug('Input path is not int: %s' % exportOption)
            if isinstance(exportOption, int) and exportOption < len(self.exportOptions):
                exportOption = self.exportOptions[exportOption]
                break
            else:
                input('Not an available option. Press enter to continue: ')
            logger.debug('Chosen export option: %s' % exportOption)
        if exportOption == 'Database':
           self.dbMenu(parsedData)
        elif exportOption == 'JSON':
            pass
        elif exportOption == 'CSV':
            pass

    def dbMenu(self, parsedData):
        """
        Database export option interface.
        """
        while True:
            subprocess.call('clear')
            print('Available databases:')
            availableDBDict = subprocess.getoutput('ls dictionaries/dbDict').split()
            for num, exOpt in enumerate(availableDBDict):
                print('%s. %s' % (num, exOpt.strip()))
            dbDict = input('Please, choose one option: ')
            try:
                dbDict = int(dbDict)
            except ValueError:
                logger.debug('Input path is not int: %s' % dbDict)
            if isinstance(dbDict, int) and dbDict < len(availableDBDict):
                dbDict = availableDBDict[dbDict]
                logger.debug('Chosen export option: %s' % dbDict)
                self.dbDict= os.path.abspath('dictionaries/dbDict/%s' % dbDict)
                modDBExport = DBExport(parsedData, DictionaryConfig(self.dbDict))
                insertQueriesList, updateQueriesList = modDBExport.generateQueries()
                modDBExport.executeGeneratedQueries(insertQueriesList, updateQueriesList)
                break
            else:
                input('Not an available option. Press enter to continue: ')
        logger.info('Successful exportation to database')

def main():
    mainManager = MainManager()
    mainManager.inputPathMenu()
    mainManager.dataDictionaryMenu()
    modDataGetter = DataGetter(mainManager.inputPath)
    dataType = mainManager.inputPath.split('.')[-1]
    if dataType.lower() == 'csv':
        rawInputData = modDataGetter.getDataFromCSV()
    inputDataDictionary = DictionaryConfig(mainManager.dataDict)
    modDataParser = DataParser(inputDataDictionary)
    parsedData = modDataParser.parseRegularCases(rawInputData)
    mainManager.exportOptionsMenu(parsedData)

if __name__ == '__main__':
    main()