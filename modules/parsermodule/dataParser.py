#
# Author: Lerox12
#

import copy

from lib.loggingManager import logger

class DataParser:

    def __init__(self, infoDict):
        self.infoDict = infoDict

    def parseRegularCases(self, rawInput):
        """
        Method that change input keys for wanted
        keys in the dictionary.

        :list rawInput: list of dictionaries.

        :list return: list with dictionaries with
        wanted keys.
        """
        result = []
        regularCasesDict = self.infoDict['regular_cases']
        for num, item in enumerate(rawInput):
            aux = {}
            for field in regularCasesDict:
                if field in item:
                    aux[regularCasesDict[field]] = item[field]
            result.append(aux)
        self.parseSpecialCases(result)
        logger.debug('Raw input parsed: %s' % result)
        return result

    def parseSpecialCases(self, rawInput):
        """
        Method that checks if a field is a special
        case and makes actions. This method is highly
        configurable and should be tested.

        :list rawInput: list of dictionaries with
        almost parsed data.

        :list return: returns parsed data.
        """
        specialCasesDict = self.infoDict['special_cases']
        for num, item in enumerate(rawInput):
            for field in specialCasesDict:
                if field in item:
                    if field == 'vehicles.wheels':
                        # Special actions
                        rawInput[num][field] = specialCasesDict[field]
        return rawInput
