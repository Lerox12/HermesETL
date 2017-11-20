#
# Author: Lerox12
#

from modules.exportmodule.dbsubmodule.dbConnector import DBConnector
from lib.loggingManager import logger

class DBExport:
    """
    Class to generate queries.
    """
    def __init__(self, parsedInput, dbDict):
        self.parsedInput = parsedInput
        self.dbDict = dbDict
        self.dbConnector = DBConnector(dbDict['connection'])
        self.selectQuery = 'SELECT {fields} FROM {table};'
        self.allSelectQuery = 'SELECT * FROM {table} WHERE {condition};'
        self.conditionSelectQuery = 'SELECT {fields} FROM {table} WHERE {condition};'
        self.updateQuery = 'UPDATE {table} SET ({updates}) WHERE ({condition});'
        self.insertQuery = 'INSERT INTO {table} ({columns}) VALUES ({values});'
        self.deleteQuery = 'DELETE FROM {table} WHERE {condition};'

    def splitForTables(self, item):
        """
        This method split an insert. Generates one
        key for each table an a dictionary inside
        each key with the related data.

        :param item: dictionary with data.

        :result: returns one item for each table
        """
        result = {}
        logger.debug('Item for splitting: %s' % item)
        for i in item:
            try:
                result[i.split('.')[0]][i] = item[i]
            except KeyError:
                try:
                    result[i.split('.')[0]] = {}
                    result[i.split('.')[0]][i] = item[i]
                except Exception:
                    logger.error('Error splitting item key: %s, result: %s, item: %s' % (i, result, item))
                    raise
        logger.debug('Splitted item: %s' % result)
        return result    

    def searchFK(self, item):
        """
        Method that set foreign keys and insert if they
        don't exist.

        :return: parsed input
        """
        foreignKeys = self.dbDict['foreing_keys']
        for fk in foreignKeys:
            if fk in item:
                f = foreignKeys[fk].split(',')
                logger.critical('FK: %s' % foreignKeys[fk])
                # Ask for foreign key field if not exists
                if len(f) < 3:
                    fk_field = input("\nPlease, insert the relation with '%s' inside '%s': " % (fk, item))
                    foreignKeys[fk] = ','.join([foreignKeys[fk], fk_field])
                    f.append(fk_field)
                    self.dbConnector.writeDatabaseDictionary(fkFileName, fk.strip(), "%s = %s" % (fk.strip(), foreignKeys[fk]), True)
                # query = "SELECT `{table}`.`{fk_key}` FROM `{table}` WHERE `{table}`.`{key}` = '%s';".format(table=f[0], fk_key=f[1], key=f[2]) % item[fk]
                result = dbConnector.quickQuery(self.conditionSelectQuery.format(fields='.'.join([f[0], f[1]]), table=f[1], condition=' = '.join([f[0] + f[2], '"%s"' % item[fk]])))
                
                # controlQuery = "SELECT `{table}`.`{fk_key}` FROM `{table}` WHERE `{table}`.`{fk_key}` = '%s';".format(table=f[0], fk_key=f[1], key=f[2]) % item[fk]
                control = self.dbConnector.quickQuery(self.conditionSelectQuery.format(fields='.'.join([f[0], f[1]]), table=f[1], condition=' = '.join([f[0] + f[1], '"%s"' % item[fk]])))
                if result:
                    item[fk] = result
                elif not control:
                    item2 = {}
                    item2["%s" % f[2]] = item[fk]
                    for v in item:
                        if f[0] in item:
                            item2[f[0]] = item[f[0]]
                    for fk in foreignKeys:
                        if fk in item2:
                            item2 = searchFK(prim, item2, fkFileName)
                        else:
                            insertQuery, up = generateQuery(database, item2, f[0])
                            self.dbConnector.quickQuery(insertQuery)
                            return searchFK(prim, item, fkFileName)
        logger.info('Vul with FKs: ' % item)
        return item

    def generateQuery(self, item, table):
        """
        Method that generate a query in order to
        insert or update inside a DB.

        :str return insertQuery
        :str return updatequery
        """
        primaryKeys = self.dbDict['primary_keys']
        updateKeys = self.dbDict['update_keys']
        insertQuery = ''
        updateQuery = ''
        auxWhereListKeys = []
        auxWhereListTables = []
        auxWhereListValues = []
        for prim in primaryKeys.values():
            prim = prim.split(',')
            for onePrim in prim:
                if onePrim in item:
                    auxWhereListKeys.append(onePrim)
                    if onePrim.split('.')[0] not in auxWhereListTables:
                        auxWhereListTables.append(onePrim.split('.')[0])
                    auxWhereListValues.append('%s = "%s"' % onePrim, item[onePrim])
        if auxWhereListKeys:
            checkedResult = self.conditionSelectQuery.format(fields=','.join(auxWhereListKeys), table=','.join(auxWhereListTables), condition=','.join(auxWhereListValues))
            if not checkedResult and not updateKeys:
                # Checks if any result for checkedResult has no value
                # and add to update
                ## TODO
                pass
            if checkedResult and updateKeys:
                # Update path
                auxUpQuery = []
                for up in updateKeys:
                    if up in item:
                        # Append update field
                        auxUpQuery.append(up)
                        # Append update value
                        auxUpValueQuery.append('"%s"' % item[up])
                updateQuery = self.updateQuery.format(table=table, updates=','.join(auxUpQuery), values=','.join(auxUpQuery))
            else:
                # Insert path
                auxInsertValues = ['"%s"' % x for x in item.values()]
                insertQuery = self.insertQuery.format(table=table, columns=','.join(item.keys()), values=','.join(auxInsertValues))
        return insertQuery, updateQuery

    def generateQueries(self):
        """
        Main method in the class that connects
        with the others, excepts executeGeneratedQueries.

        :list return insertQueriesList:
        :list return updateQueriesList:
        """
        for item in self.parsedInput:
            aux = self.searchFK(item)
            if aux:
                item = aux
            splittedDict = self.splitForTables(item)
            for table, splittedItem in splittedDict.items():
                insertQuery, updateQuery = self.generateQuery(self.dbConnector, splittedItem, table)
                insertQueriesList.append(insertQuery)
                updateQueriesList.append(updateQuery)
        return insertQueriesList, updateQueriesList

    def executeGeneratedQueries(self, insertQueriesList, updateQueriesList):
        """
        Method that executes all generated queries.

        :list insertQueriesList: list of insertion queries
        :list updateQueriesList: list of update queries
        """
        relationQueryDict = {}
        relationsList = self.dbDict['relations_list']
        for query in insertQueriesList:
            dbConnector.quickQuery(query)
            # Check if is needed an insertion in a third
            # table because of many-to-many relation
            for key, relation in relationsList.items:
                relation = relation.split(',')
                if item['table'] in relation:
                    idUpdatedValue = dbConnector.quickQuery(self.selectQuery.format(fields='MAX(%s)' % relationsList[0], table=item['table']))
                    if key in relationQueryDict:
                        relationQueryDict[key] = relationQueryDict[key] + [relation[relation.index(item['table'])-1], relation[relation.index(item['table'])], idUpdatedValue]
                    else:
                        relationQueryDict[key] = [relation[relation.index(item['table'])-1], relation[relation.index(item['table'])], idUpdatedValue]

        for item in updateQueriesList:
            dbConnector.quickQuery(self.updateQuery.format(table=item['table'], updates=item['updates'], condition=item['condition']))
            # Check if an insertion in a third table
            # is needed because of many-to-many relation
            for key, relation in relationsList.items:
                relation = relation.split(',')
                if item['table'] in relation:
                    idUpdatedValue = dbConnector.quickQuery(self.selectQuery.format(fields='MAX(%s)' % relationsList[0], table=item['table']))
                    if key in relationQueryDict:
                        relationQueryDict[key] = relationQueryDict[key] + [relation[relation.index(item['table'])-1], relation[relation.index(item['table'])], idUpdatedValue]
                    else:
                        relationQueryDict[key] = [relation[relation.index(item['table'])-1], relation[relation.index(item['table'])], idUpdatedValue]
        # Insert relations if there are not yet
        for rel_key, rel_value in relationQueryDict.items():
            # condition = ','.join([x for i,x in enumerate(rel_value) if i % 2 == 0])
            # columns = ','.join([x for i,x in enumerate(rel_value) if i % 1 == 0])
            # values = ','.join(['"%s"' % x for i,x in enumerate(rel_value) if i % 1 == 0])
            if not dbConnector.quickQuery(self.allSelectQuery.format(table=rel_key, condition=condition)):
                dbConnector.quickQuery(self.insertQuery.format(table=rel_key, columns=columns, values=values))

def main():
    dbExport = DBExport('', '')

if __name__ == '__main__':
    main()
