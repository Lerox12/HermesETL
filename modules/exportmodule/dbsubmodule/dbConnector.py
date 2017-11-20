#
# Author: Lerox12
#

import pymysql.cursors

from lib.loggingManager import logger

class DBConnector:
    """
    Class which manages connection to a DB
    """

    def __init__(self, dbDict):
        self.database = dbDict['database']
        self.user = dbDict['user']
        self.password = dbDict['password']
        self.host = dbDict['host']

    def connect(self):
        """
        Method to connect with the database

        :return: Success
        """
        success = True
        try:
            self.db = MySQLdb.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                db=self.database)
        except Exception as e:
            logger.critical("Error connecting database: %s" % e)
            success = False
        try:
            self.cur = self.db.cursor()
        except Exception as e:
            logger.critical("Error with the cursor: %s" % e)
            success = False
        return success

    def close(self):
        """
        Method to close database connection

        :return: None
        """
        self.db.close()
        self.cur.close()

    def quickQuery(self, query, multiple=None, params=[]):
        """
        Execute a query in DB

        :param query: MySQL query
        :return: result (fetchone)
        """
        self.connect()
        try:
            self.cur.execute(query, params)
            logger.info("Query executed: %s" % query)
            if multiple:
                result = []
                #aux = self.cur.fetchone()
                result = self.cur.fetchall()
                return result
            else:
                result = self.cur.fetchone()
            if result is not None and len(result) > 0:
                result = str(result[0])
            self.db.commit()
        except MySQLdb.Error:
            logger.error('Fail executing query: %s' % self.cur._last_executed)
            # self.db.rollback()
            result = False
        except Exception:
            result = False
        finally:
            try:
                self.db.close()
            except Exception:
                logger.error('Fail trying to close the db')
                result = False
        return result

    def massiveExecution(self, queryList):
        """
        Execute the query list in DB

        :param queryList: list of MySQL queries
        :return: success
        """
        success = True
        self.connect()
        try:
            for q in queryList:
                try:
                    self.cur.execute(q)
                    self.db.commit()
                except Exception:
                    logger.error('Last query executed: %s' % self.cur._last_executed)
                    continue
        except MySQLdb.Error:
            logger.error('Last query executed: %s' % self.cur._last_executed)
            success = False
        except Exception as e:
            logger.error('General error executing queries: %s' % self.cur._last_executed)
            success = False
        finally:
            self.close()
            return success

    def updateDatabaseDictionary(self):
        """
        Method that generates the relation inside
        database fields and write them in a
        database config file

        :return: 
        """
        query = """SELECT
            ke.table_name 'child table',
            ke.column_name 'child column',
            ke.referenced_table_name 'parent table',
            ke.referenced_column_name 'parent column'
        FROM
            information_schema.KEY_COLUMN_USAGE ke
        WHERE
            ke.referenced_table_name IS NOT NULL
                AND table_schema = '%s'
        ORDER BY ke.referenced_table_name;""" % self.database
        self.connect()
        databaseSchema = []
        x = 0
        try:
            aux = self.quickQuery(query, "multiple")
            for row in aux:
                databaseSchema.append("{child_table}.{child_column} = {parent_table},{parent_column}".format(child_table=row[0], child_column=row[1], parent_table=row[2], parent_column=row[3]))
        except MySQLdb.Error:
            print(self.cur._last_executed)
            self.db.rollback()
            raise
            return False
        return databaseSchema

    def writeDatabaseDictionary(self, db_file, search, data, replace_item=False):
        """
        Method that ask for FK relations and write
        them in the database configuration file.

        :db_file: 
        search: 
        data: 
        """
        with open("Databases/%s" % db_file, "r") as f:
            update = f.readlines()
        with open ("Databases/%s" % db_file, "w") as f2:
            for line in update:
                line = line.strip()
                if search in line:
                    if replace_item and type(data) != str:
                        for d in data:
                            if not filter(lambda x: d.split(',')[0] in x, update):
                                f2.write('%s\n' % d)
                    elif replace_item:
                        f2.write('%s\n' % ''.join(data))
                    else:
                        f2.write('%s\n' % line)
                        for d in data:
                            if not filter(lambda x: d.split(',')[0] in x, update):
                                f2.write('%s\n' % d)
                else:
                    f2.write('%s\n' % line)
