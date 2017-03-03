from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal
from pyteradata.td_connect import get_engine
from csv import DictWriter as DW

class SQLQueryResultsThread(QThread):
    record_added = pyqtSignal(int,int,list,dict)
    def __init__(self, sql_query, database=''):
        QThread.__init__(self)
        self.database = database
        self.sql_query = sql_query

    def __del__(self):
        self.wait()


    def _get_query_results(self):
        """
        Generator for returning tuple of row number, column names and results dict
        :param sql_query: Query to fire against database
        :param database:
        :return: yields column names and results
        """

        sql_query = self.sql_query
        database = self.database
        """ Get an instance of the sqlalchemy teradata engine """
        td_engine = get_engine(database=database)

        """ Execute against the engine and return a generator """
        result_proxy = td_engine.execute(sql_query)
        for row_proxy in result_proxy:
            row_count = result_proxy.rowcount
            row = row_proxy._row
            """ Sort the columns dict by value which is the column order """
            column_names = sorted(row.columns, key=row.columns.get, reverse=False)
            dict_result = dict(zip(column_names,row.values))
            yield row_count, row.rowNum, column_names, dict_result


    def run(self):

        for row_count, rowNum, column_names, dict_result in self._get_query_results():
            self.record_added.emit(row_count, rowNum, column_names, dict_result)
            #self.emit(pyqtSignal('add_record()'), row_count, rowNum, column_names, dict_result)
            #self.sleep(2)



