import sys


from PyQt5.QtCore import (QEvent, Qt, QAbstractTableModel)
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import (QApplication, QGridLayout, QMainWindow, QWidget, QGroupBox, QLineEdit, QVBoxLayout, QTextEdit,
                             QSplitter, QPushButton, QTableView, QTableWidget, QTableWidgetItem)

#from pyteradata.td_connect import run_query, get_engine
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QProgressBar
from PyQt5.QtWidgets import QStatusBar

from utils.sqlalchemy_util import SQLQueryResultsThread

class PandasModel(QAbstractTableModel):
    """
    Class to populate a table view with a pandas dataframe
    #   model = PandasModel(your_pandas_data_frame)
        your_tableview.setModel(model)
    """
    def __init__(self, data, parent=None):
        QAbstractTableModel.__init__(self, parent)
        self._data = data

    def rowCount(self, parent=None):
        return len(self._data.values)

    def columnCount(self, parent=None):
        return self._data.columns.size

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.values[index.row()][index.column()])
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._data.columns[col]
        return None

from csv import DictReader

_dummy_file = r'P:\Documents\SS_Result_Pass1_2104_M1CDIC_2017-01-25.csv'
_dummy_headers = ["RetailOutletNumber","BPN","PlanogramName","CapacityBlocks","CaseSize","CurrentNoOfFacings",
                   "FrontFacingArea","TotalProductArea","TotalFrontFacingArea","CurrentShelfCap","HighestDaySales",
                  "Uncertainty","SafetyStock","SafetySales","SalesNormal","SalesPerAreaCm","NoReplens","Status"]

dr = DictReader(open(_dummy_file),fieldnames=_dummy_headers)
_dummy_data = [i for i in dr if dr.line_num > 1]

class DictionaryListModel(QAbstractTableModel):
    """
    Class to populate a table view with a pandas dataframe
    #   model = PandasModel(your_pandas_data_frame)
        your_tableview.setModel(model)
    """
    def __init__(self, data, columns, parent=None):
        QAbstractTableModel.__init__(self, parent)
        self._data = data
        self.columns = columns

    def rowCount(self, parent=None):
        return len(self._data)

    def columnCount(self, parent=None):

        return len(self._data[0].keys())

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                column_key = self.columns[index.column()]
                return str(self._data[index.row()].get(column_key))
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.columns[col]
        return None

class SqlTextEdit(QTextEdit):

    def __init__(self, parent=None):
        super(SqlTextEdit, self).__init__(parent)

    def event(self, event):
        if event.type() == QEvent.KeyPress and event.key() == Qt.Key_Tab:
            cursor = self.textCursor()
            cursor.insertText(" ")
            return True
        return QTextEdit.event(self, event)


class DATAToolWindow(QMainWindow):

    def __init__(self, parent=None):

        super(DATAToolWindow,self).__init__(parent=parent)

        self.data_items = None
        self.data_column_names = None

        self.setupUI()


    def setupUI(self):

        self.mainSplitter = QSplitter(Qt.Horizontal)
        self.setCentralWidget(self.mainSplitter)

        self.statusBar = self.statusBar()
        self.progressBar = QProgressBar()
        self.statusLabel = QLabel()
        self.cancelBtn = QPushButton("Cancel")
        self.cancelBtn.clicked.connect(self.terminate_query)

        self.statusBar.addWidget(self.statusLabel)
        self.statusBar.addWidget(self.progressBar, stretch=1)
        self.statusBar.addWidget(self.cancelBtn)

        self.progressBar.setMinimum(0)
        self.progressBar.setVisible(False)

        """
        self.createActions()
        self.createMenus()
        self.createToolBars()
        self.createStatusBar()
        """

        self.mainSplitter.addWidget(self.createSqlEditorGroup()) #,0,0)
        self.mainSplitter.addWidget(self.createDataTableView())

        #DEBUG
        self.sqlTextEdit.setText("select * from DXWI_PROD_VIEW_ACCESS.VWI0ROT_RETAIL_OUTLET")

        self.setGeometry(300, 300, 400, 300)
        self.setWindowTitle("Data Automated Tooling Application")
        self.setWindowIcon(QIcon('resources/sql_editor.png'))
        self.move(300, 150)

        self.show()

    def createSqlEditorGroup(self):

        sqlGrpBox = QGroupBox("SQL Text")

        vbox = QVBoxLayout()
        """
        self.paramTableView = QTableWidget()
        vbox.addWidget(self.paramTableView, stretch=1)
        """

        self.sqlTextEdit = SqlTextEdit(self)
        vbox.addWidget(self.sqlTextEdit, stretch=1)

        self.btn_RunSql = QPushButton("Run SQL")
        self.btn_RunSql.clicked.connect(self.btnRunSql_clicked)

        vbox.addWidget(self.btn_RunSql)

        sqlGrpBox.setLayout(vbox)

        return sqlGrpBox

    def createDataTableView(self):
        dataGrpBox = QGroupBox("Data Output")

        vbox = QVBoxLayout()
        self.dataTableView = QTableView(self)
        vbox.addWidget(self.dataTableView)

        dataGrpBox.setLayout(vbox)

        return dataGrpBox

    def btnRunSql_clicked(self):

        sql = self.sqlTextEdit.toPlainText()

        if len(sql.strip())>0:
            self.start_running_query(sql) #"select *  from DXWI_PROD_VIEW_ACCESS.VWI0ROT_RETAIL_OUTLET where country_code not in (63,74,0) order by total_sales_area desc")


    def start_running_query(self, sql_query):

        self.results_thread = SQLQueryResultsThread(sql_query=sql_query)

        self.results_thread.started.connect(self.started)
        self.results_thread.record_added.connect(self.add_record)
        self.results_thread.finished.connect(self.done)
        self.results_thread.start()


    def add_record(self, rowCount, rowNum, columnNames, resultDict):

        if rowNum==1:
            self.progressBar.setMaximum(rowCount)
            self.data_items = []
            self.data_column_names = columnNames

        self.data_items.append(resultDict)
        self.progressBar.setValue(self.progressBar.value()+1)
        self.statusLabel.setText("{} of {}".format(rowNum,rowCount))

    def started(self):
        self.btn_RunSql.setEnabled(False)
        self.btn_RunSql.setText("Running...")
        self.progressBar.setValue(0)
        self.progressBar.setVisible(True)

    def terminate_query(self):
        if self.results_thread and self.results_thread.isRunning():
            self.results_thread.terminate()

    def done(self):
        self.btn_RunSql.setEnabled(True)
        self.btn_RunSql.setText("Run SQL")

        self.progressBar.setValue(self.progressBar.maximum())

        model = DictionaryListModel(data=self.data_items, columns=self.data_column_names, parent=self)
        self.dataTableView.setModel(model)


def main():
    try:
        app = QApplication(sys.argv)
        _ex = DATAToolWindow()
        sys.exit(app.exec_())
    except Exception as ex :
        print(ex)
        print(sys.exc_info())

if __name__=='__main__':
    main()


