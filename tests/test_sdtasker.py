import sys
from os.path import dirname, abspath, join as p_join

from pyteradata.td_connect import run_query

assets_path = p_join(dirname(dirname(abspath(__file__))),'assets')

template_module_path = p_join(dirname(dirname(abspath(__file__))),'src')

if template_module_path not in sys.path:
    sys.path.append(template_module_path)

from sd_template import SDTemplateSQL, SDTemplateMissingParametersException
from sd_tasker import SDTaskPipeline, SDTaskException, SDSQLDatabaseTask as SQLTask

def info_board(message, **kwargs):
    print(message)

def main():
    run_query1 = None

    sql_task = SQLTask("select top 10 * from DBC.Tables", executor=run_query, info_cb=info_board,name="Top 10 Tables in DBC")

    df = sql_task.run()






if __name__ == '__main__':
    main()


