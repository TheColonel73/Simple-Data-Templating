import sqlalchemy
from pyteradata.td_connect import run_query, run_nonquery

"""
TODO: Add a logger for attaching as decorator to methods
"""

from uuid import uuid1
from datetime import datetime as dte

import inspect # Use this to perform runtime inspection
import pandas as pd


class SDTaskException(Exception):
    pass

class SDTaskBase:

    def __init__(self, info_cb=None, *args, **kwargs):

        self.taskid = str(uuid1())
        self._internal_settings = {"task_id": self.taskid}
        self._infomessages = []

        self._info_cb = info_cb

        if 'name' in kwargs:
            self.taskname = kwargs.get('name')
        else:
            self.taskname = self.taskid

    def pre_task(self):
        pass

    def log_info(self, message, *args, **kwarg):
        formatted_message = "{} - {}".format(dte.now().isoformat(), message)
        self._infomessages.append(formatted_message)

        if self._info_cb:
            if not inspect.isfunction(self._info_cb):
                raise SDTaskException("An callable function has not been passed as the information callback.")
            self._info_cb(formatted_message, task_id=self.taskid)

    def output(self):
        pass # Where will the output go!

    def run(self, *args, **kwargs):
        pass

    def post_task(self):
        pass


class SDSQLDatabaseTask(SDTaskBase):

    def __init__(self, querystring, executor,  resultset=True, info_cb=None, *args, **kwargs):
        """

        :param querytype: resultset or not
        :param executor: The function that will be called for this task
        :param info_cb: this is the callback for logging
        """
        super(SDSQLDatabaseTask,self).__init__(info_cb=info_cb, **kwargs)

        if not querystring or len(querystring) == 0:
            raise SDTaskException("No query string has been passed.")

        if not inspect.isfunction(executor):
            raise SDTaskException("An callable function has not been passed as the executor of task.")

        self.resultset = resultset
        self.executor = executor
        self.querystring = querystring
        self.result = None

        self.log_info("Initialised SDSQLDatabaseTask (Task: {}).".format(self.taskname))


    def run(self, *args, **kwargs):

        try:
            self.log_info("Preparing to run task: " + self.taskname)
            result = self.executor(self.querystring, *args, **kwargs)
            self.log_info("Completed task: " + self.taskname)
            if isinstance(result, pd.DataFrame):
                self.log_info("Returning a Dataframe size:{} (task:{})".format(str(len(result.index)), self.taskname))
            return result

        except SDTaskException as taskex:
            self.log_info("Error - {}".format(taskex))


class SDTaskPipeline:

    def __init__(self, global_settings, *args, **kwargs):
        self._tasks = []

    def add(self, task):

        assert isinstance(task, SDTask)
        self._tasks.append(task)

    def remove(self,task):
        self._tasks.remove(task)

    @property
    def tasklist(self):
        return self._tasks

    def run(self, *args, **kwargs):
        # A list of task objects, presumed to be configured

        for task in tasks:
            try:
                task.run()
            except:
                print("Terminated at:" + task.taskname)



