
from sd_template import SDTemplateSQL,SDTemplateMissingParametersException

from os import chdir

chdir(r'P:\Documents\Python Projects\General\Python Handy Utils\SD Templating\assets') #\bpg\Get_Trial_Products')

global_params = {'g_user_id':'goc7', 'g_projec_id':'12345', 'g_playpen':'DXWI_PROD_CRORDER_PLAY_PEN'}


class MyTaskRunner:

    def __init__(self, *args, **kwargs):
        self._global_params = kwargs
        self._task_chain = []

    def register_sqltemplate(self, yaml_template, **kwargs):
        sdb = SDTemplateSQL()
        sdb.load(yaml_template)

        self._task_chain.append(sdb)

    def insert(self, index, call_fn):
        self._task_chain.insert(index, call_fn)

    def append(self, call_fn):
        self._task_chain.append(call_fn)

    def remove(self, call_fn):
        self._task_chain.remove()

    def run(self, no_execute=True, *args, **kwargs):

        # Here we chain all our params together
        params = self._global_params.copy() # Return a copy
        params.update(kwargs)

        for tsk in self._task_chain:
            output = tsk(**params)
            if isinstance(output, dict):
                print(output)
                params.update(output)

        print("Finished") #output.get('insert_temp_products'))



if __name__ == '__main__':

    params = {}
    params['ns_bucket'] = '0.25'
    params['preferred_tpnd'] = 'DXWI_PROD_GENERIC_PLAY_PEN.Permanent_View_Preferred_TPND'
    params['uk'] = 'DXWI_PROD_VIEW_ACCESS'
    params['UK_ROI'] = 'DXWI_PROD_VIEW_ACCESS'
    params['trial_product_conditions'] = "and bpg.rs_22_department_code in ('A','B','D','F','K','L','M','U','V')"
    params['add_prod_exc'] = 'and bpg.product_sub_group_code is not null'
    params['control_product_conditions'] = " and bpg.rs_22_department_code in ('M','U','L')"
    params['bpg_temp_table'] = "{g_playpen}.{g_user_id}{g_projec_id}_TEMP_TABLE_bpg".format(**global_params)
    runner = MyTaskRunner(**global_params)

    runner.register_sqltemplate('bpg\Get_Trial_Products\Get_Trial_Products.yaml')
    runner.register_sqltemplate('bpg\Get_Control_Products\Get_Control_Products.yaml')
    runner.register_sqltemplate('dtc\Get Dotcom Stores.yaml')

    def tester2(*args, **kwargs):
        print("Hello from tester2")

    def tester(*args, **kwargs):
        print("Hello from tester")

    runner.append(call_fn=tester)
    runner.append(call_fn=tester2)

    #runner.register_sqltemplate('insert_trial_products_temp_table.yaml')

    runner.run(**params)
