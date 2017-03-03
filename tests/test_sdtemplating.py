import unittest

import sys
from os.path import dirname, abspath, join as p_join

assets_path = p_join(dirname(dirname(abspath(__file__))),'assets')

template_module_path = p_join(dirname(dirname(abspath(__file__))),'src')

if template_module_path not in sys.path:
    sys.path.append(template_module_path)

from sd_template import SDTemplateSQL, SDTemplateMissingParametersException

#Test Params:
global_params = {'g_user_id':'goc7', 'g_project_id':'12345', 'g_playpen':'DXWI_PROD_CRORDER_PLAY_PEN'}

params={}
params['preferred_tpnd'] = 'DXWI_PROD_GENERIC_PLAY_PEN.Permanent_View_Preferred_TPND'
params['uk'] = 'DXWI_PROD_VIEW_ACCESS'
params['UK_ROI'] = 'DXWI_PROD_VIEW_ACCESS'
#params['trial_product_conditions'] = "and bpg.rs_22_department_code in ('A','B','D','F','K','L','M','U','V')"
params['trial_product_conditions'] = "and bpg.rs_22_department_code in ('C','H','N','Y')"
params['add_prod_exc'] = 'and bpg.product_sub_group_code is not null'

class SDSqlTemplateTestCase(unittest.TestCase):

    def setUp(self):
        self.params = params


    def test_noninclude(self):

        this_params = self.params.copy()

        this_params['bpg_temp_table'] = "{g_playpen}.{g_user_id}{g_project_id}_TEMP_TABLE_bpg".format(**global_params)
        this_params['control_product_conditions'] = " and bpg.rs_22_department_code in ('M','U','L')"
        task_template_file = p_join(assets_path,r'bpg\Get_Control_Products\Get_Control_Products.yaml')
        sdt = SDTemplateSQL(task_template_file)

        result = sdt(ns_bucket='0.25', **global_params, **this_params)
        self.assertIsInstance(result, str, "Not a string! What went wrong?")

    def test_noninclude_missing(self):

        this_params = self.params.copy()
        task_template_file = p_join(assets_path,r'bpg\Get_Control_Products\Get_Control_Products.yaml')
        sdt = SDTemplateSQL(task_template_file)

        with self.assertRaises(SDTemplateMissingParametersException) as ctx:
            result = sdt(ns_bucket='0.25', **global_params, **this_params)


    def test_include(self):

        task_template_file = p_join(assets_path,r'bpg\insert_trial_products_temp_table.yaml')
        sdt = SDTemplateSQL(task_template_file)

        result = sdt(ns_bucket='0.25', **global_params, **self.params)
        self.assertIsInstance(result, str, "Not a string! What went wrong?")


if __name__ == '__main__':
    unittest.main()

