
from os.path import join as p_join

from sd_template import SDTemplateSQL
from sd_tasker import SDSQLDatabaseTask, SDTaskPipeline

from pyteradata.td_connect import run_nonquery, run_query
assets_dir = r'P:\Documents\Python Projects\General\Python Handy Utils\SD Templating\assets'

#control_products_sel_file = p_join(assets_dir, r'bpg\Get_Control_Products\Get_Control_Products.yaml')
#trial_products_sel_file = p_join(assets_dir, r'bpg\Get_Trial_Products\Get_Trial_Products.yaml')
products_temp_create_file = p_join(assets_dir, r'bpg\Create_tmp_products_table.yaml')
products_temp_insert_file = p_join(assets_dir, r'bpg\insert_trial_products_temp_table.yaml')

params = {}
params['ns_bucket'] = '0.25'

params['preferred_tpnd'] = 'DXWI_PROD_GENERIC_PLAY_PEN.Permanent_View_Preferred_TPND'
params['uk'] = 'DXWI_PROD_VIEW_ACCESS'
params['UK_ROI'] = 'DXWI_PROD_VIEW_ACCESS'

params['add_prod_exc'] = 'and bpg.product_sub_group_code is not null'


""" Trial """
trial_params={}
#trial_params['trial_product_conditions'] = "and bpg.rs_22_department_code in ('A','B','D','F','K','L','M','U','V')"
trial_params['trial_product_conditions'] = "and bpg.rs_22_department_code in ('C','H','N','Y')"

control_params={}
control_params['control_product_conditions']="and bpg.rs_22_department_code in ('M','U','L')"

def get_resultantsql(template_file, **kwargs):
    sdt = SDTemplateSQL(templatefile=template_file)
    sdt.update_inputs(**kwargs)
    result = sdt()
    return result


def main():
    project_details = {"g_playpen":"DXWI_PROD_CRORDER_PLAY_PEN","g_user_id":"goc7","g_project_id":"abc123"}
    params['bpg_temp_table'] = "{g_playpen}.{g_user_id}{g_project_id}_TEMP_TABLE_bpg".format(**project_details)

    #print(get_resultantsql(trial_products_sel_file, **params,**trial_params))

    create_table_sql = get_resultantsql(products_temp_create_file, **project_details)
    insert_table_sql = get_resultantsql(products_temp_insert_file, **project_details, **params, **trial_params)

    # Start Task Pipeline
    #tp = SDTaskPipeline()


    #print(create_table_sql)

    #sqltask1 = SDSQLDatabaseTask(querystring=create_table_sql, name="Create bg temp Table")
    #sqltask2 = SDSQLDatabaseTask(querystring=insert_table_sql, name="Create bg temp Table")
    run_nonquery(create_table_sql) #,database='DXWI_PROD_CRORDER_PLAY_PEN')
    run_nonquery(insert_table_sql)



if __name__ == '__main__':
    main()