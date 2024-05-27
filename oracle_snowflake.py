import numpy as np
import pandas as pd
import yaml
import oracledb
import snowflake.connector
import logging

#logging configuration
logging.basicConfig(filename='oracle_snowflake.log',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
#oracle driver thin -> thickmode
oracledb.init_oracle_client(lib_dir=r"C:\instantclient_21_13")

#replace yaml file here with appropriate test case
with open("oracle_snowflake.yaml") as stream:
    try:
        test_case = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


def data_source(source_type):
    if source_type[0]['type'] == 'csv':
        data_type = ["csv", source_type[1]['location'], source_type[1]['location']]
        return data_type
    elif source_type[0]['type'] == 'oracle':
        connection = oracledb.connect(
            user = source_type[1]['connection_string'][0]['user'],
            password = source_type[1]['connection_string'][1]['password'],
            host = source_type[1]['connection_string'][2]['host'],
            port = source_type[1]['connection_string'][3]['port'],
            sid = source_type[1]['connection_string'][4]['sid']
        )
        data_type = ["oracle", connection, source_type[2]['query']]
        logging.info(data_type)
        return data_type
    elif source_type[0]['type'] == 'snowflake':
        conn = snowflake.connector.connect(
            user = 'anthonyvu',
            password = 'PSRMCBR!D3it',
            account = 'ecoatm',
            warehouse = 'DW_PROD_WH',
            database = 'PROD_RAW',
            schema = 'PUBLIC'
        )
        data_type = ["snowflake", conn, source_type[2]['query']]
        logging.info(data_type)
        return data_type
    else:
        return "Unknown"


def row_count(source_data, target_data):
    if source_data.shape[0] == target_data.shape[0]:
        return "equal"
    elif source_data.shape[0] > target_data.shape[0]:
        return "source"
    else:
        return "target"


def exact_match(source_data, target_data):
    if source_data.equals(target_data):
        return True
    else:
        return False


source = data_source(test_case[1]['source'])
target = data_source(test_case[2]['target'])

if source[0] == 'csv':
    df_source = pd.read_csv(source[1])
elif source[0] == 'oracle':
    query = source[2]
    df_source = pd.read_sql(source[2], source[1])
    logging.info(df_source)
else:
    print("Unknown")

if target[0] == 'csv':
    df_target = pd.read_csv(target[1])
elif target[0] == 'snowflake':
    df_target = pd.read_sql(target[2], target[1])
    logging.info(df_target)
else:
    print("Unknown")

policies = (test_case[3]['comparison_rules'])

test_results = []

for policy in policies:
    rule = list(policy.keys())[0]
    operation = list(policy.values())[0]
    if rule == 'row_count':
        outcome = row_count(df_source, df_target)
        if outcome == operation:
            print("pass")
        else:
            print("fail")
    elif rule == 'exact_match':
        if exact_match(df_source, df_target):
            print("pass")
        else:
            df_all = pd.concat([df_source.set_index('id'), df_target.set_index('id')], axis='columns',
                               keys=['Source', 'Target'])
            df_final = df_all.swaplevel(axis='columns')[df_source.columns[1:]]

            output = df_final.style.format(precision=3, thousands=".", decimal=",").format_index(str.upper, axis=1).relabel_index(df_final.index.values.tolist(), axis=0).set_table_styles(
                [
                    {"selector": "td, th", "props": [("border", "1px solid grey !important")]},
                ]
            )
            f = open(f"{test_case[0]['name']}.html", "w")
            f.write(output.to_html())
            f.close()

