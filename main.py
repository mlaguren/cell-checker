import numpy as np
import pandas as pd
import yaml
import oracledb
oracledb.init_oracle_client(lib_dir=r"C:\instantclient_21_13")



with open("oracle_test.yaml") as stream:
    try:
        test_case = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


def data_source(source_type):
    if source_type[0]['type'] == 'csv':
        data_type = ["csv", source_type[1]['location'], source_type[1]['location']]
        return data_type
    elif source_type[0]['type'] == 'oracle':
        connection_string = oracledb.connect(
            user = source_type[0]['connection_string']['user'],
            password = source_type[0]['connection_string']['password'],
            host = source_type[0]['connection_string']['host'],
            port = source_type[0]['connection_string']['port'],
            sid = source_type[0]['connection_string']['sid']
        )
        data_type = ["oracle", connection_string, source["query"]]
        return data_type
    # add elif for == 'oracle'. data_type should return [ 'oracle', connecttion_string ]
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
    df_source = pd.read_sql(source[2], source[1])
    # create a elif: condition for oracle. See data_source function
else:
    print("Unknown")

if target[0] == 'csv':
    df_target = pd.read_csv(target[1])
elif target[0] == 'oracle':
    df_target = pd.read_sql(target[2], target[1])
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

