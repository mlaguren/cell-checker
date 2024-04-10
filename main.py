import pandas as pd
import yaml

with open("example.yaml") as stream:
    try:
        test_case = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

def data_source(source_type):
    if source_type[0]['type'] == 'csv':
        source = ["csv", source_type[1]['location']]
        source.append(source_type[1]['location'])
        return source
    else:
        return "Unknown"


def row_count(source, target):
    if source.shape[0] == target.shape[0]:
        return "equal"
    elif source.shape[0] > target.shape[0]:
        return "source"
    else:
        return "target"


source = data_source(test_case[1]['source'])
target = data_source(test_case[2]['target'])

if source[0] == 'csv':
    df_source = pd.read_csv(source[1])
else:
    print("Unknown")

if target[0] == 'csv':
    df_target = pd.read_csv(target[1])
else:
    print("Unknown")

policies = (test_case[3]['comparison_rules'])

for policy in policies:
    rule = list(policy.keys())[0]
    operation = list(policy.values())[0]
    if rule == 'row_count':
        outcome = row_count(df_source, df_target)
        if outcome == operation:
            print("pass")
        else:
            print("fail")

