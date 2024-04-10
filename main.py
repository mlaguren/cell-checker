import numpy as np
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


def exact_match(source, target):
    if source.equals(target):
        return True
    else:
        return False


def highlight_diff(data, color='yellow'):
    attr = 'background-color: {}'.format(color)
    other = data.xs('First', axis='columns', level=-1)
    return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''),
                        index=data.index, columns=data.columns)


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
    elif rule == 'exact_match':
        if exact_match(df_source, df_target):
            print("pass")
        else:
            # print(pd.concat([df_source, df_target]).drop_duplicates(keep=False))
            df_all = pd.concat([df_source.set_index('id'), df_target.set_index('id')], axis='columns', keys=['Source', 'Target'])
            df_final = df_all.swaplevel(axis='columns')[df_source.columns[1:]]
            print(df_final)
            # output = df_final.style.apply(highlight_diff, axis=None)
            # f = open("styled_dataframe.html", "w")
            # f.write(output.to_html())
            # f.close()

