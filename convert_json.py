import json
import pandas as pd

with open('expectations_robust.json') as proyect_file:
    DATA = json.load(proyect_file)


def convert_meta_statistics(json_ge: dict):
    """Convert the meta and statistics from the expectation json.

    Args:
        json_ge (dic): json object converted to dic for process.
    """
    #Take the last result of the succes expectation to save it as a name of the csv
    success = json_ge['success']
    id_exp = json_ge['meta']['run_id']['run_name']
    csv_new_name = f"statistics_{success}_{id_exp}.csv"

    df_statistics = pd.json_normalize(json_ge['statistics'])
    df_meta = pd.json_normalize(json_ge['meta'])

    #We want only some fields of the Meta Data Frame
    fields_id = ["expectation_suite_name", "run_id.run_time", 'run_id.run_name']
    fields_kwargs = ['batch_kwargs.datasource', 'batch_kwargs.path', 'batch_kwargs.reader_method']
    fields_result = fields_id + fields_kwargs

    df_meta = df_meta[fields_result]

    bigdata = pd.concat([df_statistics, df_meta], ignore_index=False, sort=False)
    bigdata.insert(0, 'ID', id_exp)
    bigdata.to_csv(csv_new_name, index=False)


def convert_expectations(json_ge: dict):
    """Convert the validations from the expectation json.

    Args:
        json_ge [dict]: json object converted to dic for process.
    """
    #save the id of the json
    id_exp = json_ge['meta']['run_id']['run_name']
    csv_new_name = f"expectation_result_{id_exp}.csv"

    # We want only some fields of the Expectations
    result = []
    for info in json_ge['results']:
        new = []
        #print(info['exception_info'])
        new.append(info['exception_info'])
        new.append(info['expectation_config']['expectation_type'])
        new.append(info['expectation_config']['kwargs'])
        new.append(info['expectation_config']['meta'])
        new.append(info['result'])
        new.append(info['meta'])
        new.append(info['success'])
        result.append(new)

    column_names = ['exception_info', 'expectation_config.type', 'expectation_config.kwargs',
                    'expectation_config.meta', 'result', 'meta', 'success']

    df_expectations = pd.DataFrame(result, columns=column_names)
    df_expectations.insert(0, 'ID', id_exp)
    df_expectations.to_csv(csv_new_name, index=False)


convert_expectations(DATA)
convert_meta_statistics(DATA)
