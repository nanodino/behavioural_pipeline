import pandas as pd
from pathlib import Path
from glob import glob
import csv


def write_to_excel(df_to_output: pd.DataFrame) -> None:
    with pd.ExcelWriter("./exceltest.xlsx") as writer:
        df_to_output.to_excel(writer)


def get_input_data_files() -> dict[str, pd.DataFrame]:
    data_path = Path("./data/")
    # check if I can get all the data from the aggregated tables??
    data_files = glob('AGG*.tsv', root_dir=data_path, recursive=False)
    input_data_tables_dict = {}
    columns_of_interest = ['Observation id',
                           'Subject', 'Behavior',
                           'Modifier #1', 'Modifier #2',
                           'Behavior type', 'Start (s)', 'Stop (s)']

    for file in data_files:
        print(f'Reading columns for file {file}')
        with open(f'{data_path}/{file}') as f:
            input_data_tables_dict[file] = pd.read_csv(f, delimiter='\t')
            input_data_tables_dict[file].drop(columns=[col for col in input_data_tables_dict[file]
                                                       if col not in columns_of_interest], inplace=True)

    return input_data_tables_dict


def concatenate_data_from_all_observations(test_output) -> pd.DataFrame:
    return pd.concat(test_output)


def get_behaviour_modifiers(concatenated: pd.DataFrame) -> pd.DataFrame:
    concatenated['Modifier #1'] = concatenated[['Behavior', 'Modifier #1']].apply(
        lambda x: x['Behavior'].split("_", 1)[1] if len(x['Behavior'].split('_')) > 1 else x['Modifier #1'], axis=1)
    concatenated['Behavior'] = concatenated[['Behavior']].apply(lambda x: x['Behavior'].split(
        "_", 1)[0] if len(x['Behavior'].split('_')) > 1 else x['Behavior'], axis=1)

    return concatenated


def assign_cage_number_from_observation_id_to_subject(modified: pd.DataFrame) -> pd.DataFrame:
    '''
    gets cage number from observation id and assigns it to subject 
    so that e.g. DBA becomes 18-DBA or 23-DBA, allowing the Subject 
    column to differentiate between mice of the same strain in 
    different cages.
    '''
    modified['Subject'] = modified[['Observation id', 'Subject']].apply(
        lambda x: f'{x["Observation id"].split(" ", 1)[0]}-{x["Subject"]}', axis=1)
    print(modified)
    return modified


def get_behaviour_data_for_each_subject(clean_data):
    # get number of bouts total

    # get average bout length + SD/variance
    # total bout length
    # average interbout interval + SD/variance
    # % bout per location
    pass

