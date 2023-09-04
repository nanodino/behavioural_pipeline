import pandas as pd
from pathlib import Path
from glob import glob
import csv


def write_to_excel(df_to_output: pd.DataFrame) -> None:
    with pd.ExcelWriter("./exceltest.xlsx") as writer:
        df_to_output.to_excel(writer)


def get_input_data_files() -> dict:
    data_path = Path("./data/")
    data_files = glob('*.tsv', root_dir=data_path, recursive=False)
    dfs_dict = {}
    columns_of_interest = ['Observation id',
                           'Observation date', 'Subject', 'Behavior',
                           'Modifier #1', 'Modifier #2', 'Time',
                           'Behavior type', 'Start (s)', 'Stop (s)', 'Duration (s)']

    for file in data_files:
        print(f'Reading columns for file {file}')
        with open(f'{data_path}/{file}') as f:
            dfs_dict[file] = pd.read_csv(f, delimiter='\t')
            dfs_dict[file].drop(columns=[col for col in dfs_dict[file]
                                         if col not in columns_of_interest], inplace=True)

    return dfs_dict
