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


def concatenate_data_from_all_observations(all_input_files) -> pd.DataFrame:
    return pd.concat(all_input_files)


def get_behaviour_modifiers(df: pd.DataFrame) -> pd.DataFrame:
    df['Modifier #1'] = df[['Behavior', 'Modifier #1']].apply(
        lambda x: x['Behavior'].split("_", 1)[1] if len(x['Behavior'].split('_')) > 1 else x['Modifier #1'], axis=1)
    df['Behavior'] = df[['Behavior']].apply(lambda x: x['Behavior'].split(
        "_", 1)[0] if len(x['Behavior'].split('_')) > 1 else x['Behavior'], axis=1)

    return df


def assign_cage_number_from_observation_id_to_subject(df: pd.DataFrame) -> pd.DataFrame:
    '''
    gets cage number from observation id and assigns it to subject 
    so that e.g. DBA becomes 18-DBA or 23-DBA, allowing the Subject 
    column to differentiate between mice of the same strain in 
    different cages.
    '''
    df['Subject'] = df[['Observation id', 'Subject']].apply(
        lambda x: f'{x["Observation id"].split(" ", 1)[0]}-{x["Subject"]}', axis=1)
    return df


def get_bout_duration_from_start_and_stop_times(df):
    df['Duration (s)'] = df[['Start (s)', 'Stop (s)']].apply(
        lambda x: x['Stop (s)'] - x['Start (s)'], axis=1)
    return df


def get_interbout_durations(df):
    ### needs some work###
    df['previous_stop_time'] = df['Stop (s)'].shift()
    df['previous_subject'] = df['Subject'].shift()
    df['previous_observation'] = df['Observation id'].shift()
    # confirm whether this needs to be part of it
    # df['previous_behaviour'] = df['Behavior'].shift()

    df['interbout duration'] = df[['Observation id', 'Subject', 'Start (s)', 'previous_stop_time',
                                   'previous_subject', 'previous_observation']].apply(
        lambda x: x['Start (s)'] - x['previous_stop_time']
        if (x['Subject'] == x['previous_subject']
            and x['Observation id'] == x['previous_observation']) else float('nan'), axis=1)
    df.drop(columns=['previous_stop_time',
            'previous_subject', 'previous_observation'], inplace=True)
    return df


def get_behaviour_data_for_each_subject(df):
    # get count of bouts, total bout length, mean bout length, variance for bout length
    basic_stats = df.groupby(['Subject', 'Behavior']).agg({'Observation id': ['count'],
                                                           'Duration (s)': ['sum', 'mean', 'var'],
                                                           'interbout duration': ['mean', 'var']})
    return basic_stats


# testing while working !!
test_output = get_input_data_files()
concatenated = concatenate_data_from_all_observations(test_output)
modified = get_behaviour_modifiers(concatenated)
with_cage = assign_cage_number_from_observation_id_to_subject(modified)
with_bout_duration = get_bout_duration_from_start_and_stop_times(with_cage)
with_interbout_duration = get_interbout_durations(with_bout_duration)
to_output = get_behaviour_data_for_each_subject(with_bout_duration)
write_to_excel(to_output)
