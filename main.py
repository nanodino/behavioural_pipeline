import pandas as pd
from pathlib import Path
from glob import glob
import csv


def write_to_excel(summary_df: pd.DataFrame, full_data_df: pd.DataFrame) -> None:

    # first rearrange columns for full data df
    full_data_df = full_data_df[['Observation id', 'Subject', 'Behavior', 'Modifier',
                                 'Time_start', 'Time_stop', 'Duration (s)', 'end of last bout', 'interbout duration']]

    with pd.ExcelWriter("./clean_data.xlsx") as writer:
        full_data_df.round(2).to_excel(writer)
    with pd.ExcelWriter("./summary_data.xlsx") as writer:
        summary_df.round(2).to_excel(writer)


def get_input_data_files() -> dict[str, pd.DataFrame]:
    data_path = Path("./new_data/")
    # check if I can get all the data from the aggregated tables??
    data_files = glob('*.tsv', root_dir=data_path, recursive=False)
    input_data_tables_dict = {}
    columns_of_interest = ['Observation id',
                           'Subject', 'Behavior',
                           'Behavior type', 'Time']

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
    df['Modifier'] = df[['Behavior']].apply(
        lambda x: x['Behavior'].split("_", 1)[1] if len(x['Behavior'].split('_')) > 1 else '', axis=1)
    df['Behavior'] = df[['Behavior']].apply(lambda x: x['Behavior'].split(
        "_", 1)[0] if len(x['Behavior'].split('_')) > 1 else x['Behavior'], axis=1)

    return df


def match_start_and_stop(df: pd.DataFrame) -> pd.DataFrame:
    start_dataframe = df[df['Behavior type'] == 'START']
    stop_dataframe = df[df['Behavior type'] == 'STOP']

    start_dataframe['start_id'] = start_dataframe.groupby(
        ['Subject', 'Behavior', 'Observation id']).cumcount()
    stop_dataframe['stop_id'] = stop_dataframe.groupby(
        ['Subject', 'Behavior', 'Observation id']).cumcount()

    merged_df = pd.merge(start_dataframe, stop_dataframe, how='left', left_on=[
        'Subject', 'Behavior', 'Observation id', 'start_id', 'Modifier'],
        right_on=['Subject', 'Behavior', 'Observation id', 'stop_id', 'Modifier'], suffixes=["_start", "_stop"])

    merged_df.drop(columns=['start_id', 'stop_id',
                   'Behavior type_start', 'Behavior type_stop'], inplace=True)

    merged_df['Duration (s)'] = merged_df[['Time_stop', 'Time_start']].apply(
        lambda x: x['Time_stop'] - x['Time_start'], axis=1)

    return merged_df


def get_time_between_bouts(df: pd.DataFrame) -> pd.DataFrame:
    df['end of last bout'] = df.groupby(
        ['Subject', 'Behavior', 'Observation id'])['Time_stop'].shift()
    df['interbout duration'] = df[['Time_start', 'end of last bout']].apply(
        lambda x: x['Time_start'] - x['end of last bout'], axis=1)

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
matched = match_start_and_stop(modified)
interbout = get_time_between_bouts(matched)
summary = get_behaviour_data_for_each_subject(interbout)
write_to_excel(summary, interbout)
