import pandas as pd
from pathlib import Path
from glob import glob

BR_AREAS = ['A', 'B', 'C', 'D']
RT_AREAS = ['H', 'I', 'J']
TWRL_AREAS = ['A', 'D']
BKFL_AREAS = ['E', 'F']
LIRDRT_AREAS = ['A', 'D']


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
    basic_stats = df.groupby(['Subject', 'Behavior', 'Modifier']).agg({'Observation id': ['count'],
                                                                       'Duration (s)': ['sum', 'mean', 'var']})
    basic_stats.columns = basic_stats.columns.map('_'.join)
    basic_stats.reset_index(inplace=True)
    # make a column for each behaviour-modifier pair
    basic_stats['behaviour_modifier'] = basic_stats[['Behavior', 'Modifier']].apply(
        lambda x: f'{x["Behavior"]}_{x["Modifier"]}', axis=1)
    basic_stats.drop(columns=['Behavior', 'Modifier'], inplace=True)
    basic_stats.set_index('Subject', inplace=True)
    basic_stats = basic_stats.pivot(columns='behaviour_modifier')
    basic_stats.columns = basic_stats.columns.map('_'.join)
    basic_stats.reset_index(inplace=True)
    basic_stats.fillna(0, inplace=True)
    basic_stats.set_index('Subject', inplace=True)
    # rename columns using function below
    basic_stats.rename(columns=get_column_name_for_summary_df, inplace=True)

    return basic_stats


def get_column_name_for_summary_df(column_name):
    column_name = column_name.replace('Observation id_', '')
    if column_name.startswith('count'):
        behaviour = column_name.split("_")[1]
        modifier = column_name.split("_")[2]
        return f'{behaviour} {modifier} bout count'
    elif column_name.startswith('Duration (s)_'):
        parts = column_name.split("_")
        behaviour = parts[2]
        modifier = parts[3]
        if parts[1] == 'sum':
            return f'{behaviour}_{modifier} total bout length (s)'
        elif parts[1] == 'mean':
            return f'{behaviour}_{modifier} mean bout length (s)'
        elif parts[1] == 'var':
            return f'{behaviour}_{modifier} variance bout length (s)'
    return column_name


def get_proportion_of_time_in_area_for_behaviour_for_subject(subject, behaviour, modifier):
    pass


test_output = get_input_data_files()
concatenated = concatenate_data_from_all_observations(test_output)
modified = get_behaviour_modifiers(concatenated)
matched = match_start_and_stop(modified)
interbout = get_time_between_bouts(matched)
summary = get_behaviour_data_for_each_subject(interbout)
write_to_excel(summary, interbout)
