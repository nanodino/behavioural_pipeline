import pandas as pd
from pathlib import Path
from glob import glob
from typing import List

BR_AREAS = ['A', 'B', 'C', 'D']
RT_AREAS = ['H', 'I', 'J']
TWRL_AREAS = ['A', 'D']
BKFL_AREAS = ['E', 'F']
LIRDRT_AREAS = ['A', 'D']


def write_to_excel(summary_dfs: pd.DataFrame, full_data_df: pd.DataFrame, partitioned) -> None:
    full_data_df = full_data_df[['Observation id', 'Subject', 'Behavior', 'Modifier',
                                 'Time_start', 'Time_stop', 'Duration (s)', 'end of last bout', 'interbout duration']]

    with pd.ExcelWriter("./observation_data.xlsx") as writer:
        full_data_df.round(2).to_excel(writer, sheet_name='Full data')
        partitioned.round(2).to_excel(writer, sheet_name='time proportions')

        names = ['Bout counts', 'Total bout length', 'Mean bout length',
                 'Bout length variance', 'Bout length standard deviation', 'Interbout duration Statistics']

        for df in summary_dfs:
            df = df[['Subject'] + [col for col in df.columns if col != 'Subject']]
            df.round(2).to_excel(writer, sheet_name=names.pop(0))


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
        ['Subject', 'Observation id'])['Time_stop'].shift()
    df['interbout duration'] = df[['Time_start', 'end of last bout']].apply(
        lambda x: x['Time_start'] - x['end of last bout'], axis=1)

    return df


def get_behaviour_data_for_each_subject(df):
    # get count of bouts, total bout length, mean bout length, variance for bout length
    basic_stats = df.groupby(['Subject', 'Behavior', 'Modifier']).agg({'Observation id': ['count'],
                                                                       'Duration (s)': ['sum', 'mean', 'var', 'std']})
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
    basic_stats['interbout duration mean'] = df.groupby(['Subject'])[
        'interbout duration'].mean()
    basic_stats['interbout duration variance'] = df.groupby(['Subject'])[
        'interbout duration'].var()
    basic_stats['interbout duration standard deviation'] = df.groupby(['Subject'])[
        'interbout duration'].std()
    basic_stats.reset_index(inplace=True)

    return basic_stats


def get_column_name_for_summary_df(column_name):
    column_name = column_name.replace('Observation id_', '')
    if column_name.startswith('count'):
        behaviour = column_name.split("_")[1]
        modifier = column_name.split("_")[2]
        return f'{behaviour}_{modifier} bout count'
    elif column_name.startswith('Duration (s)_'):
        parts = column_name.split("_")
        behaviour = parts[2]
        modifier = parts[3]
        if parts[1] == 'sum':
            return f'{behaviour}_{modifier} total bout length (s)'
        elif parts[1] == 'mean':
            return f'{behaviour}_{modifier} mean bout length (s)'
        elif parts[1] == 'var':
            return f'{behaviour}_{modifier}bout length  variance (s2)'
        elif parts[1] == 'std':
            return f'{behaviour}_{modifier} bout length standard deviation (s)'
    return column_name

# TODO: wtf is this even, make good


def divide_statistics(df) -> List[pd.DataFrame]:
    means = df[df.columns[df.columns.str.contains('mean') & ~df.columns.str.contains(
        'interbout') | df.columns.str.contains('Subject')]]
    variances = df[df.columns[df.columns.str.contains('var') & ~df.columns.str.contains(
        'interbout') | df.columns.str.contains('Subject')]]
    stds = df[df.columns[df.columns.str.contains('standard') & ~df.columns.str.contains(
        'interbout') | df.columns.str.contains('Subject')]]
    counts = df[df.columns[df.columns.str.contains(
        'count') | df.columns.str.contains('Subject')]]
    interbout = df[df.columns[df.columns.str.contains(
        'interbout') | df.columns.str.contains('Subject')]]
    totals = df[df.columns[df.columns.str.contains(
        'total') | df.columns.str.contains('Subject')]]
    return [counts, totals, means, variances, stds, interbout]

# TODO: rename, this is dumb


def get_total_time_doing_behaviour(df: pd.DataFrame) -> pd.DataFrame:
    time_df = df.groupby(['Subject', 'Behavior', 'Modifier'])[
        'Duration (s)'].agg('sum')
    time_df = time_df.reset_index()
    time_df.rename(columns={'Duration (s)': 'total time'}, inplace=True)
    # pivot to get the total time for each behaviour and the proportion of time spent doing each modifier
    pivot_df = time_df.pivot_table(index=['Subject', 'Behavior'], columns=[
        'Modifier'], values='total time', aggfunc='sum')
    pivot_df = pivot_df.div(pivot_df.sum(axis=1), axis=0)
    pivot_df = pivot_df.reset_index()
    pivot_df.fillna(0, inplace=True)
    pivot_df.set_index('Subject', inplace=True)

    return pivot_df


test_output = get_input_data_files()
concatenated = concatenate_data_from_all_observations(test_output)
modified = get_behaviour_modifiers(concatenated)
matched = match_start_and_stop(modified)
interbout = get_time_between_bouts(matched)
summary = get_behaviour_data_for_each_subject(interbout)
partitioned = get_total_time_doing_behaviour(interbout)
divided = divide_statistics(summary)
write_to_excel(divided, interbout, partitioned)
