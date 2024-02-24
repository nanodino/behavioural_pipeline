import pandas as pd
from intervaltree import Interval, IntervalTree
import numpy as np

BR_AREAS = ['A', 'B', 'C', 'D']
RT_AREAS = ['H', 'I', 'J']
TWRL_AREAS = ['A', 'D']
BKFL_AREAS = ['E', 'F']
LIRDRT_AREAS = ['A', 'D']


def separate_data_by_subject(data: pd.DataFrame) -> dict[str, pd.DataFrame]:
    print('Separating data by subject')
    data_by_subject = {}
    for subject in data['Subject'].unique():
        data_by_subject[subject] = data[data['Subject'] == subject]
    return data_by_subject


def get_behaviour_modifiers(df: pd.DataFrame, behaviour: str) -> pd.DataFrame:
    df['Modifier'] = df[['Behavior']].apply(
        lambda x: x['Behavior'].split("_", 1)[1] if len(x['Behavior'].split('_')) > 1 else '', axis=1)
    df['Behavior'] = df[['Behavior']].apply(lambda x: x['Behavior'].split(
        "_", 1)[0] if len(x['Behavior'].split('_')) > 1 else x['Behavior'], axis=1)

    return df

def get_bouts(df: pd.DataFrame, gap: float) -> pd.DataFrame:
    '''
    This function creates an interval tree,
    so that multiple overlapping behaviours 
    can be considered part of the same bout.
    Bouts are merged if the time between them 
    is shorter than the gap, in seconds.
    '''
    df = df.sort_values(['Time'], ascending=[True])
    df = match_start_and_stop_for_behaviour(df)
    raw_tree = IntervalTree(Interval(start, stop) for start, stop in zip(df['Time_start'], df['Time_stop']))

    raw_tree.merge_overlaps()
    intervals = [(i.begin, i.end) for i in raw_tree]
    intervals.sort(key=lambda x: x[0]) #i.begin

    merged = [intervals[0]]
    for current in intervals:
        previous = merged[-1]
        if current[0] - previous[1] <= gap:
            merged[-1] = (previous[0], max(previous[1], current[1]))
        else:
            merged.append(current)

    merged_tree = IntervalTree(Interval(start, stop, i+1) for i, (start, stop) in enumerate(merged))

    intervals_df = pd.DataFrame([(interval.begin, interval.end, interval.data) for interval in merged_tree], 
                                columns=['Time_start', 'Time_stop', 'bout_id'])
    df['bout_id'] = np.nan

    for i, row in df.iterrows():
        for j, interval_row in intervals_df.iterrows():
            if interval_row['Time_start'] <= row['Time_start'] <= interval_row['Time_stop']:
                df.at[i, 'bout_id'] = interval_row['bout_id']
                break

    return df

def get_behaviour_data_for_each_subject(df: pd.DataFrame) -> pd.DataFrame:
    basic_stats = df.groupby(['Subject', 'Behavior', 'Modifier']).agg({'Observation id': ['count'],
                                                                       'Behaviour Duration (s)': ['sum', 'mean', 'var', 'std']})
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
    return basic_stats


def get_column_names_for_summary_table(name: str) -> str:
    pass

def get_time_doing_behaviour(df: pd.DataFrame) -> pd.DataFrame:
    time_df = df.groupby(['Subject', 'Behavior', 'Modifier'])[
        'Behaviour Duration (s)'].agg('sum')
    time_df = time_df.reset_index()
    time_df.rename(columns={'Behaviour Duration (s)': 'total time'}, inplace=True)
    # pivot to get the total time for each behaviour and the proportion of time spent doing each modifier
    pivot_df = time_df.pivot_table(index=['Subject', 'Behavior'], columns=[
        'Modifier'], values='total time', aggfunc='sum')
    pivot_df = pivot_df.div(pivot_df.sum(axis=1), axis=0)
    pivot_df = pivot_df.reset_index()
    pivot_df.fillna(0, inplace=True)
    pivot_df.set_index('Subject', inplace=True)

    return pivot_df

def get_total_stereotyping_duration(df: pd.DataFrame) -> pd.DataFrame:
    pass

def write_to_excel(df: pd.DataFrame) -> None:
    pass


def match_start_and_stop_for_behaviour(df: pd.DataFrame) -> pd.DataFrame:
    start_dataframe = df[df['Behavior type'] == 'START']
    stop_dataframe = df[df['Behavior type'] == 'STOP']

    start_dataframe['start_id'] = start_dataframe.groupby(
        ['Subject', 'Behavior', 'Observation id', 'Observation date',
         'Observation duration']).cumcount()
    stop_dataframe['stop_id'] = stop_dataframe.groupby(
       ['Subject', 'Behavior', 'Observation id', 'Observation date',
         'Observation duration']).cumcount()

    merged_df = pd.merge(start_dataframe, stop_dataframe, how='left', left_on=[
        'Subject', 'Behavior', 'Observation id', 'start_id', 'Modifier', 'Observation date', 'Observation duration'],
        right_on=[
            'Subject', 'Behavior', 'Observation id', 'stop_id', 'Modifier', 'Observation date', 'Observation duration'], 
            suffixes=["_start", "_stop"])

    merged_df.drop(columns=['start_id', 'stop_id',
                   'Behavior type_start', 'Behavior type_stop'], inplace=True)

    merged_df['Behaviour Duration (s)'] = merged_df[['Time_stop', 'Time_start']].apply(
        lambda x: x['Time_stop'] - x['Time_start'], axis=1)

    return merged_df 


def import_input_files(data_files) ->  dict[str, pd.DataFrame]:
    input_data_tables_dict= {}
    columns_of_interest = ['Observation id',
                           'Subject', 'Behavior',
                           'Behavior type', 'Time', 
                           'Observation date', 'Observation duration']
    if data_files is not None:
        for file in data_files:
            file.seek(0)
            print(f'Reading columns for file {file.name}')
            input_data_tables_dict[file.name] = pd.read_csv(
                file, delimiter='\t')
            input_data_tables_dict[file.name].drop(columns=[col for col in input_data_tables_dict[file.name]
                                                            if col not in columns_of_interest], inplace=True)

    return input_data_tables_dict


def run_pipeline(subject, df):
    data_by_subject = get_behaviour_modifiers(df, 'Behavior')
    data_by_subject = get_bouts(data_by_subject, 10)
    stats_by_subject = get_behaviour_data_for_each_subject(data_by_subject)
    summary_df =  get_time_doing_behaviour(data_by_subject)
    return data_by_subject, stats_by_subject, summary_df