import pandas as pd
from intervaltree import Interval, IntervalTree
import numpy as np

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
    observation_dates = df['Observation date'].unique()
    all_bouts = pd.DataFrame()
    bout_id = 1

    for date in observation_dates:
        df_date = df[df['Observation date'] == date]
        df_date = df_date.sort_values(['Time'], ascending=[True])
        df_date = match_start_and_stop_for_behaviour(df_date)
        raw_tree = IntervalTree(Interval(start, stop) for start, stop in zip(df_date['Time_start'], df_date['Time_stop']))

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

        merged_tree = IntervalTree(Interval(start, stop, bout_id + i) for i, (start, stop) in enumerate(merged))

        intervals_df = pd.DataFrame([(interval.begin, interval.end, interval.data) for interval in merged_tree], 
                                    columns=['Time_start', 'Time_stop', 'bout_id'])
        df_date['bout_id'] = np.nan

        for i, row in df_date.iterrows():
            for j, interval_row in intervals_df.iterrows():
                if interval_row['Time_start'] <= row['Time_start'] <= interval_row['Time_stop']:
                    df_date.at[i, 'bout_id'] = interval_row['bout_id']
                    break

        df_date = identify_mixed_bouts(df_date)
        bout_id = df_date['bout_id'].max() + 1
        all_bouts = pd.concat([all_bouts, df_date])

    return all_bouts

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

def identify_mixed_bouts(df: pd.DataFrame) -> pd.DataFrame:
    bout_behavior_counts = df.groupby('bout_id')['Behavior'].nunique()
    df['mixed_bout'] = df['bout_id'].map(bout_behavior_counts > 1)
    return df

def generate_bouts_df(df: pd.DataFrame) -> pd.DataFrame:
    # generate a dataframe with the total time for each bout
    bouts_df = df.groupby(['Subject', 'bout_id', 'mixed_bout'])[
        'Behaviour Duration (s)'].agg('sum')
    bouts_df = bouts_df.reset_index()
    bouts_df.set_index('Subject', inplace=True)
    return bouts_df

def calculate_bout_stats(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={'Behaviour Duration (s)': 'Bout Duration (s)'}, inplace=True)
    df['mixed_bout'] = df['mixed_bout'].astype(str) #avoids mixed types
    bout_stats = df.groupby(['mixed_bout', 'Subject'])['Bout Duration (s)'].agg(['sum', 'mean', 'std', 'var'])
    bout_stats.columns = bout_stats.columns.map(''.join)
    bout_stats.reset_index(inplace=True)
    
    all_bout_stats = df.groupby('Subject')['Bout Duration (s)'].agg(['sum', 'mean', 'std', 'var'])
    all_bout_stats.columns = all_bout_stats.columns.map(''.join)
    all_bout_stats.reset_index(inplace=True)
    all_bout_stats['mixed_bout'] = 'All'
    
    bout_stats = pd.concat([bout_stats, all_bout_stats], ignore_index=True)
    bout_stats.set_index(['mixed_bout', 'Subject'], inplace=True)
    
    return bout_stats

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


def run_pipeline(df):

    data_by_subject = separate_data_by_subject(df)
    results = {}
    for subject in data_by_subject:
        subject_data = get_behaviour_modifiers(data_by_subject[subject], 'Behavior')
        subject_data = get_bouts(subject_data, 10)
        stats = get_behaviour_data_for_each_subject(subject_data)
        bouts_data = generate_bouts_df(subject_data)
        bout_stats = calculate_bout_stats(bouts_data)
        summary_df =  get_time_doing_behaviour(subject_data)

        results[subject] = {
            'data': subject_data,
            'stats': stats,
            'bouts_data': bouts_data,
            'bout_stats': bout_stats,
            'summary_df': summary_df
        }

    return results