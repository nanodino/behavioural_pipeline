import streamlit as st
import pandas as pd

BR_AREAS = ['A', 'B', 'C', 'D']
RT_AREAS = ['H', 'I', 'J']
TWRL_AREAS = ['A', 'D']
BKFL_AREAS = ['E', 'F']
LIRDRT_AREAS = ['A', 'D']


def import_input_files() ->  dict[str, pd.DataFrame]:
    input_data_tables_dict= {}
    columns_of_interest = ['Observation id',
                           'Subject', 'Behavior',
                           'Behavior type', 'Time', 'Observation date', 'Observation duration']
    data_files = st.file_uploader("Upload your data files", type=['csv', 'tsv'], accept_multiple_files=True)
    if data_files is not None:
        for file in data_files:
            file.seek(0)
            print(f'Reading columns for file {file.name}')
            input_data_tables_dict[file.name] = pd.read_csv(
                file, delimiter='\t')
            input_data_tables_dict[file.name].drop(columns=[col for col in input_data_tables_dict[file.name]
                                                            if col not in columns_of_interest], inplace=True)

    return input_data_tables_dict

def concatenate_data_from_all_observations(input_data_tables_dict) -> pd.DataFrame:
    print('Concatenating data from all observations')
    data = pd.concat(input_data_tables_dict.values(), ignore_index=True)
    return data

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
def run_pipeline(dfs: dict[str, pd.DataFrame]):
    data = concatenate_data_from_all_observations(dfs)
    data_by_subject = separate_data_by_subject(data)
    for subject, df in data_by_subject.items():
        data_by_subject[subject] = get_behaviour_modifiers(df, 'Behavior')
    st.dataframe(data_by_subject['DMO-10'])         # for demo purposes

def main():
    st.title("Behavioural analysis pipeline")
    st.header("Welcome to the behavioural analysis pipeline!")
    st.write("This pipeline takes in .tsv files and outputs an excel file with behavioural analysis data and statistics.")
    st.write("Please upload your data files below.")
    dfs = import_input_files()
    # only run if there are files
    if dfs:
        run_pipeline(dfs)


if __name__ == "__main__":
    main()
