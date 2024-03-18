import datetime
import streamlit as st
import backend as be
import pandas as pd

titles = ['Data', 'Stats', 'Proportion of time doing each behaviour in each area']

def run_and_concatenate(subjects, dfs):
    all_results = [[], [], []]
    for subject, data in subjects.items():
        results = be.run_pipeline(subject, data)
        for i in range(3):
            all_results[i].append(results[i])

    all_data = [pd.concat(results) for results in all_results]
    return all_data

def main():
    st.set_page_config(page_title="Behavioural analysis pipeline", page_icon="🧠", initial_sidebar_state="auto", 
                       menu_items={"About": f'Last deployed on {datetime.datetime.now().strftime("%d/%m/%Y at %H:%M:%S UTC")}'})
    st.title("Behavioural analysis pipeline")
    st.divider()
    st.header("Welcome to the behavioural analysis pipeline!")
    st.write("This pipeline takes in .csv or .tsv files and outputs an excel file with behavioural analysis data and statistics.")
    data_files = st.file_uploader("Upload your data files", type=['csv', 'tsv'], accept_multiple_files=True)
    dfs = be.import_input_files(data_files)
    if dfs:
        all_subjects = {}
        for file, data in dfs.items():
            data_by_subject = be.separate_data_by_subject(data)
            all_subjects.update(data_by_subject)

        subject_list = list(all_subjects.keys())
        subject_list.insert(0, 'All Subjects')
        selected_subject = st.selectbox('Select a subject to run the pipeline for', subject_list)

        if selected_subject in all_subjects:
            subjects = {selected_subject: all_subjects[selected_subject] for file, data in dfs.items()}
            all_data = run_and_concatenate(subjects, dfs)
        elif selected_subject == 'All Subjects':
            all_data = run_and_concatenate(all_subjects, dfs)

        for title, data in zip(titles, all_data):
            data.fillna(0, inplace=True)
            data.sort_index(axis=1, inplace=True)
            st.subheader(title)
            st.dataframe(data)


if __name__ == "__main__":
    main()
