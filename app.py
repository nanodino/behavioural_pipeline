import datetime
import streamlit as st
import backend as be
import pandas as pd

def main():
    st.set_page_config(page_title="Behavioural analysis pipeline", page_icon="🧠", layout="wide", initial_sidebar_state="auto", 
                       menu_items={"About": f'Last deployed on {datetime.datetime.now().strftime("%d/%m/%Y at %H:%M:%S UTC")}'})
    st.title("Behavioural analysis pipeline")
    st.divider()
    st.header("Welcome to the behavioural analysis pipeline!")
    st.write("This pipeline takes in .tsv files and outputs an excel file with behavioural analysis data and statistics.")
    st.write("Please upload your data files below.")
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
            all_results = []
            for file, data in dfs.items():
                data_by_subject = be.separate_data_by_subject(data)
                if selected_subject in data_by_subject:
                    result = be.run_pipeline(selected_subject, data_by_subject[selected_subject])
                    all_results.append(result)

            all_data = pd.concat(all_results)
            st.dataframe(all_data)
        elif selected_subject == 'All Subjects':
            all_results = []
            for subject, data in all_subjects.items():
                result = be.run_pipeline(subject, data)
                all_results.append(result)

            all_data = pd.concat(all_results)
            st.dataframe(all_data)


if __name__ == "__main__":
    main()
