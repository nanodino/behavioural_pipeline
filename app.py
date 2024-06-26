import datetime
import streamlit as st
import backend as be
import pandas as pd

titles = ['Data', 'Behaviour Stats', 'Bouts', 'Bout Stats', 'Location Stats']

def run_and_concatenate(dfs):
    all_data = pd.concat(dfs.values())
    return be.run_pipeline(all_data)

def main():
    st.set_page_config(page_title="Behavioural analysis pipeline", page_icon="🧠", initial_sidebar_state="auto", 
                       menu_items={"About": f'Last deployed on {datetime.datetime.now().strftime("%d/%m/%Y at %H:%M:%S UTC")}'})
    st.title("Behavioural analysis pipeline")
    st.divider()
    st.header("Welcome to the behavioural analysis pipeline!")
    st.write("This pipeline takes in .csv or .tsv files and outputs a csv file with behavioural analysis data and statistics.")
    data_files = st.file_uploader("Upload your data files", type=['csv', 'tsv'], accept_multiple_files=True)
    dfs = be.import_input_files(data_files)
    
    if dfs:
        all_data = run_and_concatenate(dfs)

        subjects = list(all_data.keys())
        subjects.insert(0, "All Subjects")
        selected_subject = st.selectbox('Select a subject', subjects)

        if selected_subject == "All Subjects":
            for title in ['raw_behavioural_data', 'statistics', 'bouts_data', 'bout_statistics', 'location_statistics']:
                all_dfs = []
                for subject, results in all_data.items():
                    data = results[title]
                    data['Subject'] = subject
                    data.sort_index(axis=1, inplace=True)
                    all_dfs.append(data)

                all_data_df = pd.concat(all_dfs)
                all_data_df.fillna(0, inplace=True)
                st.subheader(title.title().replace('_', ' '))
                st.dataframe(all_data_df)
        else:
            results = all_data[selected_subject]
            for title, data in results.items():
                data.fillna(0, inplace=True)
                data.sort_index(axis=1, inplace=True)
                st.subheader(title.title().replace('_', ' '))
                st.dataframe(data)

if __name__ == "__main__":
    main()
