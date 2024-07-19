import datetime
import streamlit as st
import backend as be
import pandas as pd

RAW_BEHAVIOURAL_DATA = 'raw_behavioural_data'
STATISTICS = 'statistics'
BOUTS_DATA = 'bouts_data'
BOUT_STATISTICS = 'bout_statistics'
LOCATION_STATISTICS = 'location_statistics'
ALL_SUBJECTS = "All Subjects"
OUTLIERS = 'outliers'
DATA_TYPES = [RAW_BEHAVIOURAL_DATA, STATISTICS, BOUTS_DATA, BOUT_STATISTICS, LOCATION_STATISTICS, OUTLIERS]

class DataManager:
    def __init__(self):
        self.data_files = None
        self.data = None

    def load_data(self, data_files):
        self.data_files = data_files
        self.data = self._load_data()

    def _load_data(self):
        dfs = be.import_input_files(self.data_files)
        data = self._run_and_concatenate(dfs) if dfs else None
        return data

    @staticmethod
    def _run_and_concatenate(dfs):
        all_data = pd.concat(dfs.values())
        results = be.run_pipeline(all_data)
        return results

    def get_subjects(self):
        return [ALL_SUBJECTS] + list(self.data.keys()) if self.data else []

    def get_data(self, subject, data_type):
        if subject == ALL_SUBJECTS:
            return self._get_all_subjects_data(data_type)
        else:
            return self._get_single_subject_data(subject, data_type)

    def _get_all_subjects_data(self, data_type):
        all_dfs = []
        for subject, results in self.data.items():
            data = results[data_type]
            data['Subject'] = subject
            data.sort_index(axis=1, inplace=True)
            all_dfs.append(data)

        all_data_df = pd.concat(all_dfs)
        all_data_df.fillna(0, inplace=True)
        return all_data_df

    def _get_single_subject_data(self, subject, data_type):
        data = self.data[subject][data_type]
        data['Subject'] = subject
        data.fillna(0, inplace=True)
        data.sort_index(axis=1, inplace=True)
        return data

class UIManager:
    def __init__(self, data_manager):
        self.data_manager = data_manager

    def display(self):
        
        st.title("Behavioural analysis pipeline")
        st.divider()
        st.header("Welcome to the behavioural analysis pipeline!")
        st.write("This pipeline takes in .csv or .tsv files and outputs a csv file with behavioural analysis data and statistics.")

        data_files = st.file_uploader("Upload your data files", type=['csv', 'tsv'], accept_multiple_files=True)
        if data_files:
            self.data_manager.load_data(data_files)
            subjects = self.data_manager.get_subjects()
            selected_subjects = st.multiselect('Select one or more subjects', subjects)

            if selected_subjects: 
                for data_type in DATA_TYPES:
                    data_frames = []
                    for selected_subject in selected_subjects:
                        data = self.data_manager.get_data(selected_subject, data_type)
                        data_frames.append(data)

                    combined_data = pd.concat(data_frames, ignore_index=True)
                    if 'Subject' in combined_data.columns:
                        combined_data.set_index('Subject', inplace=True)
                    else: 
                        st.error('No subject column found in data. Please contact administrator')
                    st.subheader(f"{data_type.title().replace('_', ' ')}")
                    st.dataframe(combined_data)
            else:
                st.warning('No subjects selected. Select at least one subject to display data.')

def main():
    st.set_page_config(page_title="Behavioural analysis pipeline", page_icon="🧠", initial_sidebar_state="auto", 
                           menu_items={"About": f'Built using Streamlit and deployed using Heroku. \nLast deployed on {datetime.datetime.now().strftime("%d/%m/%Y at %H:%M:%S UTC")}'})
    data_manager = DataManager()
    ui_manager = UIManager(data_manager)
    ui_manager.display()

if __name__ == "__main__":
    main()
