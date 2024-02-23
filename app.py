import datetime
import streamlit as st
import backend as be

def main():
    st.set_page_config(page_title="Behavioural analysis pipeline", page_icon="🧠", layout="wide", initial_sidebar_state="auto", 
                       menu_items={"About": f'Last deployed on {datetime.datetime.now().strftime("%d/%m/%Y at %H:%M:%S")}'})
    st.title("Behavioural analysis pipeline")
    st.divider()
    st.header("Welcome to the behavioural analysis pipeline!")
    st.write("This pipeline takes in .tsv files and outputs an excel file with behavioural analysis data and statistics.")
    st.write("Please upload your data files below.")
    data_files = st.file_uploader("Upload your data files", type=['csv', 'tsv'], accept_multiple_files=True)
    dfs = be.import_input_files(data_files)
    # only run if there are files
    if dfs:
        for file, data in dfs.items():
            st.write(f'Running pipeline for file {file}')
            data_by_subject = be.separate_data_by_subject(data)
            for subject, df in data_by_subject.items():
                data = be.run_pipeline(subject, df)
                st.dataframe(data)

if __name__ == "__main__":
    main()
