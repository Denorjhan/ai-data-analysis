import streamlit as st
import pandas as pd
import boto3
from io import BytesIO, StringIO
import time
import chardet
from utils import convert_df_to_parquet
from s3 import upload_to_s3
from glue import create_glue_crawler, run_glue_crawler
from aws_clients import config
from athena import generate_database_ddl
from orchestrator import engine
import traceback

st.sidebar.title("Session State")
st.sidebar.write(st.session_state)

st.title("Upload Your Data Files")


# Function to load files from session state
def load_files():
    if "uploaded_files" not in st.session_state:
        st.session_state.uploaded_files = []


# Function to save files to session state
def save_files(files):
    for file in files:
        if file not in st.session_state.uploaded_files:
            st.session_state.uploaded_files.append(file)


def clear_session_state():
    st.session_state.uploaded_files = []


# Load files from session state
load_files()

# File uploader widget
uploaded_files = st.file_uploader(
    "Select CSV files to analyze", accept_multiple_files=True, type=["csv"]
)

if uploaded_files:
    save_files(uploaded_files)

# Display uploaded files
if st.session_state.uploaded_files:
    st.header("Selected Files to Analyze:")
    for file in st.session_state.uploaded_files:
        st.write(file.name)
        # Reset file pointer to the beginning
        file.seek(0)
        # Read the file as a dataframe with specified encoding
        try:
            file_bytes = file.read()
            # Detect the encoding
            result = chardet.detect(file_bytes)
            encoding = result["encoding"]
            # Convert bytes to string using detected encoding
            csv_data = file_bytes.decode(encoding)
            # Convert CSV to Pandas DataFrame
            df = pd.read_csv(StringIO(csv_data))
            # Display DataFrame
            st.dataframe(df)
        except Exception as e:
            st.error(f"Error reading CSV file: {e}")

    if st.button("Add to Data Catalog"):  #! add warning on time and cost
        try:
            with st.status("Adding to Data Catalog...", expanded=True) as status:
                i = 1
                for file in st.session_state.uploaded_files:
                    file_name_no_ext = file.name.split(".")[0]

                    # convert to parquet
                    st.write(f"Converting file {file.name} to parquet...")
                    parquet_buffer = convert_df_to_parquet(df)

                    # upload to s3
                    st.write(f"Uploading file {i} to S3...")
                    parquet_file_key = f"parquet_data/{config['aws']['glue']['database']}/{file_name_no_ext}/{file_name_no_ext}.parquet"
                    upload_to_s3(
                        parquet_buffer, config["aws"]["s3"]["bucket"], parquet_file_key
                    )
                    i += 1

                # create & run crawler to add to glue data catalog
                st.write("Adding to glue data catalog...")
                crawler_name = f"{config['aws']['glue']['database']}_crawler"
                s3_target_path = f"s3://{config['aws']['s3']['bucket']}/parquet_data/{config['aws']['glue']['database']}"
                create_glue_crawler(
                    crawler_name, config["aws"]["glue"]["database"], s3_target_path
                )
                run_glue_crawler(crawler_name)

                st.write("Creating ddl...")
                ddl_statements = generate_database_ddl()

                st.write("Training model...")
                for table_name, ddl in ddl_statements.items():
                    print(f"adding table {table_name} to training data")
                    engine.train(ddl=ddl)
                print(engine.get_training_data())

                status.update(label="Complete!", state="complete", expanded=True)
                clear_session_state()
            st.success(
                f"Your data is now available in the data catalog and is ready for analysis."
            )

        except Exception as e:
            status.update(
                label="Error adding to data catalog", state="error", expanded=True
            )
            print(traceback.format_exc())


# Button to clear uploaded file, move to the bottom left corner
if st.button("Clear Uploaded Files", on_click=clear_session_state):
    st.success("Uploaded files cleared")
