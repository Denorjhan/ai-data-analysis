import streamlit as st
import pandas as pd
import boto3
from io import BytesIO
import time

st.sidebar.title("Session State")
st.sidebar.write(st.session_state)

# Load S3 credentials from secrets
s3 = boto3.client(
    "s3",
    aws_access_key_id=st.secrets["aws_access_key_id"],
    aws_secret_access_key=st.secrets["aws_secret_access_key"],
    region_name=st.secrets["region_name"],
)

bucket_name = st.secrets["bucket_name"]


# Function to upload file to S3
@st.cache_data()  # show_spinner="Uploading file...")
def upload_to_s3(file, bucket, object_name):
    try:
        # st.success(f"file: {file}")
        # st.success(f"bucket: {bucket}")
        # st.warning(f"object_name: {object_name}")
        time.sleep(5)
        # response = s3.upload_file(file, bucket, object_name)
    except Exception as e:
        st.error(f"Error uploading file to S3: {e}")


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
            df = pd.read_csv(file, encoding="utf-8")
            st.dataframe(df)
        except Exception as e:
            st.error(f"Error reading CSV file: {e}")

    if st.button("Add to Data Catalog"):  #! add warning on time and cost
        try:
            # upload to s3
            # convert to parquet
            # add to glue data catalog
            # create ddl
            # train model
            with st.status("Adding to Data Catalog...", expanded=True) as status:
                st.write("Uploading to S3...")
                upload_to_s3("file", "bucket_name", "file.name")
                time.sleep(2)
                st.write("Converting to parquet...")
                time.sleep(2)
                st.write("Adding to glue data catalog...")
                time.sleep(2)
                st.write("Creating ddl...")
                time.sleep(2)
                st.write("Training model...")
                time.sleep(2)
                status.update(label="Complete!", state="complete", expanded=True)
        except Exception as e:
            status.update(
                label="Error adding to data catalog", state="error", expanded=True
            )
        clear_session_state()
        st.success(
            f"Your data is now available in the data catalog and is ready for analysis."
        )


# Button to clear uploaded file, move to the bottom left corner
# st.button("Clear Uploaded Files", on_click=clear_session_state)
#     st.session_state.uploaded_files = []
