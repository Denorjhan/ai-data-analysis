import streamlit as st

# st.sidebar.title("AI Data Explorer")
# st.logo(icon_image="😁", image="sdfs.png")

# Define the pages
pages = {
    "Data Management": [
        st.Page("upload_files.py", title="Data Sources", icon="📥"),
        # st.Page("dev/test_chat.py", title="Data Catalog", icon="🗃️"),
        st.Page("llm_training_data.py", title="LLM Training Data", icon="📚"),
        st.Page("dev/testing.py", title="Testing", icon="🧪"),
    ],
    "Data Analysis": [st.Page("app.py", title="Explore Your Data", icon="🔭")],
}

pg = st.navigation(pages)

# Set global page config
st.set_page_config(page_title="AI Data Explorer", page_icon="🧠", layout="wide")


pg.run()
