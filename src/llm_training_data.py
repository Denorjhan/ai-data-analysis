import streamlit as st
from orchestrator import engine
import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)


st.sidebar.write(st.session_state)


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe

    Returns:
        pd.DataFrame: Filtered dataframe
    """

    df = df[["training_data_type", "content", "question"]]

    modification_container = st.container()

    with modification_container:
        st.subheader("Filter Training Data By:")
        to_filter_columns = st.multiselect(
            label="hidden text",
            placeholder="Choose a filter",
            options=df.columns,
            label_visibility="collapsed",
        )
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    _min,
                    _max,
                    (_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].str.contains(user_text_input)]

    return df


@st.experimental_dialog("Cast your vote")
def add_training_data():
    st.write("Select Data Type")
    reason = st.radio(
        label="Select the reason for your favorite:",
        options=["ddl", "sql/question pairs", "documentation"],
        index=None,
        key="data_type",
    )
    if reason == "ddl":
        st.write("You selected ddl.")
        ddl_content = st.text_area("Enter DDL content:")
        if st.button("Add DDL"):
            # engine.add_ddl(ddl_content)
            st.rerun()
    elif reason == "sql/question pairs":
        st.write("You selected sql/question pairs.")
        sql_question = st.text_input("Enter SQL question:")
        sql_content = st.text_area("Enter SQL content:")
        if st.button("Add SQL/Question Pair"):
            # engine.add_question_sql(sql_question, sql_content)
            st.rerun()
    elif reason == "documentation":
        st.write("You selected documentation.")
        documentation_content = st.text_area("Enter documentation content:")
        if st.button("Add Documentation"):
            # engine.add_documentation(documentation_content)
            st.rerun()


left, right = st.columns([0.85, 0.15], vertical_alignment="bottom")
left.title("LLM Training Data")
if right.button(label="Add Training Data", use_container_width=True, type="primary"):
    add_training_data()

df = engine.get_training_data()
column_config = {
    "training_data_type": st.column_config.Column(
        "Training Data Type",
        help="The type of training data. Can be: **ddl**, **documentation**, or **sql**",
        width="small",
    ),
    "content": st.column_config.TextColumn(
        "Content", help="The data that is used to train the AI model"
    ),
    "question": st.column_config.TextColumn(
        "SQL Question",
        help="The question used to generate the SQL query. N/A if Training Data Type is **NOT** 'sql'",
    ),
}

st.dataframe(
    filter_dataframe(df),
    use_container_width=True,
    # column_order=["training_data_type", "content", "question"], #TODO: remove and add to df in method
    column_config=column_config,
    hide_index=True,
)
