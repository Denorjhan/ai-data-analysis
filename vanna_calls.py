import streamlit as st

from main import engine


# @st.cache_resource(ttl=3600)
# def setup_vanna():
    # engine = VannaDefault(api_key=st.secrets.get("VANNA_API_KEY"), model='chinook')
    # engine.connect_to_sqlite("https://vanna.ai/Chinook.sqlite")
    # return engine


@st.cache_data(show_spinner="Generating sample questions ...")
def generate_questions_cached():
    
    return engine.generate_questions()


@st.cache_data(show_spinner="Generating SQL query ...")
def generate_sql_cached(question: str):
    
    return engine.generate_sql(question=question, allow_llm_to_see_data=True)


@st.cache_data(show_spinner="Checking for valid SQL ...")
def is_sql_valid_cached(sql: str):
    
    return engine.is_sql_valid(sql=sql)


@st.cache_data(show_spinner="Running SQL query ...")
def run_sql_cached(sql: str):
    
    return engine.run_sql(sql=sql)


@st.cache_data(show_spinner="Checking if we should generate a chart ...")
def should_generate_chart_cached(df):
    
    return engine.should_generate_chart(df=df)


@st.cache_data(show_spinner="Generating Plotly code ...")
def generate_plotly_code_cached(question, sql, df):
    
    code = engine.generate_plotly_code(question=question, sql=sql, df=df)
    return code


@st.cache_data(show_spinner="Running Plotly code ...")
def generate_plot_cached(code, df):
    
    return engine.get_plotly_figure(plotly_code=code, df=df)


@st.cache_data(show_spinner="Generating followup questions ...")
def generate_followup_cached(question, sql, df):
    
    return engine.generate_followup_questions(question=question, sql=sql, df=df)


@st.cache_data(show_spinner="Generating summary ...")
def generate_summary_cached(question, df):
    
    return engine.generate_summary(question=question, df=df)
