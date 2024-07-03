import streamlit as st
from vanna_calls import generate_sql_cached, is_sql_valid_cached, run_sql_cached, generate_plotly_code_cached, generate_plot_cached, generate_summary_cached, generate_followup_cached, should_generate_chart_cached
import pandas as pd
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
import uuid

def filter_dataframe(df: pd.DataFrame, key) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    if key not in st.session_state.keys():
        st.session_state[key] = False
        
    # modify = st.checkbox("Add filters", on_change=toggle_state, args=(key,))
    modify = st.checkbox("Add filters", key=key)

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")
            # Treat columns with < 10 unique values as categorical

            if is_categorical_dtype(df[column]) or df[column].nunique() <= 2:
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


def new_convo():
    st.session_state.messages = [
        {"role": "assistant", "content": "Ask me a question about your data to get started!", "avatar": ai_icon, "content_type": "text"}
    ]

ai_icon = "🧠"
user_icon = "😃"

def set_user_question(question):
    st.session_state.messages.append({"role": "user", "content": question, "avatar": user_icon, "content_type": "text"})

def handle_text(content):
    content

def handle_code(content):
    language = "python" if "plotly" in content else "sql"
    st.code(content, language=language, line_numbers=True)

def handle_error(content):
    st.error(content)

def handle_dataframe(content, key):
    st.dataframe(filter_dataframe(content, key))
    
def toggle_state(key):
    st.session_state[key] = not st.session_state[key]

if "show_sql" not in st.session_state.keys():
    st.session_state.show_sql = True

if "show_table" not in st.session_state.keys():
    st.session_state.show_table = True

if "show_plotly_code" not in st.session_state.keys():
    st.session_state.show_plotly_code = False

if "show_chart" not in st.session_state.keys():
    st.session_state.show_chart = False

if "show_summary" not in st.session_state.keys():
    st.session_state.show_summary = False

if "show_followup" not in st.session_state.keys():
    st.session_state.show_followup = False

# st.write(st.session_state)
      
st.sidebar.title("Output Settings")
#TODO: on change toggle state
st.sidebar.checkbox("Show SQL", value=st.session_state.show_sql, on_change=toggle_state, args=("show_sql",))
st.sidebar.checkbox("Show Table", value=st.session_state.show_table, on_change=toggle_state, args=("show_table",))
st.sidebar.checkbox("Show Plotly Code", value=st.session_state.show_plotly_code, on_change=toggle_state, args=("show_plotly_code",))
st.sidebar.checkbox("Show Chart", value=st.session_state.show_chart, on_change=toggle_state, args=("show_chart",))
st.sidebar.checkbox("Show Summary", value=st.session_state.show_summary, on_change=toggle_state, args=("show_summary",))
st.sidebar.checkbox("Show Follow-up Questions", value=st.session_state.show_followup, on_change=toggle_state, args=("show_followup",))
st.sidebar.button("Clear Chat History", on_click=new_convo, use_container_width=True, type="primary")

st.title("AI Data Explorer")
st.sidebar.write(st.session_state)

# if "explanation_status" not in st.session_state:
#     st.session_state.explanation_status = False


# initialize_messages()
if "messages" not in st.session_state.keys(): 
        new_convo()
        
# Initialize the prompt status if it doesn't exist
if prompt := st.chat_input("Your question"): 
    set_user_question(prompt)


# Mapping content types to their handlers
content_handlers = {
    "text": handle_text, # can also handle dataframes and charts
    "code": handle_code,
    "error": handle_error,
    "dataframe": handle_dataframe
}

# print prior chat messages when streamlit runs the script from top to bottom
for message in st.session_state.messages: 
    with st.chat_message(message["role"], avatar=message["avatar"]):
        content = message["content"]
        content_type = message.get("content_type", "text")
        
        # Get the handler function from the dictionary, default to handle_default
        handler = content_handlers.get(content_type, handle_text)
        if content_type == "dataframe":
            handler(content, message["key"])
        else:
            handler(content)


# If last message is not from assistant, generate a new response
if st.session_state.messages[-1]["role"] != "assistant":
    with st.spinner("Thinking..."):
        my_question = st.session_state.messages[-1]["content"]
        # display the sql
        sql, sql_explanation, clarification_request = generate_sql_cached(question=my_question, explain=True)
        if sql:
            if st.session_state.get("show_sql", True):
                assistant_message_sql = st.chat_message(
                    "assistant", avatar=ai_icon 
                )
                assistant_message_sql.code(sql, language="sql", line_numbers=True)
                st.session_state.messages.append({"role": "assistant", "content": sql, "avatar": ai_icon, "content_type": "code"})
                # print explanation of sql
                assistant_message_sql.write(sql_explanation)
                st.session_state.messages.append({"role": "assistant", "content": sql_explanation, "avatar": ai_icon, "content_type": "text"})
            if clarification_request:
                assistant_message_clarification = st.chat_message(
                    "assistant", avatar=ai_icon
                )
                assistant_message_clarification.write(clarification_request)
                st.session_state.messages.append({"role": "assistant", "content": clarification_request, "avatar": ai_icon, "content_type": "text"})

            # else:
            #     assistant_message = st.chat_message(
            #         "assistant", avatar=ai_icon
            #     )
            #     assistant_message.write(sql)
            #     st.stop()

            # display the table
            df = run_sql_cached(sql=sql, question=my_question)
            if df is not None:
                st.session_state["df"] = df

            if st.session_state.get("df") is not None:
                if st.session_state.get("show_table", True):
                    df = st.session_state.get("df")
                    assistant_message_table = st.chat_message(
                        "assistant",
                        avatar=ai_icon,
                    )
                    key = uuid.uuid4()
                    with assistant_message_table:
                        st.dataframe(filter_dataframe(df, key))
                    st.session_state.messages.append({"role": "assistant", "content": df, "avatar": ai_icon, "content_type": "dataframe", "key": key})
                    
            # display chart code
            if should_generate_chart_cached(df=df):
                code = generate_plotly_code_cached(question=my_question, sql=sql, df=df)
                if st.session_state.get("show_plotly_code", True):
                    assistant_message_plotly_code = st.chat_message(
                        "assistant",
                    avatar=ai_icon,
                    )
                    assistant_message_plotly_code.code(
                        code, language="python", line_numbers=True
                    )
                    st.session_state.messages.append({"role": "assistant", "content": code, "avatar": ai_icon, "content_type": "code"})
                
            # display chart
                if code is not None and code != "":
                    if st.session_state.get("show_chart", True):
                        assistant_message_chart = st.chat_message(
                            "assistant",
                            avatar=ai_icon,
                        )
                        fig = generate_plot_cached(code=code, df=df)
                        if fig is not None:
                            assistant_message_chart.plotly_chart(fig)
                            st.session_state.messages.append({"role": "assistant", "content": fig, "avatar": ai_icon, "content_type": "chart"})
                        else:
                            assistant_message_chart.error("I couldn't generate a chart")
                            st.session_state.messages.append({"role": "assistant", "content": "I couldn't generate a chart", "avatar": ai_icon, "content_type": "error"})

            # display the summary
            if st.session_state.get("show_summary", True):
                assistant_message_summary = st.chat_message(
                    "assistant",
                    avatar=ai_icon,
                )
                summary = generate_summary_cached(question=my_question, df=df)
                if summary is not None:
                    assistant_message_summary.text(summary)
                    st.session_state.messages.append({"role": "assistant", "content": summary, "avatar": ai_icon, "content_type": "text"})

            # display the followup questions
            if st.session_state.get("show_followup", True):
                assistant_message_followup = st.chat_message(
                    "assistant",
                    avatar=ai_icon,
                )
                followup_questions = generate_followup_cached(
                    question=my_question, sql=sql, df=df
                )
                st.session_state["df"] = None

                if len(followup_questions) > 0:
                    assistant_message_followup.text(
                        "Here are some possible follow-up questions"
                    )
                    # Print the first 5 follow-up questions
                    for question in followup_questions[:5]:
                        assistant_message_followup.button(question, on_click=set_user_question, args=(question,))
        else:
            assistant_message_sql_explanation = st.chat_message(
                "assistant", avatar=ai_icon
            )
            assistant_message_sql_explanation.write(sql_explanation)
            st.session_state.messages.append({"role": "assistant", "content": sql_explanation, "avatar": ai_icon, "content_type": "text"})
            if clarification_request:
                assistant_message_clarification = st.chat_message(
                    "assistant", avatar=ai_icon
                )
                assistant_message_clarification.write(clarification_request)
                st.session_state.messages.append({"role": "assistant", "content": clarification_request, "avatar": ai_icon, "content_type": "text"})
            
        # ask the model
        # response = model.ask(st.session_state.messages[-1]["content"])
        # update the explanation status
        # st.session_state.explanation_status = True
        # update the last message
        # st.session_state.last_message = my_question

    # Print the response as a new message from the assistant
    # chatbot_message_response = st.chat_message("assistant", avatar=ai_icon)
    # chatbot_message_response.code(my_question, language="sql", line_numbers=True)
    # message =  {"role": "assistant", "content": my_question}

    # Add response to message history
    # st.session_state.messages.append(message) 


# if st.session_state.explanation_status:
#     if st.button('Do you want an explanation of the query?'):
#         with st.spinner("Thinking..."):
#             # ask the model for an explanation of the last query
#             explanation = model.explain(st.session_state.last_message)
            
#             # print the explanation as a new message from the assistant
#             explanation_message_response = st.chat_message("assistant", avatar="🤖")
#             explanation_message_response.write(explanation)

#             # add explanation to message history
#             explanation_message = {"role": "assistant", "content": explanation}
#             st.session_state.messages.append(explanation_message)
