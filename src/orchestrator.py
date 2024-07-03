from vanna.openai import OpenAI_Chat
from vanna.anthropic import Anthropic_Chat
from vanna.chromadb import ChromaDB_VectorStore
import os
import traceback
import logging
import pandas as pd
from groqllm import GroqLLM
import time
from typing import Union, Tuple
import plotly
from prompt_chain import MinimalChainable
import json
from prompts import generate_sql_prompt, debug_sql_prompt, generate_plotly_code_prompt
import sqlparse
from utils import get_value_from_text

os.environ["TOKENIZERS_PARALLELISM"] = "false"


class Orchestrator(ChromaDB_VectorStore, Anthropic_Chat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        # OpenAI_Chat.__init__(self, config=config)
        Anthropic_Chat.__init__(self, config=config)
        # GroqLLM.__init__(self, config=config)

    def generate_sql(self, question: str, **kwargs) -> str:
        try:
            question_sql_list = self.get_similar_question_sql(question)
            ddl_list = self.get_related_ddl(question)
            doc_list = self.get_related_documentation(question)

            result, filled_prompts = MinimalChainable.run(
                context={
                    "user_question": question,
                    "database_ddl": ddl_list,
                    "documentation": doc_list,
                    "example_pairs": question_sql_list,
                },
                model=self,
                callable=self.submit_prompt,
                prompts=[
                    # prompt #1
                    generate_sql_prompt
                ],
            )
            # get json as a dictionary
            print("########### SQL GEN RESPONSE FROM LLM ###########")
            print(result)
            # json_result = json.loads(result[0])
            sql_query = get_value_from_text(result[0], "sql_query")
            explanation = get_value_from_text(result[0], "explanation")
            clarification_request = get_value_from_text(
                result[0], "clarification_request"
            )
            #! potentially filter for clarification qs here
            try:
                if self.is_sql_valid(sql_query, question):
                    return sql_query, explanation, clarification_request
                else:
                    return None, explanation, clarification_request
            except Exception as e:
                raise Exception(f"Error generating SQL prompt: {str(e)}")

        except Exception as e:
            traceback_str = traceback.format_exc()
            self.log(title="Failed to generate SQL prompt:", message=traceback_str)
            # check for clarification_request
            if clarification_request:
                return None, None, clarification_request
            else:
                return None, f"Error generating SQL prompt", None

    def debug_sql(self, sql: str, error_message: str, question: str, **kwargs) -> str:
        combined_question_error = (
            f"Question: {question}\nError Message: {error_message}"
        )

        # the validate method calls this method with explain=true becasue it runs an EXPLAIN query
        if kwargs.get("explain", False):
            sql = f"EXPLAIN {sql}"

        question_sql_pairs = self.get_similar_question_sql(combined_question_error)
        ddl_list = self.get_related_ddl(combined_question_error)
        doc_list = self.get_related_documentation(combined_question_error)
        try:
            result, filled_prompts = MinimalChainable.run(
                context={
                    "user_question": question,
                    "generated_sql": sql,
                    "error_message": error_message,
                    "database_ddl": ddl_list,
                    "documentation": doc_list,
                    "sql_examples": question_sql_pairs,
                },
                model=self,
                callable=self.submit_prompt,
                prompts=[
                    # prompt #1
                    debug_sql_prompt
                ],
            )
            print("########### DEBUG SQL RESPONSE FROM LLM ###########")
            print(result)
            # json_result = json.loads(result[0])
            corrected_query = get_value_from_text(result[0], "corrected_query")
            return corrected_query
        except Exception as e:
            traceback_str = traceback.format_exc()
            self.log(title="Failed to debug SQL prompt:", message=traceback_str)
            return f"Error debugging SQL prompt: {str(e)}"

    def is_sql_valid(self, sql: str, question: str, max_attempts: int = 3) -> bool:
        from athena import syntax_checker

        attempt = 1
        # the sql statment goes through two checks
        # 1. is_sql_select checks if the sql statement is a select statement
        # 2. syntax_checker checks if the sql statement is syntactically correct
        while attempt <= max_attempts:
            if self.is_sql_select(sql):
                syntax_feedback = syntax_checker(sql)
                if syntax_feedback == "Passed":
                    return True
                else:
                    sql = self.debug_sql(sql, syntax_feedback, question)
                    attempt += 1
            else:
                error_msg = "The SQL statement is not a SELECT statement. ONly SELECT statements are allowed to be executed."
                sql = self.debug_sql(sql, error_msg, question)
                attempt += 1
        return False

    def is_sql_select(self, sql: str) -> bool:
        parsed = sqlparse.parse(sql)

        for statement in parsed:
            if statement.get_type() == "SELECT":
                return True

        return False

    def generate_plotly_code(
        self, question: str, sql: str, df_metadata: str, **kwargs
    ) -> str:
        result, filled_prompts = MinimalChainable.run(
            context={
                "QUESTION": question,
                "SQL": sql,
                "DF_METADATA": df_metadata,  # might need to be df.dtypes
            },
            model=self,
            callable=self.submit_prompt,
            prompts=[
                # prompt #1
                generate_plotly_code_prompt
            ],
        )
        print("#############################Filled prompts#############################")
        print(filled_prompts)
        print("########### PLOTLY CODE RESPONSE FROM LLM ###########")
        print(result)
        # json_result = json.loads(result[0])
        plotly_code = get_value_from_text(result[0], "plotly_code")
        print(f"########### PLOTLY CODE RESPONSE FROM LLM ########### {plotly_code}")

        code = self._sanitize_plotly_code(plotly_code)
        return code

    def generate_summary(self, question: str, df: pd.DataFrame, **kwargs) -> str:
        """
        **Example:**
        ```python
        vn.generate_summary("What are the top 10 customers by sales?", df)
        ```

        Generate a summary of the results of a SQL query.

        Args:
            question (str): The question that was asked.
            df (pd.DataFrame): The results of the SQL query.

        Returns:
            str: The summary of the results of the SQL query.
        """

        message_log = [
            self.system_message(
                f"You are a helpful data analyst. The user asked the question: '{question}'\n\nThe following is a pandas DataFrame with the results of the query: \n{df.to_markdown()}\n\n"
            ),
            self.user_message(
                "Briefly summarize the data in a few sentences based on the question that was asked. Then identify any potential useful insights or trends from the data that may not be obvious at first glance. Only respond with the Summary section and Key Insights section in markdown format. Do not respond with any additional explanation beyond the summary."
                + self._response_language()
            ),
        ]

        summary = self.submit_prompt(message_log, **kwargs)

        return summary

    def ask(
        self,
        question: Union[str, None] = None,
        print_results: bool = True,
        auto_train: bool = True,
        visualize: bool = True,  # if False, will not generate plotly code
        allow_llm_to_see_data: bool = False,
    ) -> Union[
        Tuple[
            Union[str, None],
            Union[pd.DataFrame, None],
            Union[plotly.graph_objs.Figure, None],
        ],
        None,
    ]:
        """
        **Example:**
        ```python
        vn.ask("What are the top 10 customers by sales?")
        ```

        Ask Vanna.AI a question and get the SQL query that answers it.

        Args:
            question (str): The question to ask.
            print_results (bool): Whether to print the results of the SQL query.
            auto_train (bool): Whether to automatically train Vanna.AI on the question and SQL query.
            visualize (bool): Whether to generate plotly code and display the plotly figure.

        Returns:
            Tuple[str, pd.DataFrame, plotly.graph_objs.Figure]: The SQL query, the results of the SQL query, and the plotly figure.
        """

        start_time = time.time()  # Measure the start time

        if question is None:
            question = input("Enter a question: ")

        try:
            sql = self.generate_sql(
                question=question, allow_llm_to_see_data=allow_llm_to_see_data
            )
        except Exception as e:
            print(e)
            return None, None, None

        if print_results:
            try:
                Code = __import__("IPython.display", fromList=["Code"]).Code
                display(Code(sql))
            except Exception as e:
                print(sql)

        if self.run_sql_is_set is False:
            print(
                "If you want to run the SQL query, connect to a database first. See here: https://vanna.ai/docs/databases.html"
            )

            if print_results:
                return None
            else:
                return sql, None, None

        try:
            df = self.run_sql(sql, question)

            if print_results:
                try:
                    display = __import__(
                        "IPython.display", fromList=["display"]
                    ).display
                    display(df)
                except Exception as e:
                    print(df)

            if len(df) > 0 and auto_train:
                self.add_question_sql(question=question, sql=sql)
            # Only generate plotly code if visualize is True
            if visualize:
                try:
                    plotly_code = self.generate_plotly_code(
                        question=question,
                        sql=sql,
                        df_metadata=f"Running df.dtypes gives:\n {df.dtypes}",
                    )
                    fig = self.get_plotly_figure(plotly_code=plotly_code, df=df)
                    if print_results:
                        try:
                            display = __import__(
                                "IPython.display", fromlist=["display"]
                            ).display
                            Image = __import__(
                                "IPython.display", fromlist=["Image"]
                            ).Image
                            img_bytes = fig.to_image(format="png", scale=2)
                            display(Image(img_bytes))
                        except Exception as e:
                            fig.show()
                except Exception as e:
                    # Print stack trace
                    traceback.print_exc()
                    print("Couldn't run plotly code: ", e)
                    if print_results:
                        return None
                    else:
                        return sql, df, None
            else:
                return sql, df, None

        except Exception as e:
            print("Couldn't run sql: ", e)
            if print_results:
                return None
            else:
                return sql, None, None

        finally:
            end_time = time.time()
            print(
                f"############################# Time taken: {end_time - start_time} seconds #############################"
            )

        return sql, df, fig


def set_run_sql(engine):
    from athena import (
        execute_query_with_autocorrect,
    )  # Local import to avoid circular dependency

    engine.run_sql = execute_query_with_autocorrect
    engine.run_sql_is_set = True


engine = Orchestrator(config={'api_key': os.getenv('ANTHROPIC_API_KEY'), 'model': 'claude-3-5-sonnet-20240620', 'max_tokens': 1500})
# engine = Orchestrator(
#     config={"api_key": os.getenv("OPENAI_API_KEY"), "model": "gpt-3.5-turbo"}
# )
# engine = Orchestrator(config={'api_key': os.getenv('GROQ_API_KEY'), 'model': 'llama3-8b-8192'})
# engine = Orchestrator(config={'api_key': os.getenv('GROQ_API_KEY'), 'model': 'llama3-70b-8192'})

set_run_sql(engine)
