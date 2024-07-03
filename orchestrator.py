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
from dev.prompt_chain import MinimalChainable
import json
from prompts import generate_sql_prompt, debug_sql_prompt, generate_plotly_code_prompt
import sqlparse
from utils import get_value_from_text
# from athena import execute_query_with_autocorrect

os.environ["TOKENIZERS_PARALLELISM"] = "false"


class Orchestrator(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)
        OpenAI_Chat.__init__(self, config=config)
        # Anthropic_Chat.__init__(self, config=config)
        # GroqLLM.__init__(self, config=config)


    # def generate_sql(
    #     self, question: str, allow_llm_to_see_data=True, max_attempt=4, **kwargs
    # ) -> str:
    #     """
    #     Generate and Validate SQL query.

    #     Args:
    #     - question (str): The question or prompt for generating the SQL query.
    #     - allow_llm_to_see_data (bool): Flag to allow the LLM to see data in the database.
    #     - max_attempt (int): Maximum number of attempts to correct the syntax of the SQL.

    #     Returns:
    #     - str: SQL query generated and validated.
    #     """
    #     logging.info(f"We are in the generate_sql function")
    #     if self.config is not None:
    #         initial_prompt = self.config.get("initial_prompt", None)
    #     else:
    #         initial_prompt = None

    #     question_sql_list = self.get_similar_question_sql(question, **kwargs)
    #     ddl_list = self.get_related_ddl(question, **kwargs)
    #     doc_list = self.get_related_documentation(question, **kwargs)
        
    #     sql_plan = None
    #     llm_response = ""
    #     #! if retry, get debug prompt. else get plan
    #     # if 'retry' not in kwargs:
    #         # sql_plan = self.get_sql_plan(question, question_sql_list, ddl_list, doc_list, **kwargs)    
    #         # pass        

    #     prompt = self.get_sql_prompt(
    #         initial_prompt=initial_prompt,
    #         question=question,
    #         question_sql_list=question_sql_list,
    #         ddl_list=ddl_list,
    #         doc_list=doc_list,
    #         sql_plan=sql_plan
    #     )
        
    #     self.log(title="SQL Prompt", message=prompt)
    #     print(f"$$$$$$$$$$$$$$$$$$$$$ SQL Prompt $$$$$$$$$$$$$$$$$$$$$$ {prompt}")

    #     attempt = 0
    #     error_messages = []
    #     prompts = [prompt]

    #     while attempt < max_attempt:
    #         self.log(f"SQL Generation attempt Count: {attempt + 1}")
    #         try:
    #             self.log(
    #                 f"We are in the try block to generate the SQL and count is: {attempt + 1}"
    #             )
    #             llm_response = self.submit_prompt(prompt, **kwargs)
    #             self.log(title="LLM Response", message=llm_response)

    #             # # Ensure llm_response is handled correctly
    #             # if isinstance(llm_response, str):
    #             #     self.log(f'LLM Response is a string: {llm_response}')
    #             #     raise ValueError("Expected LLM response to be a dictionary, got string instead.")

    #             sql_query = self.extract_sql(llm_response)

    #             syntaxcheckmsg = ""
    #             # check if llm response has a query or if its all text
    #             if not self.is_sql_valid(sql_query):
    #                 syntaxcheckmsg = sql_query
    #             else:
    #                 self.log(f"is_valid_sql check passed")
    #                 syntaxcheckmsg = self.athena_executor.syntax_checker(sql_query)
    #                 self.log(f"######syntaxcheckmsg: {syntaxcheckmsg}")

    #             # return if query is syntactically correct or adjust prompt to retry
    #             if syntaxcheckmsg == "Passed":
    #                 self.log(
    #                     f"Syntax checked for query passed in attempt number: {attempt + 1}"
    #                 )
    #                 return sql_query
    #             else:
    #                 error_context = f"""This is an error: {syntaxcheckmsg}. 
    #                 To correct this, please generate an alternative SQL query which will correct the mentioned errors.
    #                 The updated query should take care of all the syntax and type issues encountered.
    #                 Follow the instructions mentioned above to remediate the error. 
    #                 you are provided with a list of ddl statements above.
    #                 Update the below SQL query to resolve the issue:
    #                 {sql_query}
    #                 Make sure the updated SQL query aligns with the requirements provided in the initial question."""
    #                 prompt.append(self.system_message(error_context))
    #                 self.log(title="Prompt", message=prompt)
    #                 prompts.append(prompt)
    #                 attempt += 1
    #         except Exception as e:
    #             self.log(title="Error", message="FAILED")
    #             msg = str(e)
    #             traceback_str = traceback.format_exc()
    #             self.log(title="Error", message=traceback_str)
    #             error_messages.append(msg)
    #             attempt += 1

    #     if "intermediate_sql" in llm_response:
    #         if not allow_llm_to_see_data:
    #             return "The LLM is not allowed to see the data in your database. Your question requires database introspection to generate the necessary SQL. Please set allow_llm_to_see_data=True to enable this."

    #         if allow_llm_to_see_data:
    #             intermediate_sql = self.extract_sql(llm_response)

    #             try:
    #                 self.log(title="Running Intermediate SQL", message=intermediate_sql)
    #                 df = self.run_sql(intermediate_sql)

    #                 prompt = self.get_sql_prompt(
    #                     initial_prompt=initial_prompt,
    #                     question=question,
    #                     question_sql_list=question_sql_list,
    #                     ddl_list=ddl_list,
    #                     doc_list=doc_list
    #                     + [
    #                         f"The following is a pandas DataFrame with the results of the intermediate SQL query {intermediate_sql}: \n"
    #                         + df.to_markdown()
    #                     ],
    #                     **kwargs,
    #                 )
    #                 self.log(title="Final SQL Prompt", message=prompt)
    #                 llm_response = self.submit_prompt(prompt, **kwargs)
    #                 self.log(title="LLM Response", message=llm_response)
    #             except Exception as e:
    #                 return f"Error running intermediate SQL: {e}"

    #     return self.extract_sql(llm_response)

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
                        "example_pairs": question_sql_list
                    },
                    model=self,
                    callable=self.submit_prompt,
                    prompts=[
                        # prompt #1
                        generate_sql_prompt
                    ]
                )
            # get json as a dictionary
            print("########### SQL GEN RESPONSE FROM LLM ###########")
            print(result)
            # json_result = json.loads(result[0])
            sql_query = get_value_from_text(result[0], "sql_query")
            explanation = get_value_from_text(result[0], "explanation")
            clarification_request = get_value_from_text(result[0], "clarification_request")
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
            #check for clarification_request
            if clarification_request:
                return None, None, clarification_request
            else:
                return None, f"Error generating SQL prompt", None

    def debug_sql(self, sql: str, error_message: str, question: str, **kwargs) -> str:
        
        combined_question_error = f"Question: {question}\nError Message: {error_message}"
        
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
                        "sql_examples": question_sql_pairs
                    },
                    model=self,
                    callable=self.submit_prompt,
                    prompts=[
                        # prompt #1
                        debug_sql_prompt
                    ]
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
        attempt = 1
        # the sql statment goes through two checks
        # 1. is_sql_select checks if the sql statement is a select statement
        # 2. syntax_checker checks if the sql statement is syntactically correct
        from athena import execute_query_with_autocorrect # importing here to avoid circular import
        while attempt <= max_attempts:
            if self.is_sql_select(sql):
                syntax_feedback = execute_query_with_autocorrect(sql)
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
            if statement.get_type() == 'SELECT':
                return True

        return False
    
    # def generate_plotly_code(
    #     self, question: str = None, sql: str = None, df_metadata: str = None, **kwargs
    # ) -> str:
    #     if question is not None:
    #         system_msg = f"The following is a pandas DataFrame that contains the results of the query that answers the question the user asked: '{question}'"
    #     else:
    #         system_msg = "The following is a pandas DataFrame "

    #     if sql is not None:
    #         system_msg += f"\n\nThe DataFrame was produced using this query: {sql}\n\n"

    #     system_msg += f"The following is information about the resulting pandas DataFrame 'df': \n{df_metadata}"

    #     user_msg = """===Please generate the Python plotly code to chart the results of the dataframe based on the following instructions. \n 
    #                 1. Always assume the data is in a pandas dataframe called 'df'. Never make up data. \n
    #                 2. Before generating any code, think carefully about the type of plot to use and the coloumns to use for each axis given the context of the question that would best visualize the data. \n
    #                 3. Add units to the axes for clarity on the data being plotted. \n
    #                 4. If there is only one value in the dataframe, use an Indicator. \n
    #                 5. Respond with only Python code. Do not answer with any explanations -- just the code. \n"""

    #     message_log = [
    #         self.system_message(system_msg),
    #         self.user_message(user_msg),
    #     ]

    #     plotly_code = self.submit_prompt(message_log, kwargs=kwargs)

    #     code = self._sanitize_plotly_code(self._extract_python_code(plotly_code))
    #     return code

    def generate_plotly_code(
        self, question: str, sql: str, df_metadata: str, **kwargs
    ) -> str:
        result, filled_prompts = MinimalChainable.run(
                    context={
                        "question": question,
                        "sql": sql,
                        "df_metadata": df_metadata # might need to be df.dtypes
                    },
                    model=self,
                    callable=self.submit_prompt,
                    prompts=[
                        # prompt #1
                        generate_plotly_code_prompt
                    ]
                )
        print("########### PLOTLY CODE RESPONSE FROM LLM ###########")
        print(result)
        # json_result = json.loads(result[0])
        plotly_code = get_value_from_text(result[0], "plotly_code")

        code = self._sanitize_plotly_code(self._extract_python_code(plotly_code))
        return code

    # def get_sql_prompt(
    #     self,
    #     initial_prompt: str,
    #     question: str,
    #     question_sql_list: list,
    #     ddl_list: list,
    #     doc_list: list,
    #     **kwargs,
    # ):
    #     """
    #     Example:
    #     ```python
    #     vn.get_sql_prompt(
    #         question="What are the top 10 customers by sales?",
    #         question_sql_list=[{"question": "What are the top 10 customers by sales?", "sql": "SELECT * FROM customers ORDER BY sales DESC LIMIT 10"}],
    #         ddl_list=["CREATE TABLE customers (id INT, name TEXT, sales DECIMAL)"],
    #         doc_list=["The customers table contains information about customers and their sales."],
    #     )

    #     ```

    #     This method is used to generate a prompt for the LLM to generate SQL.

    #     Args:
    #         question (str): The question to generate SQL for.
    #         question_sql_list (list): A list of questions and their corresponding SQL statements.
    #         ddl_list (list): A list of DDL statements.
    #         doc_list (list): A list of documentation.

    #     Returns:
    #         any: The prompt for the LLM to generate SQL.
    #     """

    #     if initial_prompt is None:
    #         initial_prompt = (
    #             f"You are a {self.dialect} expert. Please help to generate a valid {self.dialect} SQL query to answer the question. Your response should ONLY be based on the given context and follow the response guidelines and format instructions. The context provides DDL statements of the tables in the database, the documentation and the previous questions and answers in sql pairs. "
    #         )

        
    #     # adding ddl, documentation and example sql to the prompt
    #     initial_prompt = self.add_ddl_to_prompt(
    #         initial_prompt, ddl_list, max_tokens=self.max_tokens
    #     )

    #     if self.static_documentation != "":
    #         doc_list.append(self.static_documentation)

    #     initial_prompt = self.add_documentation_to_prompt(
    #         initial_prompt, doc_list, max_tokens=self.max_tokens
    #     )
        
    #     message_log = [self.system_message(initial_prompt)]
        
    #     initial_prompt += (
    #         "Response Guidelines \n"
    #         "- Before generating a SQL query, think step by step on how to solve the user's question given the available table and coloumn names in the context. \n"
    #         "- If the provided context is sufficient, please generate a valid SQL query for the question. \n"
    #         "- If the provided context is almost sufficient but requires knowledge of a specific string in a particular column, please generate an intermediate SQL query to find the distinct strings in that column. Prepend the query with a comment saying intermediate_sql \n"
    #         "- If the provided context is insufficient, please explain why it can't be generated. \n"
    #         "- If the user's question is ambigous, please ask clarifying questions to get more information on how to answer the question given the avaliable context. \n"
    #         "- Please use the most relevant table(s)to get the exact coloumn names and data types. Do not guess the name of the column. \n"
    #         "- Do not use column aliases defined in the SELECT clause in the GROUP BY or ORDER BY clauses. Instead, repeat the full expressions used to define the aliases in these clauses. \n"
    #         '- When referencing column names in your SQL queries, please use quotes (e.g., "column_name") to ensure clarity and avoid ambiguity. \n'
    #         "- Use a default limit of 100 in your SQL queries unless the user's question requires a different limit. \n"
    #         "- When working with dates, please ensure that the date column is of type DATE. \n"
    #         "- If the question has been asked and answered before, please repeat the answer exactly as it was given before. \n"
    #     )

    #     for example in question_sql_list:
    #         if example is None:
    #             print("example is None")
    #         else:
    #             if example is not None and "question" in example and "sql" in example:
    #                 message_log.append(self.user_message(example["question"]))
    #                 message_log.append(self.assistant_message(example["sql"]))

    #     message_log.append(self.user_message(question))

    #     return message_log

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



# engine = Orchestrator(config={'api_key': os.getenv('ANTHROPIC_API_KEY'), 'model': 'claude-3-5-sonnet-20240620'})
engine = Orchestrator(config={"api_key": os.getenv("OPENAI_API_KEY"), "model": "gpt-3.5-turbo"})
# engine = Orchestrator(config={'api_key': os.getenv('GROQ_API_KEY'), 'model': 'llama3-8b-8192'})
# engine = Orchestrator(config={'api_key': os.getenv('GROQ_API_KEY'), 'model': 'llama3-70b-8192'})
