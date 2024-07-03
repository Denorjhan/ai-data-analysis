from orchestrator import Orchestrator, engine
from vanna.flask import VannaFlaskApp
# from athena_execution import AthenaQueryExecute
import pandas as pd
import os
import time
from dev.flask_app import CustomVannaFlaskApp
from athena import execute_query_with_autocorrect


# -------- Set up Athena as a custom Databse --------

conn_details = {
    "database": "electronics_store",
    "glue_databucket_name": "athena-output-deno",
    "result_folder": "outputs",
}
# athena_instance = AthenaQueryExecute(config=conn_details)

# -------- Set up engine that handles core logic --------

# engine = Orchestrator(
#     config={"api_key": os.getenv("OPENAI_API_KEY"), "model": "gpt-3.5-turbo"}
# )
# engine = Orchestrator(config={'api_key': os.getenv('ANTHROPIC_API_KEY'), 'model': 'claude-3-5-sonnet-20240620'})
# engine = Orchestrator(config={'api_key': os.getenv('GROQ_API_KEY'), 'model': 'llama3-8b-8192'})
# engine = Orchestrator(config={'api_key': os.getenv('GROQ_API_KEY'), 'model': 'llama3-70b-8192'})


# Set additionl properties for the engine
# engine.athena_executor = athena_instance
# athena_instance.orchestrator = engine
# engine.run_sql = athena_instance.execute_query

engine.run_sql = execute_query_with_autocorrect

engine.run_sql_is_set = True

# -------- Training --------

# only need to run once
# ddl_statements = athena_instance.generate_database_ddl()
# for table_name, ddl in ddl_statements.items():
#     print(f"adding table {table_name} to training data")
#     engine.train(ddl=ddl)
# print(engine.get_training_data())
#! train on provided ddl file


# q = "What are the total sales for the year 2019?"
# ans = engine.ask(q, allow_llm_to_see_data=True)


# path = os.path.dirname(os.path.abspath(__file__))
# VannaFlaskApp(engine, allow_llm_to_see_data=True, followup_questions=True, assets_folder=f'{path}/frontend/assets/', index_html_path=f'{path}/frontend/index.html').run()
# VannaFlaskApp(engine, allow_llm_to_see_data=True, followup_questions=True).run()
