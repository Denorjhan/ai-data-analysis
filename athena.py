import logging
import json
import os, sys
import re
import time
import pandas as pd
import io
import traceback
import sqlparse
from aws_clients import aws_client, config
from utils import get_value_from_text
from dev.prompt_chain import MinimalChainable
from prompts import generate_sql_prompt, debug_sql_prompt, generate_plotly_code_prompt
from orchestrator import engine
from s3 import get_csv_results


def execute_query_with_autocorrect(sql: str, question: str = "", max_attempts: int = 3) -> pd.DataFrame:
    athena_client = aws_client.get_athena_client()
    attempt = 0
    output_location = f"s3://{config['aws']['athena']['output_location']}"
    catalog = config['aws']['athena']['catalog']
    database = config['aws']['glue']['database']
    
    while attempt < max_attempts:
        try:
            print(f"Attempt {attempt + 1}")
            print(f"Executing: {sql}")
            query_execution = athena_client.start_query_execution(
                QueryString=sql,
                ResultConfiguration={"OutputLocation": output_location},
                QueryExecutionContext={"Catalog": catalog, "Database": database},
            )
            execution_id = query_execution["QueryExecutionId"]
            
            # wait for the query to complete
            while True:
                response = athena_client.get_query_execution(
                    QueryExecutionId=execution_id
                )
                status = response["QueryExecution"]["Status"]["State"]
                if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    break
                print(f"Query status: {status}. Waiting for completion...")
                time.sleep(0.5)  # Wait before checking again
                
            if status == "SUCCEEDED":
                break
            # debug and validate query
            elif attempt + 1 <= max_attempts:
                attempt += 1
                # regenerate sql and iterate back over the while loop
                error_message = response["QueryExecution"]["Status"]["StateChangeReason"]
                sql = engine.debug_sql(sql=sql, error_message=error_message, question=question, retry=True)
                if engine.is_sql_valid(sql, question):
                    attempt += 1
                else:
                    raise Exception(f"Query failed to fix the error after.")
            else:
                error_message = response["QueryExecution"]["Status"]["StateChangeReason"]
                raise Exception(f"Query failed after {attempt} attempts with the following error: {error_message}")
                
        except Exception as e:
            print(traceback.format_exc())
            raise Exception(f"Query failed with error: {e}")
        
    result_folder = config['aws']['athena']['output_location'].split('/')[3]
    df = get_csv_results(execution_id, result_folder)
    return df


def syntax_checker(query_string):
    athena_client = aws_client.get_athena_client()
    # print("Inside execute query", query_string)
    output_location = f"s3://{config['aws']['athena']['output_location']}"
    catalog = config['aws']['athena']['catalog']
    database = config['aws']['glue']['database']

    query_string = "Explain  " + query_string
    print(f"Executing: {query_string}")
    try:
        print("Checking Query Syntax")
        query_execution = athena_client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration={"OutputLocation": output_location},
            QueryExecutionContext={"Catalog": catalog, "Database": database},
        )
        execution_id = query_execution["QueryExecutionId"]
        print(f"execution_id: {execution_id}")
        time.sleep(3)
        results = athena_client.get_query_execution(
            QueryExecutionId=execution_id
        )
        # print(f"results: {results}")
        status = results["QueryExecution"]["Status"]
        print("Status :", status)
        if status["State"] == "SUCCEEDED":
            return "Passed"
        else:
            print(results["QueryExecution"]["Status"]["StateChangeReason"])
            errmsg = results["QueryExecution"]["Status"]["StateChangeReason"]
            return errmsg
        # return results
    except Exception as e:
        print("Error in exception")
        msg = str(e)
        print(msg)
        return msg #! doubke check this return statment

def generate_database_ddl():
    tables_query = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{config['aws']['glue']['database']}'
    """

    # Execute the query and load the results into a DataFrame
    tables_df = execute_query_with_autocorrect(tables_query)
    # Iterate over all tables and generate DDL
    ddl_statements = {}
    for table_name in tables_df["table_name"]:
        ddl_statements[table_name] = generate_table_ddl(table_name)

    return ddl_statements

def generate_table_ddl(table):
    # Query to get the columns information for the current table
    columns_query = f"""
    SELECT column_name, data_type, is_nullable, column_default, extra_info
    FROM information_schema.columns
    WHERE table_schema = '{config['aws']['glue']['database']}' AND table_name = '{table}'
    """

    # Execute the query and load the results into a DataFrame
    columns_df = execute_query_with_autocorrect(columns_query)

    # Construct the CREATE TABLE statement
    ddl = f"CREATE TABLE {table} (\n"
    for index, row in columns_df.iterrows():
        column_def = f"  {row['column_name']} {row['data_type']}"
        if row["is_nullable"] == "NO":
            column_def += " NOT NULL"
        if pd.notna(row["column_default"]):
            column_def += f" DEFAULT {row['column_default']}"
        if pd.notna(row["extra_info"]):
            column_def += f" {row['extra_info']}"
        column_def += ",\n"
        ddl += column_def
    ddl = ddl.rstrip(",\n") + "\n);"

    return ddl

