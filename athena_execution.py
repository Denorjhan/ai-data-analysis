import logging
import json
import os, sys
import re

# sys.path.append("/home/ec2-user/SageMaker/llm_bedrock_v0/")
from aws_clients import Clientmodules
import time
import pandas as pd
import io
import traceback


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class AthenaQueryExecute:
    def __init__(self, config):
        self.config = config
        self.glue_databucket_name = self.config["glue_databucket_name"] # output location
        self.athena_client = Clientmodules.createAthenaClient()
        self.s3_client = Clientmodules.createS3Client()
        self.orchestrator = None

    # def execute_query(self, sql: str) -> pd.DataFrame:
    #     max_attempts = 3
    #     attempt = 0
    #     result_folder = self.config["result_folder"]
    #     result_config = {
    #         "OutputLocation": f"s3://{self.glue_databucket_name}/{result_folder}"
    #     }
    #     query_execution_context = {
    #         "Catalog": "AwsDataCatalog",
    #         "Database": self.config["database"],
    #     }

    #     while attempt < max_attempts:
    #         try:
    #             print(f"Attempt {attempt + 1}")
    #             print(f"Executing: {sql}")
    #             query_execution = self.athena_client.start_query_execution(
    #                 QueryString=sql,
    #                 ResultConfiguration=result_config,
    #                 QueryExecutionContext=query_execution_context,
    #             )
    #             execution_id = query_execution["QueryExecutionId"]

    #             # wait for the query to complete
    #             while True:
    #                 response = self.athena_client.get_query_execution(
    #                     QueryExecutionId=execution_id
    #                 )
    #                 status = response["QueryExecution"]["Status"]["State"]
    #                 if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
    #                     break
    #                 print(f"Query status: {status}. Waiting for completion...")
    #                 time.sleep(0.5)  # Wait before checking again

    #             # at this point the query is executed with either sucess, fail, or cancel
    #             if status == "SUCCEEDED":
    #                 break
    #             elif attempt + 1 < max_attempts:
    #                 # regenerate sql and iterate back over the while loop
    #                 attempt += 1
    #                 error_message = response["QueryExecution"]["Status"][
    #                     "StateChangeReason"
    #                 ]
    #                 print(f"Error message: {error_message}")
    #                 prompt = f"""This is an error: {error_message} for the following sql query {sql}. 
    #                 To correct this, please generate an alternative SQL query which will correct the mentioned errors.
    #                 The updated query should take care of all the errors present.
    #                 Follow the instructions mentioned above to remediate the error. 
    #                 You will alos be provided with a list of ddl statements.
    #                 Update the below SQL query to resolve all errors."""
    #                 print("REGENERATING SQL **************************")
    #                 sql = self.orchestrator.generate_sql(question=prompt, retry=True)
    #                 print(f"#########NEW SQL: {sql}")
    #             # if the query is not successful after max attempts, raise an error
    #             else:
    #                 raise Exception(f"Query failed after {attempt} attempts.")
    #         except Exception as e:
    #             print(traceback.format_exc())
    #             raise Exception(f"Query failed with error: {e}")

    #     df = self.get_csv_results(execution_id, result_folder)
    #     return df

    def new_execution(self, sql: str, question: str, max_attempts: int = 3) -> pd.DataFrame:
        attempt = 0
        result_folder = self.config["result_folder"]
        result_config = {
            "OutputLocation": f"s3://{self.glue_databucket_name}/{result_folder}"
        }
        query_execution_context = {
            "Catalog": "AwsDataCatalog",
            "Database": self.config["database"],
        }
        
        while attempt < max_attempts:
            try:
                print(f"Attempt {attempt + 1}")
                print(f"Executing: {sql}")
                query_execution = self.athena_client.start_query_execution(
                    QueryString=sql,
                    ResultConfiguration=result_config,
                    QueryExecutionContext=query_execution_context,
                )
                execution_id = query_execution["QueryExecutionId"]
                
                # wait for the query to complete
                while True:
                    response = self.athena_client.get_query_execution(
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
                    sql = self.orchestrator.debug_sql(sql=sql, error_message=error_message, question=question, retry=True)
                    if self.orchestrator.is_sql_valid(sql, question):
                        attempt += 1
                    else:
                        raise Exception(f"Query failed to fix the error after.")
                else:
                    error_message = response["QueryExecution"]["Status"]["StateChangeReason"]
                    raise Exception(f"Query failed after {attempt} attempts with the following error: {error_message}")
                    
            except Exception as e:
                print(traceback.format_exc())
                raise Exception(f"Query failed with error: {e}")
        
        df = self.get_csv_results(execution_id, result_folder)
        return df

    # get csv results stored in s3.
    def get_csv_results(self, execution_id, result_folder):
        file_name = f"{result_folder}/{execution_id}.csv"
        logger.info(f"checking for file :{file_name}")
        local_file_name = f"/tmp/{file_name}"

        print(f"Calling download fine with params {local_file_name}")
        obj = self.s3_client.get_object(Bucket=self.glue_databucket_name, Key=file_name)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="utf8")
        print(df)
        return df

    def syntax_checker(self, query_string):
        # print("Inside execute query", query_string)
        result_folder = self.config["result_folder"]
        result_config = {
            "OutputLocation": f"s3://{self.glue_databucket_name}/{result_folder}"
        }
        query_execution_context = {
            "Catalog": "AwsDataCatalog",
            "Database": self.config["database"],
        }
        query_string = "Explain  " + query_string
        print(f"Executing: {query_string}")
        try:
            print(" I am checking the syntax here")
            query_execution = self.athena_client.start_query_execution(
                QueryString=query_string,
                ResultConfiguration=result_config,
                QueryExecutionContext=query_execution_context,
            )
            execution_id = query_execution["QueryExecutionId"]
            print(f"execution_id: {execution_id}")
            time.sleep(3)
            results = self.athena_client.get_query_execution(
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

    def generate_database_ddl(self):
        tables_query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{self.config['database']}'
        """

        # Execute the query and load the results into a DataFrame
        tables_df = self.new_execution(tables_query)
        # Iterate over all tables and generate DDL
        ddl_statements = {}
        for table_name in tables_df["table_name"]:
            ddl_statements[table_name] = self.generate_table_ddl(table_name)

        return ddl_statements

    def generate_table_ddl(self, table):
        # Query to get the columns information for the current table
        columns_query = f"""
        SELECT column_name, data_type, is_nullable, column_default, extra_info
        FROM information_schema.columns
        WHERE table_schema = '{self.config['database']}' AND table_name = '{table}'
        """

        # Execute the query and load the results into a DataFrame
        columns_df = self.execute_query(columns_query)

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


# Creating an instance of the class and testing the execute method

# conn_details = {'database': 'cube_ecom',
#           'glue_databucket_name': 'athena-output-deno',
#           'result_folder': 'outputs'
#           }
# athena_instance = AthenaQueryExecute(config=conn_details)
# x = athena_instance.execute_query("SELECT id, status, user_ FROM cube_ecom.orders limit 10;")
# print("x#############\n",x)
