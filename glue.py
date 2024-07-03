from aws_clients import aws_client
import time
from aws_clients import config

def create_glue_crawler(crawler_name, database_name, s3_target_path):
    glue_client = aws_client.get_glue_client()
    try:
        glue_client.get_crawler(Name=crawler_name)
        print(f"Crawler {crawler_name} already exists.")
        return {"Status": "Crawler already exists"}
    except glue_client.exceptions.EntityNotFoundException:
        pass  # Crawler does not exist, proceed to create it

    response = glue_client.create_crawler(
        Name=crawler_name,
        Role=config['aws']['glue']['role'],
        DatabaseName=database_name,
        Targets={
            "S3Targets": [
                {
                    "Path": s3_target_path,
                }
            ]
        },
        TablePrefix="",
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
    )

    return response


def run_glue_crawler(crawler_name):
    glue_client = aws_client.get_glue_client()
    glue_client.start_crawler(Name=crawler_name)
    print(f"Started crawler: {crawler_name}")

    # Wait for the crawler to finish
    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        state = response["Crawler"]["State"]
        if state == "READY": # change to stopping
            print(f"Crawler {crawler_name} finished successfully.")
            break
        elif state == "FAILED":
            raise Exception(f"Crawler {crawler_name} failed.")
        else:
            print(f"Crawler {crawler_name} is in state: {state}. Waiting...")
            time.sleep(10)  # Wait for 10 seconds before checking again
