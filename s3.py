from aws_clients import aws_client, config
import pandas as pd
import io

def upload_to_s3(buffer, bucket_name, key):
    s3_client = aws_client.get_s3_client()
    s3_client.upload_fileobj(buffer, bucket_name, key)
    
# get csv results stored in s3.
#! move to s3 file
def get_csv_results(execution_id, result_folder):
    s3_client = aws_client.get_s3_client()
    file_name = f"{result_folder}/{execution_id}.csv"
    bucket = config['aws']['athena']['output_location'].split('/')[2]

    obj = s3_client.get_object(Bucket=bucket, Key=file_name)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="utf8")
    print(df)
    return df