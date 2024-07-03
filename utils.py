import re
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from aws_clients import aws_client
import time

def get_value_from_text(text, key):
    pattern = f'<{key}>(.*?)</{key}>'
    match = re.search(pattern, text, re.DOTALL)  # Added re.DOTALL flag
    if match:
        return match.group(1)
    return None  # or a default value

def convert_df_to_parquet(df):
    buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer, compression="zstd")
                        
    buffer.seek(0)  # Reset buffer pointer to the beginning
    return buffer
