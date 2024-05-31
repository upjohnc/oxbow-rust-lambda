import os
import sys

import polars as pl


def read_s3():
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"].strip(),
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"].strip(),
        "AWS_SESSION_TOKEN": os.environ["AWS_SESSION_TOKEN"].strip(),
        "AWS_REGION": "us-east-2",
    }

    file_path = "s3://chad-pinnsg-oxbow-simple/my_table/"

    df = pl.read_delta(file_path, storage_options=storage_options)
    print(df)


if __name__ == "__main__":
    read_s3()
