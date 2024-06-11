import os
import sys

import polars as pl


def read_local():
    print(pl.read_delta("./my_table"))


def read_s3():
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "").strip(),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "").strip(),
        "AWS_SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN", "").strip(),
        "AWS_REGION": "us-east-2",
    }

    file_path = "s3://chad-pinnsg-oxbow-simple/my_table/"

    df = pl.read_delta(file_path, storage_options=storage_options)
    print(df)


if __name__ == "__main__":
    data_locale = "local"
    if len(sys.argv) > 1 and sys.argv[1] in ["local", "s3"]:
        data_locale = sys.argv[1]

    if data_locale == "s3":
        read_s3()
    else:
        read_local()
