# Pinnacle Solutions Group

In the README.adoc are the notes from the author
of the project.  This markdown page is the explanation
on the work done by Chad Upjohn of Pinnacle Solutions Group.

The two things done were to fix some errors with the lambda deploy (probably
due to some aws changes) and a refactor of the local delta log build.

## Running in a Lambda

The lambda creates and adds to a delta table in the bucket+prefix
of the file.  The function does it in place so there isn't any reading
of the parquet file into memory.  When the file is added to s3, the lambda
is triggered.

To create the lambda:

- Deploy Lambda:

```bash
% just cargo-lambda
% just tf-init
% just tf-plan
% just tf-apply
```

- Running Lambda:

```text
- add a folder for the table name in the bucket
- add a parquet file in the new folder
- Check that the delta table is set up:
    `just read-s3`
```

## Running Locally

The code is hardcoded to work with the directory `my_table`.
When the code is run, if a delta table does not exist, then one is created.
Like the lambda code, the parquet file is not read into memory.

```text
- create directory `my_table` (hardcoded in the main function of rust)
- add a parquet file from the `hold_data` directory into `my_table`
- run the code to create a new delta table : `just run-local-file`
- check that the table is created by reading the table: `just read-local`
- add a new file from the `hold_data` directory into `my_table`
- run the code to create a new delta table : `just run-local-file`
- check that the table is created by reading the table: `just read-local`
```

