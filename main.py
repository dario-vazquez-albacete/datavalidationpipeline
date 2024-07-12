import os
import logging
import pandas as pd
import asyncio
from src.data_validation.validation_functions import (validate_data, retrieve_schema, create_validations_model)
import yaml
import json

# Configure and set a local logger for the pipeline. Filename is the name of the log file in the working directory. filemode can be set to 'w' which overwrites the file or 'a' to append lines the file
logging.basicConfig(filename='async_pipeline.log', filemode='w', format='{"timestamp": %(asctime)s, "level": %(levelname)s, "message":%(message)s}')
logger = logging.getLogger('async_queue')
logger.setLevel(logging.DEBUG)

# Load YAML file with pipeline configuration
with open("pipeline_config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Read and validate files in the read queue and hand them over to the next write queue
async def read_validate(read_queue: asyncio.Queue, write_queue: asyncio.Queue, logger: logging.Logger):
    """
    Asynchronously reads and validates data from files in a queue against a JSON schema stored in a duckdb database.

    This function continuously retrieves file paths from a queue, retrieves schema to validate them based on dataset name, validates them and transfers them to the write queue

    Parameters:
    queue: The queue from which file paths are retrieved.

    """
    while True:
        dataset, file_path = await read_queue.get()
        df = pd.read_csv(file_path).to_json(orient='records',force_ascii=False,date_format='iso')
        # serialize the dataframe to json for validaton
        df = json.loads(df)
        head, tail = os.path.split(file_path)
        file_name = tail.rstrip('.csv')
        # load the dataset validation schema from the database
        schema = await retrieve_schema('DataValidationDB01',dataset,logger)
        # validate the data using the validation schema
        status = await validate_data(df, schema, tail, logger)
        # if validaton passes hand over the validated data to the write queue
        if status:
            logger.info(f"File {file_path} successfully validated")
            await write_queue.put((df, file_path))
        read_queue.task_done()

# write validated data in the write queue to parquet files
async def write_files(write_queue: asyncio.Queue, logger: logging.Logger):
    """
    Asynchronously writes data from validated files in a queue to a file.

    This function continuously retrieves DataFrames from a queue and writes them to a parquet file.

    Parameters:
    queue: The queue from which DataFrames are retrieved.
    """
    while True:
        df, file_path = await write_queue.get()
        try:
            head, tail = os.path.split(file_path)
            # Write DataFrame to a parquet file
            file_name = tail.rstrip('.csv')
            parquet_file_path = f"./data/{file_name}_validated.parquet"
            df = pd.DataFrame(df)
            df.to_parquet(parquet_file_path)
            logger.info(f"DataFrame successfully written to: {parquet_file_path}")
        except Exception as e:
            logger.error(f"Error writing {file_name} to a file: {e}")
        write_queue.task_done()


async def main():
    create_validations_model("DataValidationDB01")
    read_queue = asyncio.Queue()
    write_queue = asyncio.Queue()
    for item in config["validations"]:
        dataset = item["dataset"]
        path = item["path"]
        # Start producer coroutines to populate the read queue
        await read_queue.put((dataset, path))
    # Start consumer coroutines to process files from the read queue and write to the write queue
    parse_task = asyncio.create_task(read_validate(read_queue, write_queue, logger))
    write_task = asyncio.create_task(write_files(write_queue, logger))
    # Wait for all tasks to complete
    await read_queue.join()
    await write_queue.join()
    parse_task.cancel()
    write_task.cancel()

if __name__ == "__main__":
    import time
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
