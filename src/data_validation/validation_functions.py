from jsonschema import validate, ValidationError, Draft7Validator
import duckdb
import pandas as pd
import json
import logging

def create_validations_model(name):
    conn = duckdb.connect(name)
    conn.execute("""
            CREATE OR REPLACE TABLE Validations (
            id INTEGER PRIMARY KEY,
            dataset VARCHAR,
            validation_schema JSON,
            version INTEGER,
            valid_from DATE,
            expired_from DATE
            );""")
    with open('./src/schemas/adsl_schema.json') as json_data_file:
        adsl_schema = json.load(json_data_file)
    with open('./src/schemas/adlbc_schema.json') as json_data_file:
        adlbc_schema = json.load(json_data_file)
    conn.execute("INSERT INTO Validations VALUES (1, 'adsl',?, 1, today(),NULL);", (adsl_schema,))
    conn.execute("INSERT INTO Validations VALUES (2, 'adlbc',?, 1, today(),NULL);", (adlbc_schema,))
    conn.close()

async def validate_data(data: dict, schema:dict, file_name: str, logger: logging.Logger) -> bool:
        """
        Asynchronously validates data against a JSON schema.

        Parameters:
        data (dict): The data to be validated.
        schema (dict): The JSON schema against which the data is to be validated.
        file_name (str): A path to the file that will be validated
        logger (logging.Logger): The logger to use for logging errors. It is a Logger class from logging package

        Returns:
        bool: True if the data is valid according to the schema, False otherwise.

        Raises:
        Exception: If the data cannot be validated
        """
        try:
            # Validate data against the JSON schema
            validator = Draft7Validator(schema)
            if validator.is_valid(data):
                return True
            else:
                for error in validator.iter_errors(data):
                    error_data = {'cause': error.message, 'Field': list(error.path), 'File': file_name}
                    if 'SUBJID' in data and isinstance(data['SUBJID'], int):
                        error_data['SUBJID'] = data['SUBJID']
                    logger.error(error_data)
                return False
        except Exception as e:
            logger.error({'Error in validation step': {e}})
    
async def retrieve_schema(db_name: str, dataset: str, logger: logging.Logger) -> dict:
        """
        Asynchronously serializes a DataFrame and retrieves the corresponding validation schema from an embedded database.

        Parameters:
        db_name: A databasename where the schemas are stored as JSON object
        logger (logging.Logger): The logger to use for logging errors. It is a Logger class from logging package

        Returns:
        A validation schema in json format

        Raises:
        Exception: If an error occurs during serialization or database access.

        IMPORTANT! Requires a global logger configrued using the logging package
        """
        try:
            conn = duckdb.connect(f"{db_name}")
            print(conn.execute("DESCRIBE SELECT * FROM Validations"))
            df2 = conn.execute(f"""SELECT validation_schema ->> '$' AS schema FROM Validations
                               WHERE dataset=?;""", (dataset,)).fetch_df()['schema'].values[0]
            return json.loads(df2) 
        except Exception as e:
            logger.error({'Error retrieving the schema': {e}})