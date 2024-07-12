# Asynchronous queues for data validation with JSON schema

## Description

Data validation is an often overlooked aspect in many data projects. In this project I am exploring how to use JSON schema as data contract to validate data using Python. The rationale for using JSON schema instead of a Python library like Pydantic is the interoperability aspect. While Pydantic can export a model as JSON schema it would require an extra step to be able to validate this model by any other application or programming language such as R or Javascript. Another interesting aspect of JSON schema is that forces data owners to think about their data requirements upfront using a human and machine-readable format. For this project I also took the opportunity to implement queue data structures and asynchronous programming to make the pipeline more performant on large number of files. The pipeline uses an open clinical dataset as an example. In summary the is a short project implements several key aspects in data engineering and data management: 

* Queue data structures: Implement producer-consumer queues natively in Python to process files
* Asynchronous programming: Process files concurrently using asyncio to increase performance
* JSON schema: The tool to define data requirements and establish data contracts. Version DRAFT7 is used in this pipeline.
* Data validation: I use the json schema and the assocaited Python package to run the validations and log the validation results
* High observability: Manually set up logging for errors, validation results and save them into a flat file with the Python Logging module
* DuckDB database: Is an embedded OLAP database where the json schema is stored

## Project structure

```
├───data
├───src
│   ├───data_validation
│   │   ├───__init__.py
│   │   └───validation_functions.py
│   ├───schemas
│   │   ├───adlbc_schema.json
│   │   └───adsl_schema.json
├───main.py
├───pipeline_config.yaml
├───Readme.md
├───requirements.txt
```

## Getting Started

### Dependencies

* Ubuntu server 22.04
* Python 3.10 or higher

### Installing

* Create a Python virtual enviornment and install the dependencies

```
$python -m venv venv
$source env/bin/activate
$pip install -r requirements.txt
```

### Executing the pipeline

* Configure the pipeline to validate the desired clinical datasets in the pipeline_config.yaml file. Eah item inside validation references a dataset, the path where json schemas are stored and the relative path to the source file.

```
validations:
validations:
- dataset: adsl
  schema: ./src/schemas/adsl_schema.json
  path: ./data/adsl.csv
```
* Run the pipeline
```
$python main.py
```
* After you run the pipeline a log file called async_pipeline.log should appear in the root directory with the validation results and errors. You can also play around with the validation and change JSON schema or the datasets to observe how the pipeline validates it by looking at the resulting log file.
* The pipeline will also create a new DuckDB database file called DataValidationDB01 where the schemas will be loaded into a table called Validations. You can also query this database using duckdb python package to read or update the schemas or metadata.

### Extending the pipeline

The pipeline can validate virtually any dataset in CSV format given a valid json schema. In order to extend it:
* Add your dataset in CSV format in the data folder
* Add the json schema draft7 version with your definitions in the schema folder
* Add your new validation details to the pipeline_config.yml
* Run the pipeline

## Authors

Contributors names and contact info

Dario Vazquez (DAVA)

## Version History

* 0.1
    * First development version

## References

* https://json-schema.org/draft-07/json-schema-release-notes
* https://python-jsonschema.readthedocs.io/en/stable/