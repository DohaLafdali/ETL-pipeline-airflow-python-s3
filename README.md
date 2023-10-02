# ETL Project with Apache Airflow

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow for task orchestration, Pandas for data transformation, and AWS S3 for storage.

## Accomplished Steps

1. **Data Extraction**:
   - Extracting weather data from the Open Meteo API.
   - Here is our API: https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m

2. **Data Transformation**:
   - Transforming data from JSON format to a readable DataFrame using Pandas.
   - Expanding 'hourly.time' and 'hourly.temperature_2m' columns for better analysis.

3. **Data Loading**:
   - Loading transformed data into AWS S3 using AWS Wrangler.
   ![Data Loading](https://github.com/DohaLafdali/ETL-pipeline-airflow-python-s3/assets/72882387/5f5edd84-b8c2-4b0d-905c-fcc9df736f93)

4. **Containerization with Docker**:
   - Configuring and running Apache Airflow in a Docker environment to ensure portability and ease of deployment.

5. **DAG Execution**:
   - Orchestrating the ETL pipeline by defining a DAG in Apache Airflow.
   ![DAG Execution](https://github.com/DohaLafdali/ETL-pipeline-airflow-python-s3/assets/72882387/09fe3ad4-e7bd-4372-8e3c-6cef6617ecd2)
   ![DAG Execution](https://github.com/DohaLafdali/ETL-pipeline-airflow-python-s3/assets/72882387/5689ba1b-4ce0-4911-9e66-48e1f78a2ec5)
   - Executing the DAG to automate the ETL process.
   ![DAG Execution](https://github.com/DohaLafdali/ETL-pipeline-airflow-python-s3/assets/72882387/d28773fc-b188-4977-a8ce-1b280ffca055)

## How to Run the Project

1. Ensure Docker is installed.
2. Clone this repository.
3. Create a `.env` file at the project root with the necessary environment variables.
4. Run `docker-compose up --build` to start the project.

## Project Structure

- **dags/**: Contains the DAG definition files for Apache Airflow.
- **logs/**: Location where Apache Airflow logs will be stored.
- **plugins/**: Directory for custom Apache Airflow plugins.
- **config/**: Location for configuration files.

## Author

[Doha Lafdali.]

---
