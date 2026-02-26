-- Create Airflow database
CREATE DATABASE airflow;

-- Grant permissions (station user owns both)
GRANT ALL PRIVILEGES ON DATABASE airflow TO station;