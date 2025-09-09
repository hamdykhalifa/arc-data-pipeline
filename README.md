# Arc Data Pipeline

A lightweight, production-grade, containerized ETL pipeline using Python and Docker.
Extracts, transforms, and loads JSONPlaceholder “posts” data to Parquet files locally or to S3, with partitioning and data quality reporting.

---

## Features
- Configurable pipeline (env file or arguments)
- Data extraction from a REST API (JSONPlaceholder)
- Transformations: cleaning, validation, derived fields, deduplication
- Partitioned Parquet output (by userId)
- Simple data quality report
- Local or S3 loading (toggle by config)
- Logging, error handling, unit tests
- Dockerized for reproducibility

---

## Setup
1. Clone and Install
```
git clone https://github.com/hamdykhalifa/arc-data-pipeline.git
cd arc-data-pipeline
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Configure Environment Variables
Create an .env and fill in your values.

```
# .env
API_BASE_URL=https://jsonplaceholder.typicode.com/posts
USE_S3=False                # Set to True for S3 write
AWS_S3_BUCKET=your-s3-bucket    # Required if USE_S3=True
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-north-1
S3_PREFIX=posts
OUTPUT_DIR=data/output 
PARTITION_COLS=userId
```
---

## Running the Pipeline
*Locally (default, writes to data/output):*

```
python main.py
```

*With Docker:*
```
docker build -t arc-etl-pipeline .
docker run --rm --env-file .env -v $(pwd)/data:/app/data arc-etl-pipeline
```

*Output:*

- Parquet files (partitioned by userId) in data/output
- Data Quality Report: data/output/data_quality_report.json
- Or, files uploaded to S3 if USE_S3=True

---

## Testing
```
pytest tests/
```
Unit tests cover extraction, transformation, and loader logic.

---

## Configuration
Most settings live in .env:

- API source URL
- S3 credentials and bucket (optional)
- Toggle S3/local output
- Partition columns, output format (can also be set in code/config)

---

## Design & Extensibility
- Modular codebase: Each pipeline stage in its own file/class
- Partitioning ready: Output is organized for scalable patterns
- Data quality reporting: Generates simple JSON summary per run
- Adding new sources, destinations, or formats is straightforward

---

## Security
- S3 bucket is deployed in eu-north-1
- Programmatic user arc-data-pipeline-user is accessing the bucket via group permissions
- arc-data-pipeline-group is created which has access to S3 bucket