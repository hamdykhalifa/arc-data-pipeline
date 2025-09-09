# Use official Python slim image
FROM python:3.12-slim

# Set environment variables to prevent .pyc files and buffers
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory inside the container
WORKDIR /app

# Install system dependencies if needed (optional: ex. for pandas/pyarrow)
RUN apt-get update && \
    apt-get install -y gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy project files into the container
COPY . .

# Default command
CMD ["python", "main.py"]