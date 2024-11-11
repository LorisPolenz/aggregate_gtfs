# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the script to the container
COPY main.py .
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir  -r requirements.txt

# Command to run the Python script
ENTRYPOINT [ "python", "main.py" ]
