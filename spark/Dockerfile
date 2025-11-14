FROM python:3.10-slim

WORKDIR /app

# Install your required libraries (adjust as needed)
RUN pip install kafka-python pymongo

COPY . /app

CMD ["python3", "spark_job.py"]
