import requests
import pandas as pd
from kafka import KafkaProducer
import csv
import time
import json
import os
import zipfile
import datetime
from typing import List
from dotenv import load_dotenv
from kafka_client.transformations import transform_row, transform_dataframe  # Import transformation functions
from constants import (
    TOPIC,
    BOOTSTRAP_SERVERS  
)

print("your bootstrap server is " + BOOTSTRAP_SERVERS + " and your topic is " + TOPIC) 
load_dotenv()


def get_data(dataset_url):
    """
    Downloads and extracts a dataset from a given URL.
    Returns the CSV file path if successful, else None.
    """
    dataset_filename = "online_retail.zip"
    dataset_dir = "dataset"

    # Download the dataset
    response = requests.get(dataset_url, stream=True)  # Enable streaming for large files
    if response.status_code == 200:
        with open(dataset_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        print("Dataset downloaded successfully.")
    else:
        print("Failed to download dataset.")
        return None  # Exit if download fails

    # Extract the dataset
    try:
        with zipfile.ZipFile(dataset_filename, 'r') as zip_ref:
            zip_ref.extractall(dataset_dir)
        print("Dataset extracted successfully.")
    except zipfile.BadZipFile:
        print("Error: Corrupt ZIP file.")
        return None

    # Locate the extracted CSV file
    csv_files = [f for f in os.listdir(dataset_dir) if f.endswith(".csv")]
    if not csv_files:
        print("No CSV file found in extracted data.")
        return None
    
    csv_path = os.path.join(dataset_dir, csv_files[0])  # Full path
    print(f"Extracted dataset: {csv_path}")

    return csv_path  # Return full path

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def publish_to_kafka(csv_file_path):
    """
    Reads a CSV file, applies transformations, and publishes rows as JSON messages to Kafka.
    """
    try:
        # Load the dataset into a DataFrame
        df = pd.read_csv(csv_file_path, encoding="ISO-8859-1")

        # Apply transformations
        df = transform_dataframe(df)

        for _, row in df.iterrows():
            try:
                message = transform_row(row)
                if message:  # Ensure transformation was successful
                    producer.send(TOPIC, value=message)
                    print(f"Published: {message}")
                    time.sleep(1)  # Simulate real-time streaming
            except Exception as row_error:
                print(f"Skipping row due to error: {row_error}")

    except Exception as e:
        print(f"Error while publishing to Kafka: {e}")

    finally:
        producer.flush()
        producer.close()

def stream():
    """ Function to be called by Airflow DAG to start Kafka producer """
    dataset_url = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
    csv_file_path = get_data(dataset_url)  # ✅ Get the CSV file
    if csv_file_path:
        publish_to_kafka(csv_file_path)  # ✅ Start publishing messages to Kafka
    else:
        print("Dataset preparation failed.")

if __name__ == "__main__":
    try:
        stream()  # ✅ Run the streaming process when executed directly
    except KeyboardInterrupt:
        print("Producer stopped.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        producer.flush()
        producer.close()
