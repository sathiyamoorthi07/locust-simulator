
from dotenv import load_dotenv
import os
import json
import random
import string
import time
from locust import HttpUser, task, between
import pandas as pd  # Pandas used for processing Excel files
load_dotenv()

# Constants (replace with your settings)
CONTACT_FOLDER = "./data"  # Folder containing Excel files
  # Path after the base host (in Locust, it will append the full host)

NUM_OF_FILE_LOADS = int(os.getenv('NUM_OF_FILE_LOADS', 1))
TEMPLATE_NAME = os.getenv('TEMPLATE_NAME', "load_test")
BUSINESS_ID = os.getenv('BUSINESS_ID', "")

WEBHOOK_URL = "/api/v1/push?sms=1&api_key="+BUSINESS_ID

EXCEL_FILES = [
    f for f in os.listdir(CONTACT_FOLDER) if f.endswith(".xlsx")
][:NUM_OF_FILE_LOADS]  # Load only the specified number of files

def generate_random_string():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))


def preprocess_message(entry):
    return {
        "mobile_number":str(entry["wa_id"]),
        "template_id":TEMPLATE_NAME,
        "object": "whatsapp_business_account"
    }

class LoadTestUser(HttpUser):
    wait_time = between(1, 3)  # Simulate think time between requests

    @task
    def send_requests_from_excel(self):
        if not EXCEL_FILES:
            print("No Excel files found in the folder.")
            return

        for file in EXCEL_FILES:
            file_path = os.path.join(CONTACT_FOLDER, file)
            df = pd.read_excel(file_path)  # Read the Excel file
            entries = df.to_dict(orient="records")  # Convert each row to a dictionary

            batch = []
            for idx, entry in enumerate(entries):
                message = preprocess_message(entry)

                with self.client.post(
                        WEBHOOK_URL, json=message, catch_response=True
                ) as response:
                    if response.status_code == 200:
                        print(f"Success: {file} - Entry {idx} - Status {response.status_code}")
                        response.success()
                    else:
                        print(f"Error: {file} - Entry {idx} - Status {response.status_code}")
                        response.failure(f"Failed with status {response.status_code}")

            print(f"Finished processing file: {file}")

        # Save results to JSON (optional)
        with open("report.json", "w") as report_file:
            report_file.write(json.dumps({"processed_files": len(EXCEL_FILES)}, indent=2))
