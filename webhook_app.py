import os
import json
import random
import string
import time
from locust import HttpUser, task, between
import pandas as pd  # Pandas used for processing Excel files
from dotenv import load_dotenv
load_dotenv()

# Constants (replace with your settings)
CONTACT_FOLDER = "./data"  # Folder containing Excel files
WEBHOOK_URL = "/webhook"  # Path after the base host (in Locust, it will append the full host)
NUM_OF_FILE_LOADS = int(os.getenv('NUM_OF_FILE_LOADS', 1))
EXCEL_FILES = [
    f for f in os.listdir(CONTACT_FOLDER) if f.endswith(".xlsx")
][:NUM_OF_FILE_LOADS]  # Load only the specified number of files

WBA_ID = str(os.getenv('WBA_ID',''))
PHONE_NUMBER_ID =str(os.getenv('PHONE_NUMBER_ID',''))
DISPLAY_PHONE_NUMBER = str(os.getenv('DISPLAY_PHONE_NUMBER',''))

def generate_random_string():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))


def preprocess_message(entry):
    wba_id = WBA_ID if WBA_ID != "" else str(entry["wba_id"])
    phone_number_id = PHONE_NUMBER_ID if PHONE_NUMBER_ID != "" else str(entry["phone_number_id"])
    display_phone_number = DISPLAY_PHONE_NUMBER if DISPLAY_PHONE_NUMBER != "" else str(entry["display_phone_number"])

    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": wba_id,
                "changes": [
                    {
                        "value": {
                            "messaging_product": "whatsapp",
                            "metadata": {
                                "display_phone_number": display_phone_number,
                                "phone_number_id": phone_number_id,
                            },
                            "contacts": [
                                {
                                    "profile": {"name": entry["profileName"]},
                                    "wa_id": str(entry["wa_id"]),
                                }
                            ],
                            "messages": [
                                {
                                    "from": str(entry["wa_id"]),
                                    "id": "wamid." + generate_random_string(),
                                    "timestamp": str(int(time.time())),
                                    "text": {"body": entry["textBody"]},
                                    "type": "text",
                                }
                            ],
                        },
                        "field": "messages",
                    }
                ],
            }
        ],
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
