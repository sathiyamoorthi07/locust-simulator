import os
import json
import random
import string
import time

from gevent.pool import Group
from locust import HttpUser, task, between, TaskSet
import pandas as pd  # Pandas used for processing Excel files
from queue import Queue,Empty

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



class UserBehavior(TaskSet):
    def on_start(self):
        # Get a row of data for this user
        if not self.user.data_queue.empty():
            self.user.data = self.user.data_queue.get()
            # Re-add data to queue to loop indefinitely (optional)
            # self.user.data_queue.put(self.user.data)

    @task
    def send_request(self):

        try:
            # Get data from queue (blocks for 1 second)
            entry = self.user.data_queue.get(timeout=1)
        except Empty:
            # Stop user if queue is empty
            self.user.stop()
            return

        # Example: Send a POST request with user-specific data
        message = preprocess_message(self.user.data)
        with self.client.post(
                WEBHOOK_URL, json=message, catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed with status {response.status_code}")

        self.user.stop()

class LoadTestUser(HttpUser):
    tasks = [UserBehavior]
    wait_time = between(1, 3)  # Simulate think time between requests
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize data queue once per user class
        if not hasattr(self.__class__, 'data_queue'):
            self.__class__.data_queue = Queue()
            for file in EXCEL_FILES:
                file_path = os.path.join(CONTACT_FOLDER, file)
                df = pd.read_excel(file_path)  # Read the Excel file
                entries = df.to_dict(orient="records")
                for idx, entry in enumerate(entries):
                    self.__class__.data_queue.put(entry)
            print("DATA SET LOADED")