
from dotenv import load_dotenv
import os
import json
import random
import string
import time
from locust import HttpUser, task, between, TaskSet
import pandas as pd  # Pandas used for processing Excel files
load_dotenv()
from queue import Queue,Empty

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




class UserBehavior(TaskSet):
    def on_start(self):
        # Get a row of data for this user
        if not self.user.data_queue.empty():
            self.user.data = self.user.data_queue.get()
            # Re-add data to queue to loop indefinitely (optional)
            # self.user.data_queue.put(self.user.data)

    @task
    def send_request(self):

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