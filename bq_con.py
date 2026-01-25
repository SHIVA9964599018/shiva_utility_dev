from google.cloud import bigquery
from google.oauth2 import service_account

# Step 1: Path to your service account key
key_path = r"C:\Users\shivac\PycharmProjects\pythonProject\BQ_PRD_Key.json"

# Step 2: Load credentials
credentials = service_account.Credentials.from_service_account_file(key_path)

# Step 3: Initialize BigQuery client
client = bigquery.Client(credentials=credentials, project=credentials.project_id)
print(f"credentials.project_id:{credentials.project_id}")

# Step 4: Print the project ID
print(f"Project ID: {client.project}")

# Step 5: List and print all datasets in the project
print("Datasets in the project:")
datasets = client.list_datasets()  # returns an iterator
dataset_ids = [d.dataset_id for d in datasets]
print(f"Number of Datasets in the project {client.project}:{len(dataset_ids)}")

# for i, dataset in enumerate(dataset_ids):
#     if i >= 5:
#         break
#     print(f"- {dataset}")

tab_list=[]
while True:
    tab_list=input("\nenter the table names in comma seprated or line separated\n").upper().replace(",","\n")
    print(f"Entered tab list:\n {tab_list}")
    if input("Would you like to try with different inputs\n").lower()=="n":
       break