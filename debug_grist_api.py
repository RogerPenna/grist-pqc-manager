import requests
import os
import json

# Manually setting the key from your previous context or environment
# For this script I'll assume it's set in the environment or I'll read it
try:
    with open('.env', 'r') as f:
        for line in f:
            if line.startswith('GRIST_API_KEY'):
                os.environ['GRIST_API_KEY'] = line.split('=')[1].strip()
except:
    pass

API_KEY = os.getenv("GRIST_API_KEY")
ORG_ID = "54594" # Prêmio da Qualidade Contábil
BASE_URL = "https://docs.getgrist.com/api"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

print(f"Checking workspaces for Org ID: {ORG_ID}")

try:
    response = requests.get(f"{BASE_URL}/orgs/{ORG_ID}/workspaces", headers=HEADERS)
    data = response.json()
    print("Response type:", type(data))
    if isinstance(data, list) and len(data) > 0:
        print("First workspace keys:", data[0].keys())
        if 'docs' in data[0]:
            print("Docs found in workspace!")
        else:
            print("WARNING: 'docs' key NOT found in workspace object.")
    else:
        print("Response data:", data)

except Exception as e:
    print(f"Error: {e}")
