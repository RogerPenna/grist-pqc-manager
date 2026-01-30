import requests
import os
import json

# Setup env
try:
    with open('.env', 'r') as f:
        for line in f:
            if line.startswith('GRIST_API_KEY'):
                os.environ['GRIST_API_KEY'] = line.split('=')[1].strip()
except:
    pass

API_KEY = os.getenv("GRIST_API_KEY")
ORG_ID = "54594"
BASE_URL = "https://docs.getgrist.com/api"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# 1. Get first 5 docs to pick one for testing
print("--- Fetching Docs ---")
resp = requests.get(f"{BASE_URL}/orgs/{ORG_ID}/workspaces", headers=HEADERS)
data = resp.json()

target_doc_id = None
target_doc_name = None

for ws in data:
    if 'docs' in ws:
        for doc in ws['docs']:
            print(f"Found Doc: {doc['name']} (ID: {doc['id']})")
            # Let's pick '🎖️PQC RS 24-25 (Serra)' or similar if found, otherwise the first one
            if "Serra" in doc['name']:
                target_doc_id = doc['id']
                target_doc_name = doc['name']
                break
    if target_doc_id: break

if not target_doc_id:
    print("Could not find specific 'Serra' doc, picking the first available one...")
    # Fallback logic omitted for brevity, assuming we find something or re-run
    
if target_doc_id:
    print(f"\n--- INSPECTING DOC: {target_doc_name} ({target_doc_id}) ---")
    
    # Check /access endpoint
    url = f"{BASE_URL}/docs/{target_doc_id}/access"
    print(f"GET {url}")
    user_resp = requests.get(url, headers=HEADERS)
    
    print(f"Status Code: {user_resp.status_code}")
    try:
        user_data = user_resp.json()
        print("Raw JSON Response:")
        print(json.dumps(user_data, indent=2))
    except:
        print("Could not parse JSON response")
