import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GRIST_API_KEY")
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def debug_personal():
    print("--- 1. Buscando Organizações ---")
    r = requests.get("https://docs.getgrist.com/api/orgs", headers=HEADERS)
    if r.status_code != 200:
        print("Erro orgs:", r.text)
        return
    
    orgs = r.json()
    personal_orgs = []
    for org in orgs:
        if "Personal" in org['name']:
            personal_orgs.append(org)
            
    if not personal_orgs:
        print("\nNenhuma org 'Personal' encontrada!")
        return

    print(f"\nEncontradas {len(personal_orgs)} organizações 'Personal'. Listando conteúdo de TODAS...\n")
    
    base_url = "https://docs.getgrist.com/api"
    
    for p_org in personal_orgs:
        print(f"==================================================")
        print(f"📂 ORG: {p_org['name']} | ID: {p_org['id']} | Domain: {p_org.get('domain')}")
        print(f"==================================================")
        
        url = f"{base_url}/orgs/{p_org['id']}/workspaces"
        try:
            r_ws = requests.get(url, headers=HEADERS)
            if r_ws.status_code != 200:
                print(f"  ❌ Erro ao ler workspaces: {r_ws.status_code} - {r_ws.text}")
                continue
                
            workspaces = r_ws.json()
            if not workspaces:
                print("  (Vazio - Nenhum Workspace encontrado)")
                continue

            count_docs = 0
            for ws in workspaces:
                docs = ws.get('docs', [])
                ws_id = ws.get('id')
                if docs:
                    print(f"  📁 Workspace: '{ws['name']}' (ID: {ws_id})")
                    for d in docs:
                        print(f"     📄 {d['name']} (ID: {d['id']})")
                        count_docs += 1
                else:
                    print(f"  📁 Workspace: '{ws['name']}' (ID: {ws_id}) (Vazio)")
            
            if count_docs == 0:
                print("  (Nenhum documento encontrado nesta Org)")
                
        except Exception as e:
            print(f"  ❌ Exceção: {e}")
        
        print("\n")

if __name__ == "__main__":
    debug_personal()
