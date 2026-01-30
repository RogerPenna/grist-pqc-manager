import streamlit as st
import pandas as pd
import requests
import os
import time
from dotenv import load_dotenv

# Page Configuration
st.set_page_config(
    page_title="Gestor PQC - Grist",
    page_icon="📊",
    layout="wide"
)

# 1. Configuration & Setup
load_dotenv()
API_KEY = os.getenv("GRIST_API_KEY")
GENERIC_BASE_URL = "https://docs.getgrist.com/api"

if not API_KEY:
    st.error("❌ GRIST_API_KEY não encontrada no arquivo .env")
    st.stop()

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest"
}

# 2. API Helper Functions with Caching

@st.cache_data(ttl=300)
def get_orgs():
    """Fetches available organizations."""
    try:
        response = requests.get(f"{GENERIC_BASE_URL}/orgs", headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Erro ao buscar organizações: {e}")
        return []

@st.cache_data(ttl=300)
def get_org_users(base_url, org_id):
    """Fetches users at the organization level."""
    try:
        url = f"{base_url}/orgs/{org_id}/access"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return data.get("users", [])
    except Exception as e:
        st.error(f"Erro ao buscar usuários da organização: {e}")
        return []

@st.cache_data(ttl=300)
def get_workspaces_and_docs(base_url, org_id):
    """Fetches all workspaces and their documents for an org."""
    try:
        url = f"{base_url}/orgs/{org_id}/workspaces"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Erro ao buscar workspaces: {e}")
        return []

@st.cache_data(ttl=600)
def get_doc_users(base_url, doc_id):
    """Fetches users assigned to a specific document."""
    try:
        url = f"{base_url}/docs/{doc_id}/access"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return data.get("users", [])
    except Exception as e:
        return []

def update_doc_access(base_url, doc_id, email, role):
    """Updates user access via PATCH /access with delta."""
    try:
        url = f"{base_url}/docs/{doc_id.strip()}/access"
        payload = {"delta": {"users": {email.strip(): role}}}
        response = requests.patch(url, headers=HEADERS, json=payload)
        if response.status_code != 200:
             return False, f"Erro {response.status_code}: {response.text}"
        return True, "Sucesso!"
    except Exception as e:
        return False, str(e)

# --- ACL HELPER FUNCTIONS ---

def fetch_table_records(base_url, doc_id, table_name):
    """Fetches all records from a table."""
    try:
        url = f"{base_url}/docs/{doc_id}/tables/{table_name}/records"
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 404:
            return [] # Table usually doesn't exist if no rules
        if response.status_code == 403:
            raise PermissionError("Acesso negado. É necessário ser OWNER do documento para ler metadados de regras.")
        response.raise_for_status()
        return response.json().get('records', [])
    except PermissionError as pe:
        raise pe
    except Exception:
        return []

def get_denormalized_rules(base_url, doc_id):
    """Fetches _grist_ACLRules and _grist_ACLResources and merges them."""
    try:
        rules_records = fetch_table_records(base_url, doc_id, '_grist_ACLRules')
        resources_records = fetch_table_records(base_url, doc_id, '_grist_ACLResources')
    except PermissionError as e:
        st.error(f"🚫 {e}")
        return []

    if not rules_records:
        return []

    # Map resource ID to object (tableId, colIds)
    # Resource fields: tableId, colIds
    res_map = {}
    for r in resources_records:
        rid = r['id']
        rf = r['fields']
        tid = rf.get('tableId') or "*"
        cids = rf.get('colIds') or "*"
        res_map[rid] = f"{tid} [{cids}]" if cids != "*" else tid
    
    denormalized = []
    for rule in rules_records:
        fields = rule['fields']
        res_id = fields.get('resource')
        
        # Resolve resource name
        resource_name = res_map.get(res_id, "Geral/Desconhecido")
        
        # Build display dict
        # Columns requested: Recurso, Condição, Permissões
        denormalized.append({
            "ID Regra": rule['id'],
            "Recurso": resource_name,
            "Condição": fields.get('aclFormula') or "(Sempre)",
            "Permissões": fields.get('permissionsText'),
            "Memo": fields.get('memo') or "",
            "Posição": fields.get('rulePos')
        })
    
    # Sort by rulePos
    denormalized.sort(key=lambda x: x.get('Posição', 0))
    return denormalized

# 3. Main UI Layout

st.title("🏆 Gestor de Acessos PQC-RS (Grist)")

# --- Sidebar: Org Selection ---
st.sidebar.header("Configuração")
orgs = get_orgs()

if not orgs:
    st.warning("Nenhuma organização encontrada.")
    st.stop()

org_map = {f"{org['name']} ({org['id']})": org['id'] for org in orgs}
org_domain_map = {org['id']: org.get('domain') for org in orgs}

# Try to default to PQC
default_idx = 0
keys_list = list(org_map.keys())
for i, name_with_id in enumerate(keys_list):
    if "Qualidade Contábil" in name_with_id:
        default_idx = i
        break

selected_org_key = st.sidebar.selectbox(
    "Selecione a Organização", 
    keys_list, 
    index=default_idx, 
    key="org_selector_main"
)
selected_org_id = org_map[selected_org_key]
selected_org_name = selected_org_key # For display purposes
selected_domain = org_domain_map.get(selected_org_id)

# --- DETECTOR DE MUDANÇA DE ORG ---
if "last_org_id" not in st.session_state:
    st.session_state.last_org_id = selected_org_id

if st.session_state.last_org_id != selected_org_id:
    # Org mudou! Limpa dados da org anterior
    st.session_state.mapped_data = None
    st.session_state.last_org_id = selected_org_id
    st.cache_data.clear() # Limpa cache para forçar recarga da nova org
    st.toast(f"Trocando para: {selected_org_name}")

# Definição da URL Base Dinâmica
# Personal orgs often return a shard domain (e.g. docs-26) which might not have the API endpoint active or requires auth tweaks.
# It is safer to use the generic docs.getgrist.com for Personal.
is_personal = "personal" in selected_org_name.lower()

if selected_domain and not is_personal:
    CURRENT_BASE_URL = f"https://{selected_domain}.getgrist.com/api"
else:
    CURRENT_BASE_URL = GENERIC_BASE_URL

st.sidebar.caption(f"📍 Base URL: {CURRENT_BASE_URL}")

if st.sidebar.button("🔄 Forçar Recarga Geral", key="force_reload_btn"):
    st.cache_data.clear()
    st.session_state.mapped_data = None
    st.rerun()

# Main Content Tabs
tab1, tab2, tab3, tab4 = st.tabs(["👥 Visão Global (Org)", "🗺️ Mapeamento de Documentos", "⚡ Ações Rápidas", "🛡️ Auditoria de Regras"])

# --- TAB 1: Global Organization Users ---
with tab1:
    st.header(f"Usuários: {selected_org_name}")
    
    col_g1, col_g2 = st.columns(2)
    with col_g1:
        f_name = st.text_input("Filtrar por Nome", key="search_g_name")
    with col_g2:
        f_email = st.text_input("Filtrar por Email", key="search_g_email")

    users = get_org_users(CURRENT_BASE_URL, selected_org_id)
    
    if users:
        df_users = pd.DataFrame(users)
        df_display = df_users.rename(columns={'email': 'Email', 'name': 'Nome', 'access': 'Acesso Global'})
        df_display = df_display[['Email', 'Nome', 'Acesso Global']]
        
        if f_name:
            df_display = df_display[df_display['Nome'].str.contains(f_name, case=False, na=False, regex=False)]
        if f_email:
            df_display = df_display[df_display['Email'].str.contains(f_email, case=False, na=False, regex=False)]
            
        st.dataframe(df_display, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum usuário encontrado.")

# --- TAB 2: Document Mapping ---
with tab2:
    st.header("Mapeamento de Documentos")
    
    if 'mapped_data' not in st.session_state:
        st.session_state.mapped_data = None

    if st.button("🚀 Iniciar Mapeamento Completo", key="start_map_btn"):
        with st.status("Varrendo documentos...", expanded=True) as status:
            workspaces = get_workspaces_and_docs(CURRENT_BASE_URL, selected_org_id)
            all_docs = []
            for ws in workspaces:
                for doc in ws.get('docs', []):
                    all_docs.append({'id': doc['id'], 'name': doc['name'], 'ws': ws.get('name')})
            
            consolidated = []
            progress = st.progress(0)
            for i, doc in enumerate(all_docs):
                doc_users = get_doc_users(CURRENT_BASE_URL, doc['id'])
                d_name = doc['name'].strip()
                if doc_users:
                    for u in doc_users:
                        acc = u.get('access') or f"{u.get('parentAccess')} (Herdado)"
                        consolidated.append({
                            'Selecionar': False,
                            'Documento': d_name,
                            'Email': (u.get('email') or "").strip(),
                            'Nome': (u.get('name') or "").strip(),
                            'Nível de Acesso': acc,
                            'Workspace': doc['ws'],
                            'Doc ID': doc['id']
                        })
                else:
                    consolidated.append({
                        'Selecionar': False, 'Documento': d_name, 'Email': '-', 'Nome': '-',
                        'Nível de Acesso': 'Indefinido', 'Workspace': doc['ws'], 'Doc ID': doc['id']
                    })
                progress.progress((i + 1) / len(all_docs))
            
            st.session_state.mapped_data = pd.DataFrame(consolidated)
            status.update(label="Mapeamento concluído!", state="complete")

    if st.session_state.mapped_data is not None:
        df = st.session_state.mapped_data
        
        st.markdown("### 🔍 Filtros")
        hide_inh = st.checkbox("Ocultar herdados", value=True, key="hide_inh_chk")
        
        c1, c2, c3, c4 = st.columns(4)
        f_doc = c1.text_input("Doc", key="f_doc")
        f_em = c2.text_input("Email", key="f_em")
        f_nm = c3.text_input("Nome", key="f_nm")
        f_ac = c4.text_input("Acesso", key="f_ac")

        df_f = df.copy()
        if hide_inh:
            df_f = df_f[~df_f['Nível de Acesso'].str.contains("Herdado|Indefinido", case=False, na=False)]
        if f_doc: df_f = df_f[df_f['Documento'].str.contains(f_doc, case=False, na=False, regex=False)]
        if f_em: df_f = df_f[df_f['Email'].str.contains(f_em, case=False, na=False, regex=False)]
        if f_nm: df_f = df_f[df_f['Nome'].str.contains(f_nm, case=False, na=False, regex=False)]
        if f_ac: df_f = df_f[df_f['Nível de Acesso'].str.contains(f_ac, case=False, na=False, regex=False)]

        def style_acc(v):
            v = str(v).lower()
            if 'owner' in v: return 'background-color: #ffcccc'
            if 'editor' in v: return 'background-color: #cce5ff'
            if 'viewer' in v: return 'background-color: #e6ffcc'
            return 'color: #999'

        edited_df = st.data_editor(
            df_f.style.map(style_acc, subset=['Nível de Acesso']),
            use_container_width=True, hide_index=True,
            column_config={"Doc ID": None, "Selecionar": st.column_config.CheckboxColumn("Sel", default=False)},
            disabled=["Documento", "Email", "Nome", "Nível de Acesso", "Workspace"],
            key="editor_mapping"
        )
        
        selected = edited_df[edited_df['Selecionar']]
        
        if not selected.empty:
            st.divider()
            st.subheader(f"📦 Operações em Massa ({len(selected)} itens)")
            
            # Options
            all_docs_list = st.session_state.mapped_data[['Documento', 'Doc ID']].drop_duplicates()
            doc_opts = {r['Documento']: r['Doc ID'] for _, r in all_docs_list.iterrows()}
            
            col_a, col_b = st.columns(2)
            
            with col_a:
                dest = st.selectbox("Documento Destino", sorted(doc_opts.keys()), index=None, placeholder="Pesquise...", key="bulk_dest")
                if st.button("📄 Copiar", key="btn_bulk_cp", disabled=not dest):
                    target_id = doc_opts[dest]
                    for _, row in selected.iterrows():
                        role = 'editors'
                        if 'owner' in row['Nível de Acesso'].lower(): role = 'owners'
                        elif 'viewer' in row['Nível de Acesso'].lower(): role = 'viewers'
                        update_doc_access(CURRENT_BASE_URL, target_id, row['Email'], role)
                    st.toast("Cópia finalizada!")
                    st.cache_data.clear(); time.sleep(1); st.rerun()

                if st.button("🚚 Mover", key="btn_bulk_mv", disabled=not dest):
                    target_id = doc_opts[dest]
                    for _, row in selected.iterrows():
                        role = 'editors'
                        if 'owner' in row['Nível de Acesso'].lower(): role = 'owners'
                        elif 'viewer' in row['Nível de Acesso'].lower(): role = 'viewers'
                        update_doc_access(CURRENT_BASE_URL, target_id, row['Email'], role)
                        update_doc_access(CURRENT_BASE_URL, row['Doc ID'], row['Email'], None)
                    st.toast("Movimentação finalizada!")
                    st.cache_data.clear(); time.sleep(1); st.rerun()

            with col_b:
                new_lvl = st.selectbox("Alterar Nível", ["viewers", "editors", "owners"], key="bulk_lvl")
                if st.button("✏️ Atualizar Selecionados", key="btn_bulk_upd"):
                    for _, row in selected.iterrows():
                        update_doc_access(CURRENT_BASE_URL, row['Doc ID'], row['Email'], new_lvl)
                    st.toast("Nível atualizado!")
                    st.cache_data.clear(); time.sleep(1); st.rerun()
                
                if st.button("🗑️ Remover Selecionados", key="btn_bulk_rm", type="primary"):
                    for _, row in selected.iterrows():
                        update_doc_access(CURRENT_BASE_URL, row['Doc ID'], row['Email'], None)
                    st.toast("Removidos com sucesso!")
                    st.cache_data.clear(); time.sleep(1); st.rerun()

# --- TAB 3: Quick Actions ---
with tab3:
    st.header("⚡ Ações Rápidas")
    if st.session_state.mapped_data is not None:
        all_docs_q = st.session_state.mapped_data[['Documento', 'Doc ID']].drop_duplicates()
        doc_opts_q = {r['Documento']: r['Doc ID'] for _, r in all_docs_q.iterrows()}
        target_q = st.selectbox("Selecionar Documento", sorted(doc_opts_q.keys()), index=None, placeholder="Buscar...", key="q_doc_sel")
        
        if target_q:
            tid = doc_opts_q[target_q]
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("🟢 Adicionar")
                em = st.text_input("Email", key="q_add_em")
                rl = st.selectbox("Nível", ["viewers", "editors", "owners"], key="q_add_rl")
                if st.button("Adicionar", key="q_add_btn"):
                    s, m = update_doc_access(CURRENT_BASE_URL, tid, em, rl)
                    st.toast(m)
                    st.cache_data.clear(); time.sleep(1); st.rerun()
            with c2:
                st.subheader("🔴 Remover")
                em_r = st.text_input("Email", key="q_rm_em")
                if st.button("Remover", key="q_rm_btn"):
                    s, m = update_doc_access(CURRENT_BASE_URL, tid, em_r, None)
                    st.toast(m)
                    st.cache_data.clear(); time.sleep(1); st.rerun()
    else:
        st.info("Faça o mapeamento na aba anterior primeiro.")

# --- TAB 4: Rules Audit ---
import os
from datetime import datetime

# ... existing code ...

def backup_rules_locally(doc_name, rules_data):
    """Saves rules to backups/ folder."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = doc_name.replace(" ", "_")
        filename = f"backups/rules_{safe_name}_{timestamp}.json"
        
        import json
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(rules_data, f, indent=2, ensure_ascii=False)
        return True, filename
    except Exception as e:
        return False, str(e)

def find_or_create_resource(base_url, doc_id, resource_str):
    """
    Parses resource string "Table [Cols]" -> finds/creates ID in _grist_ACLResources.
    """
    # Parse string
    import re
    table_id = "*"
    col_ids = "*"
    
    # Format: "Table" or "Table [Col1, Col2]"
    match = re.match(r"^(.*?) \[ (.*?) \]$", resource_str) # Simple regex, might need tweaking for spaces
    # Actually, let's look at how we formatted it: f"{tid} [{cids}]" if cids != "*"
    
    if "[" in resource_str and resource_str.endswith("]"):
        parts = resource_str.split(" [")
        table_id = parts[0].strip()
        col_ids = parts[1].rstrip("]").strip()
    else:
        table_id = resource_str.strip()
        col_ids = "*"
        
    # 1. Search existing
    # We need to fetch all resources again to check. Inefficient for loop, but safest.
    # Optimization: Pass a cache map if possible. For now, fetch all.
    all_res = fetch_table_records(base_url, doc_id, '_grist_ACLResources')
    for r in all_res:
        rf = r['fields']
        r_tid = rf.get('tableId') or "*"
        r_cids = rf.get('colIds') or "*"
        if r_tid == table_id and r_cids == col_ids:
            return r['id']

    # 2. Create if not found
    url = f"{base_url}/docs/{doc_id}/tables/_grist_ACLResources/records"
    payload = {
        'records': [{
            'fields': {
                'tableId': table_id,
                'colIds': col_ids
            }
        }]
    }
    resp = requests.post(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    # Returns {records: [{id: 123}]}
    return resp.json()['records'][0]['id']

def apply_denormalized_rules(base_url, doc_id, new_rules_json):
    """Renormalizes and overwrites _grist_ACLRules."""
    # 1. Delete all existing Rules
    # Fetch IDs first
    current = fetch_table_records(base_url, doc_id, '_grist_ACLRules')
    if current:
        ids_to_del = [r['id'] for r in current]
        # Chunking delete just in case? API usually handles valid payloads.
        url_del = f"{base_url}/docs/{doc_id}/tables/_grist_ACLRules/data/delete"
        requests.post(url_del, headers=HEADERS, json=ids_to_del)
    
    # 2. Prepare new records
    records_to_add = []
    
    # Cache resources to avoid re-fetching per rule
    # Actually, find_or_create does a fetch. To optimize, we should fetch once.
    # Let's just trust find_or_create for now, or optimizing is better.
    # Simple optimization: Fetch all resources once, build map.
    all_res = fetch_table_records(base_url, doc_id, '_grist_ACLResources')
    res_map = {} # Key: "tid|cids" -> ID
    for r in all_res:
        rf = r['fields']
        k = f"{rf.get('tableId') or '*'}|{rf.get('colIds') or '*'}"
        res_map[k] = r['id']
        
    for i, rule in enumerate(new_rules_json):
        # Resolve Resource
        r_str = rule.get('Recurso', 'Geral')
        # Parse
        if "[" in r_str and r_str.endswith("]"):
            parts = r_str.split(" [")
            tid = parts[0].strip()
            cids = parts[1].rstrip("]").strip()
        else:
            tid = r_str.strip()
            cids = "*"
            
        key = f"{tid}|{cids}"
        res_id = res_map.get(key)
        
        if not res_id:
            # Create
            url_res = f"{base_url}/docs/{doc_id}/tables/_grist_ACLResources/records"
            payload = {'records': [{'fields': {'tableId': tid, 'colIds': cids}}]}
            resp = requests.post(url_res, headers=HEADERS, json=payload)
            if resp.status_code == 200:
                new_id = resp.json()['records'][0]['id']
                res_map[key] = new_id
                res_id = new_id
            else:
                raise Exception(f"Falha ao criar recurso {r_str}: {resp.text}")

        # Build Rule Record
        records_to_add.append({
            'fields': {
                'resource': res_id,
                'aclFormula': rule.get('Condição', ''),
                'permissionsText': rule.get('Permissões', ''),
                'memo': rule.get('Memo', ''),
                'rulePos': i + 1 # Force strict ordering
            }
        })
        
    # 3. Batch Insert
    if records_to_add:
        url_add = f"{base_url}/docs/{doc_id}/tables/_grist_ACLRules/records"
        requests.post(url_add, headers=HEADERS, json={'records': records_to_add})

    return True

# --- ... inside Tab 4 ... ---
with tab4:
    st.header("🛡️ Auditoria de Regras (Access Rules)")
    st.info("Visualização avançada das regras de acesso (tabelas _grist_ACLRules e _grist_ACLResources).")
    
    # ... doc selector (re-use existing) ...
    if st.session_state.mapped_data is not None:
        all_docs_list = st.session_state.mapped_data[['Documento', 'Doc ID']].drop_duplicates()
        doc_opts_r = {r['Documento']: r['Doc ID'] for _, r in all_docs_list.iterrows()}
    else:
        wss = get_workspaces_and_docs(CURRENT_BASE_URL, selected_org_id)
        doc_opts_r = {}
        for ws in wss:
            for d in ws.get('docs', []):
                doc_opts_r[d['name']] = d['id']
    
    target_r_name = st.selectbox("Selecionar Documento para Auditoria", sorted(doc_opts_r.keys()), index=None, key="acl_doc_sel_audit")
    
    if target_r_name:
        target_r_id = doc_opts_r[target_r_name]
        
        # SUB-TABS
        sub_t1, sub_t2 = st.tabs(["👁️ Visualizar", "✍️ Editar Regras"])
        
        with sub_t1:
            if st.button("🔍 Carregar Regras", key="btn_load_acl_audit"):
                with st.spinner("Buscando metadados de regras..."):
                    data = get_denormalized_rules(CURRENT_BASE_URL, target_r_id)
                    st.session_state.acl_audit_data = data
                    if not data:
                        st.warning("Nenhuma regra encontrada ou erro de permissão.")
                    else:
                        st.success(f"{len(data)} regras encontradas!")

            if 'acl_audit_data' in st.session_state and st.session_state.acl_audit_data:
                df_rules = pd.DataFrame(st.session_state.acl_audit_data)
                
                # Filtros
                f_rec = st.text_input("Filtrar por Recurso (Tabela)", key="audit_filter_rec")
                if f_rec:
                    df_rules = df_rules[df_rules['Recurso'].str.contains(f_rec, case=False, na=False)]

                st.dataframe(
                    df_rules,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ID Regra": None, # Hide ID
                        "Posição": None,
                        "Memo": st.column_config.TextColumn("Descrição/Memo", width="medium"),
                        "Recurso": st.column_config.TextColumn("Recurso", width="medium"),
                        "Condição": st.column_config.TextColumn("Fórmula de Condição", width="large"),
                        "Permissões": st.column_config.TextColumn("Permissões", width="small"),
                    }
                )

                # --- EXPORT SECTION ---
                st.divider()
                st.subheader("📥 Exportar para IA (JSON)")
                
                import json
                export_data = df_rules.to_dict(orient='records')
                # Remove internal ID for export cleanliness
                for d in export_data:
                    d.pop('ID Regra', None)
                    d.pop('Posição', None) # We rely on list order implicitly or re-gen it
                    
                json_str = json.dumps(export_data, indent=2, ensure_ascii=False)
                
                st.download_button(
                    label="💾 Baixar JSON de Regras",
                    data=json_str,
                    file_name=f"regras_{target_r_name.replace(' ', '_')}.json",
                    mime="application/json"
                )
                
                st.text_area("JSON para Copiar", value=json_str, height=300, key="acl_json_export_area")

        with sub_t2:
            st.header("Modificar Regras")
            st.warning("⚠️ Cuidado: Esta operação substitui TODAS as regras do documento.")
            
            edit_json = st.text_area("Cole o novo JSON de regras aqui:", height=400, key="edit_json_area")
            
            if st.button("📤 Enviar Regras para o Grist", type="primary"):
                if not edit_json.strip():
                    st.error("JSON vazio!")
                else:
                    try:
                        import json
                        new_rules = json.loads(edit_json)
                        
                        # 1. Backup Current
                        st.info("Criando backup das regras atuais...")
                        current_data = get_denormalized_rules(CURRENT_BASE_URL, target_r_id)
                        ok_bkp, path_bkp = backup_rules_locally(target_r_name, current_data)
                        
                        if ok_bkp:
                            st.success(f"Backup salvo em: {path_bkp}")
                            
                            # 2. Apply
                            with st.spinner("Aplicando novas regras..."):
                                apply_denormalized_rules(CURRENT_BASE_URL, target_r_id, new_rules)
                                st.balloons()
                                st.success("Regras atualizadas com sucesso! Verifique na aba 'Visualizar'.")
                                st.cache_data.clear()
                        else:
                            st.error(f"Falha no backup: {path_bkp}. Operação abortada.")
                            
                    except json.JSONDecodeError:
                        st.error("Erro: JSON inválido.")
                    except Exception as e:
                        st.error(f"Erro ao aplicar regras: {e}")



        