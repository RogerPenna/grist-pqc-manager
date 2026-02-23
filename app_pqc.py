import streamlit as st
import pandas as pd
import requests
import os
import time
import json
import re
from datetime import datetime
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

@st.cache_data(ttl=300)
def get_tables(base_url, doc_id):
    """Fetches list of tables for a document."""
    try:
        url = f"{base_url}/docs/{doc_id}/tables"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json().get('tables', [])
    except Exception as e:
        st.error(f"Erro ao buscar tabelas: {e}")
        return []

@st.cache_data(ttl=300)
def get_columns(base_url, doc_id, table_id):
    """Fetches columns for a specific table."""
    try:
        url = f"{base_url}/docs/{doc_id}/tables/{table_id}/columns"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json().get('columns', [])
    except Exception as e:
        st.error(f"Erro ao buscar colunas: {e}")
        return []

def add_table_record(base_url, doc_id, table_id, col_id, value):
    """Adds a new record to a table with a specific value in one column."""
    try:
        url = f"{base_url}/docs/{doc_id}/tables/{table_id}/records"
        payload = {
            "records": [{"fields": {col_id: value}}]
        }
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        return True, "Registro adicionado com sucesso!"
    except Exception as e:
        return False, str(e)

def create_table(base_url, doc_id, table_id, columns_payload):
    """Creates a new table in the document with the provided columns."""
    try:
        url = f"{base_url}/docs/{doc_id}/tables"
        # Grist requires columns to be present when creating a table
        payload = {"tables": [{"id": table_id, "columns": columns_payload}]}
        response = requests.post(url, headers=HEADERS, json=payload)
        if response.status_code == 200:
            return True, "Tabela e colunas criadas com sucesso!"
        # If table already exists, return a specific status
        if "already exists" in response.text.lower():
            return False, "EXISTING"
        return False, f"Erro {response.status_code}: {response.text}"
    except Exception as e:
        return False, str(e)

def add_columns(base_url, doc_id, table_id, columns_payload):
    """Adds columns to an existing table."""
    try:
        url = f"{base_url}/docs/{doc_id}/tables/{table_id}/columns"
        # columns_payload should be a list of {id, fields: {label, type, formula, isFormula, ...}}
        payload = {"columns": columns_payload}
        response = requests.post(url, headers=HEADERS, json=payload)
        if response.status_code == 200:
            return True, "Colunas adicionadas com sucesso!"
        return False, f"Erro {response.status_code}: {response.text}"
    except Exception as e:
        return False, str(e)

def load_audit_configs():
    """Loads saved audit configurations from JSON."""
    if os.path.exists("audit_configs.json"):
        try:
            with open("audit_configs.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_audit_config(name, config_data):
    """Saves a new audit configuration."""
    configs = load_audit_configs()
    configs[name] = config_data
    with open("audit_configs.json", "w", encoding="utf-8") as f:
        json.dump(configs, f, indent=2, ensure_ascii=False)

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

# Garante inicialização do mapped_data
if "mapped_data" not in st.session_state:
    st.session_state.mapped_data = None

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
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["👥 Visão Global (Org)", "🗺️ Mapeamento de Documentos", "⚡ Ações Rápidas", "🛡️ Auditoria de Regras", "❓ Ajuda", "⚖️ Auditoria de Integridade", "🏗️ Clonador de Templates"])

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
        
        # --- NEW: User Document Details (Cross-reference with Tab 2) ---
        st.divider()
        st.subheader("🕵️ Detalhes de Acesso por Usuário")
        
        if st.session_state.mapped_data is not None:
            # Use filtered emails from the main table
            valid_emails = sorted([e for e in df_display['Email'].unique() if e and e != '-'])
            
            selected_user_detail = st.selectbox("Selecione um Usuário (da lista acima) para ver seus Documentos:", valid_emails, key="sel_user_detail")
            
            if selected_user_detail:
                # Filter mapped data
                user_docs = st.session_state.mapped_data[st.session_state.mapped_data['Email'] == selected_user_detail]
                # Hide inherited access (requested by user)
                user_docs = user_docs[~user_docs['Nível de Acesso'].str.contains("Herdado", case=False, na=False)]
                
                if not user_docs.empty:
                    st.write(f"📂 **Documentos com acesso explícito para: {selected_user_detail}**")
                    st.dataframe(
                        user_docs[['Documento', 'Workspace', 'Nível de Acesso']], 
                        use_container_width=True, 
                        hide_index=True
                    )
                else:
                    st.warning(f"O usuário {selected_user_detail} não possui acessos diretos em documentos (apenas herdados ou nenhum).")
        else:
            st.info("💡 Para ver a lista de documentos de cada usuário aqui, vá até a aba **'🗺️ Mapeamento de Documentos'** e clique em **'Iniciar Mapeamento Completo'**.")
            
    else:
        st.info("Nenhum usuário encontrado.")

# --- TAB 2: Document Mapping ---
with tab2:
    st.header("Mapeamento de Documentos")
    
    # LOAD CACHED MAPPING ON STARTUP
    MAPPING_FILE = "mapping_cache.json"
    
    if st.session_state.mapped_data is None:
        if os.path.exists(MAPPING_FILE):
            try:
                with open(MAPPING_FILE, "r", encoding="utf-8") as f:
                    cache_obj = json.load(f)
                    # Check if org matches
                    if cache_obj.get("org_id") == selected_org_id:
                        st.session_state.mapped_data = pd.DataFrame(cache_obj["data"])
                        st.session_state.mapping_ts = cache_obj.get("timestamp", "")
            except Exception:
                pass # Ignore load errors

    # Display Timestamp
    if 'mapping_ts' in st.session_state and st.session_state.mapping_ts:
        st.caption(f"📅 Última atualização: {st.session_state.mapping_ts}")
    else:
        st.caption("⚠️ Nenhum mapeamento recente encontrado.")

    if st.button("🚀 Iniciar/Atualizar Mapeamento", key="start_map_btn"):
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
            
            df_map = pd.DataFrame(consolidated)
            st.session_state.mapped_data = df_map
            
            # SAVE TO CACHE
            ts_now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            st.session_state.mapping_ts = ts_now
            with open(MAPPING_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "org_id": selected_org_id,
                    "timestamp": ts_now,
                    "data": consolidated
                }, f, indent=2, ensure_ascii=False)
                
            status.update(label="Mapeamento concluído e salvo!", state="complete")

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

        # --- CONTADOR E SELEÇÃO EM MASSA ---
        st.info(f"📊 Mostrando **{len(df_f)}** de **{len(df)}** registros totais.")
        
        col_sel1, col_sel2, _ = st.columns([1, 1, 2])
        if col_sel1.button("✅ Selecionar Todos Filtrados"):
            st.session_state.mapped_data.loc[df_f.index, 'Selecionar'] = True
            st.rerun()
        if col_sel2.button("❌ Desmarcar Todos"):
            st.session_state.mapped_data['Selecionar'] = False
            st.rerun()

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
                st.subheader("🟢 Adicionar (Massa)")
                ems_raw = st.text_area("Email(s)", help="Cole um ou mais emails separados por vírgula, espaço ou linha.", key="q_add_ems")
                rl = st.selectbox("Nível", ["viewers", "editors", "owners"], key="q_add_rl")
                if st.button("Adicionar em Massa", key="q_add_btn"):
                    # Process emails
                    # Split by comma, space, or newline
                    emails = [e.strip().lower() for e in re.split(r'[,\s\n]+', ems_raw) if e.strip()]
                    
                    if not emails:
                        st.error("Nenhum email válido inserido.")
                    else:
                        success_count = 0
                        errors = []
                        with st.status(f"Adicionando {len(emails)} usuários...", expanded=True) as status:
                            for em in emails:
                                s, m = update_doc_access(CURRENT_BASE_URL, tid, em, rl)
                                if s:
                                    success_count += 1
                                    st.write(f"✅ {em}: Sucesso")
                                else:
                                    errors.append(f"{em}: {m}")
                                    st.write(f"❌ {em}: {m}")
                            status.update(label="Processamento concluído!", state="complete")
                        
                        if success_count > 0:
                            st.toast(f"{success_count} usuários adicionados!")
                        if errors:
                            st.error(f"Ocorreram {len(errors)} erros.")
                        
                        st.cache_data.clear(); time.sleep(1); st.rerun()
            with c2:
                st.subheader("🔴 Remover (Massa)")
                em_r_raw = st.text_area("Email(s)", help="Cole um ou mais emails para remover.", key="q_rm_ems")
                if st.button("Remover em Massa", key="q_rm_btn", type="primary"):
                    emails_r = [e.strip().lower() for e in re.split(r'[,\s\n]+', em_r_raw) if e.strip()]
                    
                    if not emails_r:
                        st.error("Nenhum email válido inserido.")
                    else:
                        success_count = 0
                        errors = []
                        with st.status(f"Removendo {len(emails_r)} usuários...", expanded=True) as status:
                            for em in emails_r:
                                s, m = update_doc_access(CURRENT_BASE_URL, tid, em, None)
                                if s:
                                    success_count += 1
                                    st.write(f"✅ {em}: Removido")
                                else:
                                    errors.append(f"{em}: {m}")
                                    st.write(f"❌ {em}: {m}")
                            status.update(label="Remoção concluída!", state="complete")
                        
                        if success_count > 0:
                            st.toast(f"{success_count} usuários removidos!")
                        if errors:
                            st.error(f"Ocorreram {len(errors)} erros.")
                            
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

# --- TAB 5: Ajuda ---
with tab5:
    st.markdown("""
    # 📘 Manual de Ajuda - Gestor PQC

    Bem-vindo ao **Gestor de Acessos PQC-RS**. Esta ferramenta foi desenvolvida para facilitar a administração de permissões, usuários e regras de acesso (ACL) dentro da sua organização Grist.

    ---

    ## 🚀 Primeiros Passos

    ### 1. Seleção de Organização
    Na barra lateral esquerda (**Sidebar**), você encontra o menu de configuração.
    - **Selecione a Organização**: Escolha qual organização Grist você deseja gerenciar. O sistema tenta selecionar automaticamente a "Qualidade Contábil" se disponível.
    - **Base URL**: O sistema ajusta automaticamente a URL da API (ex: `docs.getgrist.com` ou domínios personalizados).
    - **Forçar Recarga**: Use este botão se você fez alterações fora do sistema e quer garantir que os dados exibidos estejam 100% atualizados, limpando o cache local.

    ---

    ## 🛠️ Funcionalidades por Aba

    ### 👥 1. Visão Global (Org)
    Esta aba mostra todos os usuários que têm acesso à organização como um todo (não necessariamente a documentos específicos, mas ao "Team Site").
    - **Filtros**: Use os campos de texto para buscar por Nome ou Email.
    - **Dados**: Exibe Nome, Email e o Nível de Acesso Global.

    ### 🗺️ 2. Mapeamento de Documentos
    Esta é a ferramenta mais poderosa para auditoria em massa.
    1. **Botão "Iniciar Mapeamento Completo"**: Varre **todos** os workspaces e documentos da organização selecionada. Isso pode levar alguns segundos.
    2. **Tabela de Resultados**: Lista cada combinação de Usuário x Documento.
       - Usuários "Indefinidos" aparecem se o documento não tiver usuários explícitos listados na API de acesso.
    3. **Filtros**:
       - **Ocultar herdados**: Esconde acessos que vêm da organização/workspace, focando em acessos diretos.
       - Filtros por Doc, Email, Nome e Acesso.
    4. **Edição e Seleção**: Marque a caixa "Sel" (Selecionar) ao lado dos itens que deseja modificar.
    5. **📦 Operações em Massa** (aparecem após selecionar itens):
       - **📄 Copiar**: Copia o acesso dos usuários selecionados para um **Documento Destino**.
       - **🚚 Mover**: Copia o acesso para o destino e **remove** do documento original.
       - **✏️ Atualizar Nível**: Altera o papel (Viewer, Editor, Owner) dos usuários selecionados no documento atual.
       - **🗑️ Remover**: Remove o acesso dos usuários selecionados.

    ### ⚡ 3. Ações Rápidas
    Ideal para ajustes pontuais sem precisar rodar o mapeamento completo.
    - **Selecionar Documento**: Escolha o arquivo alvo.
    - **🟢 Adicionar**: Insira um email e escolha o nível (Viewer, Editor, Owner) para conceder acesso imediato.
    - **🔴 Remover**: Digite o email para revogar o acesso imediatamente.

    ### 🏗️ 7. Clonador de Templates
    Esta ferramenta permite copiar a estrutura (esquema) de uma tabela de um documento para outros.
    - **Origem**: Escolha o documento e a tabela que servem de modelo (ex: `Checklistdiamante`).
    - **Destinos**: Selecione um ou mais documentos onde você deseja que essa tabela seja criada.
    - **O que é copiado**: IDs das colunas, Nomes (Labels), Tipos de dados (Text, Numeric, Ref, etc), Fórmulas e Opções de Widget.
    - **O que NÃO é copiado**: Os dados (registros) da tabela.
    - **Nota**: Útil para padronizar documentos novos com as mesmas tabelas de suporte.

    ---

    ## 💡 Dicas e Solução de Problemas
    - **Cache**: O sistema guarda dados por 5 a 10 minutos para ser rápido. Se algo parecer desatualizado, use o botão **🔄 Forçar Recarga Geral** na barra lateral.
    - **Permissões**: Para ler ou escrever regras (Aba 4), seu usuário da API (`GRIST_API_KEY`) deve ser **DONO (Owner)** do documento.
    - **Erros de API**: Verifique se sua chave API no arquivo `.env` está correta e tem as permissões necessárias.
    """)

# --- TAB 6: Auditoria de Integridade ---
with tab6:
    st.header("⚖️ Auditoria de Integridade")
    st.markdown("Auditoria avançada comparando acessos reais com múltiplas colunas de referência.")

    # --- CONFIGURATION MANAGEMENT ---
    saved_configs = load_audit_configs()
    config_names = ["(Nova Configuração)"] + list(saved_configs.keys())
    
    col_cfg1, col_cfg2 = st.columns([3, 1])
    sel_config_name = col_cfg1.selectbox("📂 Carregar Configuração Salva", config_names, key="audit_config_loader")
    
    # Initialize session state for config inputs if loading
    if sel_config_name != "(Nova Configuração)":
        cfg = saved_configs[sel_config_name]
        # We store these temporarily to pre-fill, but selects depend on dynamic API calls
        # so we handle the defaults inside the widgets below using 'index' logic where possible
        # or just letting the user see the values.
        # Ideally, we set indices.
        pass

    st.divider()

    # --- SETUP FORM ---
    
    # 1. Document
    if st.session_state.mapped_data is not None:
        all_docs_list = st.session_state.mapped_data[['Documento', 'Doc ID']].drop_duplicates()
        doc_opts_audit = {r['Documento']: r['Doc ID'] for _, r in all_docs_list.iterrows()}
    else:
        wss = get_workspaces_and_docs(CURRENT_BASE_URL, selected_org_id)
        doc_opts_audit = {}
        for ws in wss:
            for d in ws.get('docs', []):
                doc_opts_audit[d['name']] = d['id']
    
    # Try to match loaded config
    def_doc_idx = None
    loaded_doc_id = None
    if sel_config_name != "(Nova Configuração)":
        loaded_doc_id = saved_configs[sel_config_name].get("doc_id")
        # Find index
        keys_d = sorted(doc_opts_audit.keys())
        for i, k in enumerate(keys_d):
            if doc_opts_audit[k] == loaded_doc_id:
                def_doc_idx = i
                break

    sel_doc_audit = st.selectbox("1. Documento Alvo", sorted(doc_opts_audit.keys()), index=def_doc_idx, key="audit_doc_sel")

    if sel_doc_audit:
        doc_id_audit = doc_opts_audit[sel_doc_audit]
        
        # 2. Table
        tables = get_tables(CURRENT_BASE_URL, doc_id_audit)
        table_opts = {t['id']: t['id'] for t in tables}
        
        def_tbl_idx = None
        if sel_config_name != "(Nova Configuração)":
            saved_tbl = saved_configs[sel_config_name].get("table_id")
            if saved_tbl in table_opts:
                keys_t = sorted(table_opts.keys())
                if saved_tbl in keys_t:
                    def_tbl_idx = keys_t.index(saved_tbl)

        sel_table_audit = st.selectbox("2. Tabela de Referência", sorted(table_opts.keys()), index=def_tbl_idx, key="audit_table_sel")
        
        if sel_table_audit:
            # 3. Columns
            cols = get_columns(CURRENT_BASE_URL, doc_id_audit, sel_table_audit)
            col_opts = {c['id']: c['fields']['label'] for c in cols}
            col_map_rev = {v: k for k, v in col_opts.items()}
            sorted_col_labels = sorted(col_opts.values())

            # Load defaults
            def_title_idx = None
            def_emails = []
            if sel_config_name != "(Nova Configuração)":
                c = saved_configs[sel_config_name]
                s_title_id = c.get("title_col")
                s_email_ids = c.get("email_cols", [])
                
                # Title index
                if s_title_id and s_title_id in col_opts:
                    lbl = col_opts[s_title_id]
                    if lbl in sorted_col_labels:
                        def_title_idx = sorted_col_labels.index(lbl)
                
                # Email multiselect
                for eid in s_email_ids:
                    if eid in col_opts:
                        def_emails.append(col_opts[eid])

            c1, c2 = st.columns(2)
            sel_title_label = c1.selectbox("3. Coluna de Título (ex: Empresa)", sorted_col_labels, index=def_title_idx, key="audit_col_title")
            sel_email_labels = c2.multiselect("4. Colunas de E-mail (Avaliadores etc)", sorted_col_labels, default=def_emails, key="audit_col_emails")
            
            # --- REFERENCE RESOLUTION LOGIC ---
            col_types = {c['id']: c['fields']['type'] for c in cols} # ID -> Type (e.g., 'Ref:Users')
            
            ref_configs = {} # Store resolution config: {SourceColID: {'target_table': Tbl, 'target_col': ColID}}
            
            if sel_email_labels:
                for label in sel_email_labels:
                    cid = col_map_rev.get(label)
                    ctype = col_types.get(cid, "")
                    if ctype.startswith("Ref:"):
                        ref_table = ctype.split(":")[1]
                        st.info(f"🔗 Coluna '{label}' é uma referência para a tabela '{ref_table}'.")
                        
                        # Fetch cols of target table to let user pick the email field
                        # We use a unique key for the selectbox based on label
                        ref_cols_raw = get_columns(CURRENT_BASE_URL, doc_id_audit, ref_table)
                        ref_col_opts = {rc['id']: rc['fields']['label'] for rc in ref_cols_raw}
                        
                        # Try to guess 'Email'
                        def_ref_idx = None
                        sorted_rc = sorted(ref_col_opts.values())
                        for i, rcl in enumerate(sorted_rc):
                            if "email" in rcl.lower():
                                def_ref_idx = i
                                break
                        
                        target_col_label = st.selectbox(f"Selecione a coluna de E-mail em '{ref_table}' para '{label}':", 
                                                      sorted_rc, 
                                                      index=def_ref_idx,
                                                      key=f"ref_res_{cid}")
                        
                        if target_col_label:
                            # Reverse lookup target col ID
                            target_col_id = [k for k,v in ref_col_opts.items() if v == target_col_label][0]
                            ref_configs[cid] = {
                                "target_table": ref_table,
                                "target_col": target_col_id
                            }

            # --- MANUAL REF CONFIG (For 'Any' types or undetected refs) ---
            with st.expander("⚙️ Configurar Referências Manuais (Se houver IDs numéricos)"):
                st.caption("Use isso se suas colunas mostram números (IDs) mas o sistema não detectou automaticamente (ex: Tipo 'Any').")
                
                # Dropdown to pick one of the selected email columns
                if sel_email_labels:
                    m_col_label = st.selectbox("Coluna para configurar:", ["(Selecione)"] + sel_email_labels, key="man_ref_col_sel")
                    if m_col_label != "(Selecione)":
                        m_cid = col_map_rev[m_col_label]
                        
                        # Fetch all tables to pick target
                        all_tables = get_tables(CURRENT_BASE_URL, doc_id_audit)
                        # tables have 'id'
                        # sort by id
                        all_tbl_ids = sorted([t['id'] for t in all_tables])
                        
                        m_target_table = st.selectbox("Tabela de Origem (que contém o email):", all_tbl_ids, key="man_ref_tbl_sel")
                        
                        if m_target_table:
                             # Fetch cols
                             m_ref_cols = get_columns(CURRENT_BASE_URL, doc_id_audit, m_target_table)
                             m_ref_opts = {rc['id']: rc['fields']['label'] for rc in m_ref_cols}
                             m_sorted_rc = sorted(m_ref_opts.values())
                             
                             m_target_col_label = st.selectbox("Coluna de Email na tabela origem:", m_sorted_rc, key="man_ref_target_col")
                             
                             if m_target_col_label:
                                 # Save to ref_configs
                                 m_target_col_id = [k for k,v in m_ref_opts.items() if v == m_target_col_label][0]
                                 
                                 # Overwrite/Set
                                 ref_configs[m_cid] = {
                                     "target_table": m_target_table,
                                     "target_col": m_target_col_id
                                 }
                                 st.success(f"Configurado: '{m_col_label}' -> '{m_target_table}.{m_target_col_label}'")

            # Save Config Section
            with st.expander("💾 Salvar esta configuração"):
                new_cfg_name = st.text_input("Nome da Configuração", value=sel_doc_audit if sel_config_name == "(Nova Configuração)" else sel_config_name)
                if st.button("Salvar Preset"):
                    if sel_title_label and sel_email_labels:
                         data = {
                             "doc_id": doc_id_audit,
                             "table_id": sel_table_audit,
                             "title_col": col_map_rev[sel_title_label],
                             "email_cols": [col_map_rev[l] for l in sel_email_labels],
                             # Save ref configs too if simple enough? For now simpler configs.
                             # Complex ref config persistence omitted for brevity, user re-selects or we improve later.
                         }
                         save_audit_config(new_cfg_name, data)
                         st.success("Salvo!")
                         time.sleep(1); st.rerun()

            if st.button("🔎 Executar Auditoria", type="primary"):
                if not sel_title_label or not sel_email_labels:
                    st.error("Selecione a coluna de título e pelo menos uma coluna de e-mail.")
                else:
                    with st.spinner("Cruzando dados..."):
                        title_col_id = col_map_rev[sel_title_label]
                        email_col_ids = [col_map_rev[l] for l in sel_email_labels]
                        
                        # Map label to ID for easy lookup
                        col_label_map = {col_map_rev[l]: l for l in sel_email_labels}
                        
                        # --- PRE-FETCH REFERENCE LOOKUPS ---
                        # Map: SourceColID -> { RefRowID -> ResolvedValue }
                        ref_lookups = {}
                        
                        for src_cid, cfg in ref_configs.items():
                            try:
                                t_recs = fetch_table_records(CURRENT_BASE_URL, doc_id_audit, cfg['target_table'])
                                lookup = {}
                                tgt_cid = cfg['target_col']
                                for r in t_recs:
                                    # Grist records have 'id'
                                    rid = r['id']
                                    val = r['fields'].get(tgt_cid)
                                    if val:
                                        lookup[rid] = str(val).strip().lower() # Normalize email
                                ref_lookups[src_cid] = lookup
                            except Exception as e:
                                st.error(f"Erro ao resolver referência para {cfg['target_table']}: {e}")

                        # A. Get Actual Explicit Access
                        doc_users = get_doc_users(CURRENT_BASE_URL, doc_id_audit)
                        actual_access_map = {} 
                        for u in doc_users:
                            if u.get('email') and u.get('access'):
                                actual_access_map[u['email'].strip().lower()] = u.get('access')
                        
                        # B. Get Reference Data
                        records = fetch_table_records(CURRENT_BASE_URL, doc_id_audit, sel_table_audit)
                        
                        # --- DEBUG SECTION ---
                        with st.expander("🕵️ Debug Dados (Resumido)"):
                            # Filter map for selected only
                            debug_map = {k: v for k, v in col_map_rev.items() if k == sel_title_label or k in sel_email_labels}
                            st.write("IDs das Colunas Selecionadas:", debug_map)
                            
                            # Show Types
                            types_debug = {label: col_types.get(col_map_rev.get(label), "N/A") for label in [sel_title_label] + sel_email_labels}
                            st.write("Tipos de Dados (Metadata):", types_debug)
                            
                            st.write("Configurações de Referência:", ref_configs)
                            
                            if records:
                                first_rec = records[0]['fields']
                                # Extract only relevant keys
                                filtered_rec = {}
                                for label in [sel_title_label] + sel_email_labels:
                                    cid = col_map_rev.get(label)
                                    if cid:
                                        filtered_rec[f"{label} ({cid})"] = first_rec.get(cid, "NÃO ENCONTRADO / VAZIO")
                                st.write("Dados do Primeiro Registro (Colunas Alvo):", filtered_rec)
                            else:
                                st.write("Nenhum registro encontrado na tabela.")
                        # ---------------------

                        # C. Build Matrix
                        table_data = []
                        matched_emails = set()
                        
                        # 1. Process Reference Table Rows
                        for r in records:
                            row_obj = {}
                            
                            # Title
                            row_title = r['fields'].get(title_col_id)
                            # Handle cases where title is list or complex object
                            if isinstance(row_title, list): row_title = str(row_title)
                            row_obj[sel_title_label] = row_title or ""
                            
                            # Process each selected Email Column
                            row_has_missing = False
                            missing_emails_in_row = [] # Store raw emails for fixing
                            
                            for col_id in email_col_ids:
                                col_label = col_label_map[col_id]
                                val = r['fields'].get(col_id)
                                
                                cell_display = []
                                
                                # Resolve Reference Value if needed
                                if col_id in ref_lookups and isinstance(val, int):
                                    # It's a single ref
                                    resolved = ref_lookups[col_id].get(val)
                                    val = resolved # Swap ID for Email String
                                elif col_id in ref_lookups and isinstance(val, list):
                                    # List of Refs? ['L', 1, 2]
                                    # Not common for single email cols, but possible
                                    pass # Logic below handles lists, but we need to resolve items.
                                    # Complex case, assuming single ref for now based on '56'.
                                
                                if val:
                                    # Extract emails
                                    cell_emails = []
                                    if isinstance(val, list):
                                        items = val[1:] if (len(val) > 0 and val[0] == 'L') else val
                                        for item in items:
                                            # If item is int and we have lookup
                                            if isinstance(item, int) and col_id in ref_lookups:
                                                r_val = ref_lookups[col_id].get(item)
                                                if r_val: cell_emails.append(r_val)
                                            elif isinstance(item, str): 
                                                cell_emails.append(item.strip().lower())
                                    elif isinstance(val, str):
                                        parts = val.split(",")
                                        for p in parts: cell_emails.append(p.strip().lower())
                                    elif isinstance(val, int) and col_id in ref_lookups:
                                         # Already resolved above? check double logic
                                         # Logic above 'val = resolved' handled strict replacement
                                         # If resolved is str, it goes to elif isinstance(val, str)
                                         # If logic above failed (lookup miss), it stays int
                                         pass
                                    
                                    # Check status
                                    for em in cell_emails:
                                        if em in actual_access_map:
                                            cell_display.append(f"✅ {em}")
                                            matched_emails.add(em)
                                        else:
                                            cell_display.append(f"🔴 {em}")
                                            missing_emails_in_row.append(em)
                                            row_has_missing = True
                                
                                # Join multiple emails in same cell with newlines or commas
                                row_obj[col_label] = "\n".join(cell_display)
                            
                            # Hidden metadata for actions
                            row_obj["_missing_emails"] = json.dumps(missing_emails_in_row)
                            row_obj["_orphan_email"] = None
                            row_obj["_type"] = "reference"
                            
                            table_data.append(row_obj)

                        # 2. Process Orphans (In Doc, Not in Table)
                        all_doc_emails = set(actual_access_map.keys())
                        orphans = sorted(list(all_doc_emails - matched_emails))
                        
                        first_email_col_label = sel_email_labels[0] # Put orphan in the first email col
                        
                        for orphan in orphans:
                            row_obj = {}
                            row_obj[sel_title_label] = "" # Blank Title
                            
                            # Fill first col with orphan info
                            row_obj[first_email_col_label] = f"☢️ {orphan}"
                            
                            # Fill other cols blank
                            for lbl in sel_email_labels[1:]:
                                row_obj[lbl] = ""
                            
                            # Metadata
                            row_obj["_missing_emails"] = "[]"
                            row_obj["_orphan_email"] = orphan
                            row_obj["_type"] = "orphan"
                            
                            table_data.append(row_obj)
                            
                        # D. Display
                        if not table_data:
                            st.info("Nenhum dado encontrado.")
                        else:
                            df_res = pd.DataFrame(table_data)
                            
                            st.caption("Selecione as linhas abaixo para aplicar correções em massa.")
                            
                            # Add selection column
                            df_res.insert(0, "Selecionar", False)
                            
                            # Configure columns (Hide metadata by excluding from order or implicit handling)
                            # We will use column_order to show only relevant columns
                            
                            # Reorder: Select, Title, Emails...
                            cols_visible = ["Selecionar", sel_title_label] + sel_email_labels
                            
                            edited_df = st.data_editor(
                                df_res,
                                column_order=cols_visible,
                                disabled=[sel_title_label] + sel_email_labels, # Disable content editing
                                use_container_width=True,
                                hide_index=True,
                                key="audit_editor"
                            )
                            
                            # E. Actions based on selection
                            selected_rows = edited_df[edited_df["Selecionar"]]
                            
                            if not selected_rows.empty:
                                st.divider()
                                c_act1, c_act2 = st.columns(2)
                                
                                # Gather Data
                                to_grant = []
                                to_revoke = []
                                
                                for _, row in selected_rows.iterrows():
                                    # Type 1: Missing (Grant)
                                    if row["_type"] == "reference":
                                        missing = json.loads(row["_missing_emails"])
                                        to_grant.extend(missing)
                                    
                                    # Type 2: Orphan (Revoke)
                                    if row["_type"] == "orphan":
                                        if row["_orphan_email"]:
                                            to_revoke.append(row["_orphan_email"])
                                
                                to_grant = list(set(to_grant))
                                to_revoke = list(set(to_revoke))
                                
                                with c_act1:
                                    if to_grant:
                                        st.write(f"🔴 **{len(to_grant)} usuários para CONCEDER acesso (Viewer):**")
                                        st.code("\n".join(to_grant))
                                        if st.button("✨ Conceder Acesso Selecionado", key="btn_audit_grant"):
                                            progress = st.progress(0)
                                            for i, em in enumerate(to_grant):
                                                update_doc_access(CURRENT_BASE_URL, doc_id_audit, em, "viewers")
                                                progress.progress((i+1)/len(to_grant))
                                            st.success("Acessos concedidos!")
                                            time.sleep(1); st.rerun()
                                    else:
                                        st.info("Nenhuma correção de acesso pendente na seleção.")

                                with c_act2:
                                    if to_revoke:
                                        st.write(f"☢️ **{len(to_revoke)} usuários para REMOVER acesso:**")
                                        st.code("\n".join(to_revoke))
                                        if st.button("🗑️ Remover Acesso Selecionado", type="primary", key="btn_audit_revoke"):
                                            progress = st.progress(0)
                                            for i, em in enumerate(to_revoke):
                                                update_doc_access(CURRENT_BASE_URL, doc_id_audit, em, None)
                                                progress.progress((i+1)/len(to_revoke))
                                            st.success("Acessos removidos!")
                                            time.sleep(1); st.rerun()
                                    else:
                                        st.info("Nenhum usuário órfão na seleção.")

# --- TAB 7: Template Cloner ---
with tab7:
    st.header("🏗️ Clonador de Templates (Estrutura de Tabelas)")
    st.markdown("Copie a estrutura (colunas e fórmulas) de uma tabela para outros documentos, sem copiar os dados.")

    if st.session_state.mapped_data is not None:
        all_docs_list = st.session_state.mapped_data[['Documento', 'Doc ID']].drop_duplicates()
        doc_opts_clone = {r['Documento']: r['Doc ID'] for _, r in all_docs_list.iterrows()}
    else:
        wss = get_workspaces_and_docs(CURRENT_BASE_URL, selected_org_id)
        doc_opts_clone = {}
        for ws in wss:
            for d in ws.get('docs', []):
                doc_opts_clone[d['name']] = d['id']
    
    st.subheader("1. Selecione a Origem")
    col_src1, col_src2 = st.columns(2)
    src_doc_name = col_src1.selectbox("Documento de Origem", sorted(doc_opts_clone.keys()), index=None, key="clone_src_doc")
    
    if src_doc_name:
        src_doc_id = doc_opts_clone[src_doc_name]
        src_tables = get_tables(CURRENT_BASE_URL, src_doc_id)
        src_table_ids = sorted([t['id'] for t in src_tables])
        src_table_id = col_src2.selectbox("Tabela de Origem", src_table_ids, index=None, key="clone_src_table")
        
        if src_table_id:
            # Fetch Schema
            with st.status("Lendo estrutura da tabela...", expanded=False):
                raw_cols = get_columns(CURRENT_BASE_URL, src_doc_id, src_table_id)
                
                # Filter out system columns and internal metadata
                # Grist API returns 'id' and 'fields'
                clean_cols = []
                for c in raw_cols:
                    f = c['fields']
                    # We only need key functional fields for cloning
                    clean_cols.append({
                        "id": c['id'],
                        "fields": {
                            "label": f.get("label"),
                            "type": f.get("type"),
                            "isFormula": f.get("isFormula", False),
                            "formula": f.get("formula", ""),
                            "widgetOptions": f.get("widgetOptions", ""),
                            "description": f.get("description", "")
                        }
                    })
                st.write(f"Encontradas {len(clean_cols)} colunas.")
                st.json(clean_cols)

            st.subheader("2. Selecione os Destinos")
            # Multiple selection for targets
            target_doc_names = st.multiselect("Documentos de Destino", sorted(doc_opts_clone.keys()), key="clone_targets")
            
            if target_doc_names:
                st.warning(f"⚠️ Isso criará a tabela '{src_table_id}' em {len(target_doc_names)} documentos. Se a tabela já existir, a criação da tabela falhará, mas tentaremos adicionar as colunas faltantes.")
                
                if st.button("🚀 Iniciar Clonagem em Massa", type="primary"):
                    progress = st.progress(0)
                    log_area = st.empty()
                    logs = []
                    
                    for i, t_name in enumerate(target_doc_names):
                        t_id = doc_opts_clone[t_name]
                        logs.append(f"--- Processando: {t_name} ---")
                        
                        # 1. Try to create Table WITH columns in one go
                        ok_t, msg_t = create_table(CURRENT_BASE_URL, t_id, src_table_id, clean_cols)
                        
                        if ok_t:
                            logs.append(f"✅ {msg_t}")
                        elif msg_t == "EXISTING":
                            logs.append(f"ℹ️ Tabela '{src_table_id}' já existe. Verificando colunas...")
                            # 2. Add Columns only (Grist will skip existing ones if we are lucky, 
                            # or we can try to be safe and just log it)
                            ok_c, msg_c = add_columns(CURRENT_BASE_URL, t_id, src_table_id, clean_cols)
                            logs.append(f"Colunas: {msg_c}")
                        else:
                            logs.append(f"❌ {msg_t}")
                        
                        progress.progress((i + 1) / len(target_doc_names))
                        log_area.code("\n".join(logs))
                    
                    st.success("Processo de clonagem concluído!")
    else:
        st.info("Selecione um documento de origem para começar.")



        