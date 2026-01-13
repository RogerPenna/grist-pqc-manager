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

# Fixed configuration for PQC-RS
ORG_DOMAIN = "qualcontabil" 
BASE_URL = f"https://{ORG_DOMAIN}.getgrist.com/api"

if not API_KEY:
    st.error("❌ GRIST_API_KEY não encontrada no arquivo .env")
    st.stop()

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest" # Required for write ops
}

# 2. API Helper Functions with Caching

@st.cache_data(ttl=300)
def get_orgs():
    """Fetches available organizations."""
    try:
        response = requests.get("https://docs.getgrist.com/api/orgs", headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Erro ao buscar organizações: {e}")
        return []

@st.cache_data(ttl=300)
def get_org_users(org_id_or_domain):
    """Fetches users at the organization level."""
    try:
        url = f"{BASE_URL}/orgs/{org_id_or_domain}/access"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return data.get("users", [])
    except Exception as e:
        st.error(f"Erro ao buscar usuários da organização: {e}")
        return []

@st.cache_data(ttl=300)
def get_workspaces_and_docs(org_id):
    """Fetches all workspaces and their documents for an org."""
    try:
        response = requests.get(f"{BASE_URL}/orgs/{org_id}/workspaces", headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Erro ao buscar workspaces: {e}")
        return []

@st.cache_data(ttl=600)
def get_doc_users(doc_id):
    """Fetches users assigned to a specific document."""
    try:
        response = requests.get(f"{BASE_URL}/docs/{doc_id}/access", headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return data.get("users", [])
    except Exception as e:
        print(f"Erro ao ler doc {doc_id}: {e}") 
        return []

def update_doc_access(doc_id, email, role):
    """Updates user access in a document via PATCH /access endpoint with delta."""
    try:
        clean_doc_id = doc_id.strip()
        url = f"{BASE_URL}/docs/{clean_doc_id}/access"
        
        payload = {
            "delta": {
                "users": {
                    email.strip(): role # Role can be 'viewers', 'editors', 'owners' or None (to remove)
                }
            }
        }
        
        response = requests.patch(url, headers=HEADERS, json=payload)
        
        if response.status_code != 200:
             return False, f"URL: {url} | Status {response.status_code}: {response.text}"
             
        return True, "Acesso atualizado com sucesso!"
    except Exception as e:
        return False, f"Erro de Script: {e}"

def refresh_app():
    """Clears cache and reruns the app."""
    st.cache_data.clear()
    time.sleep(1)
    st.rerun()

# 3. Main UI Layout

st.title("🏆 Gestor de Acessos PQC-RS (Grist)")

# Sidebar: Organization Selection
st.sidebar.header("Configuração")
orgs = get_orgs()

if not orgs:
    st.warning("Nenhuma organização encontrada.")
    st.stop()

org_map = {org['name']: org['id'] for org in orgs}

default_index = 0
for idx, name in enumerate(org_map.keys()):
    if "Qualidade Contábil" in name:
        default_index = idx
        break

selected_org_name = st.sidebar.selectbox("Selecione a Organização", list(org_map.keys()), index=default_index, key="sb_org_select_usab")
selected_org_id = org_map[selected_org_name]

st.sidebar.caption(f"API Endpoint: {BASE_URL}")

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Limpar Cache / Recarregar", key="sb_btn_reload_usab"):
    refresh_app()

# Main Content Tabs
tab1, tab2, tab3 = st.tabs(["👥 Visão Global (Org)", "🗺️ Mapeamento de Documentos", "⚡ Ações Rápidas"])

# --- TAB 1: Global Organization Users ---
with tab1:
    st.header(f"Usuários da Organização: {selected_org_name}")
    st.info("Esta lista mostra quem tem acesso ao Site da Equipe (nível superior).")
    
    # Filters with UNIQUE KEYS
    st.markdown("#### 🔍 Buscar na Organização")
    col_g1, col_g2 = st.columns(2)
    with col_g1:
        f_name = st.text_input("Filtrar por Nome", key="search_global_name_usab", placeholder="Ex: Rogério")
    with col_g2:
        f_email = st.text_input("Filtrar por Email", key="search_global_email_usab", placeholder="Ex: @gmail.com")

    # Fetch using domain (qualcontabil)
    users = get_org_users(ORG_DOMAIN)
    
    if users:
        df_org_users = pd.DataFrame(users)
        
        if 'email' not in df_org_users.columns: df_org_users['email'] = ''
        if 'name' not in df_org_users.columns: df_org_users['name'] = ''
        if 'access' not in df_org_users.columns: df_org_users['access'] = 'No Default Access'
        
        # Rename
        df_display = df_org_users.rename(columns={
            'email': 'Email', 
            'name': 'Nome', 
            'access': 'Acesso Global', 
            'id': 'ID Usuário'
        })
        
        # Reorder
        df_display = df_display[['Email', 'Nome', 'Acesso Global', 'ID Usuário']]
        
        # Apply Filters
        if f_name:
            df_display = df_display[df_display['Nome'].str.contains(f_name, case=False, na=False, regex=False)]
        if f_email:
            df_display = df_display[df_display['Email'].str.contains(f_email, case=False, na=False, regex=False)]
            
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        st.caption(f"Usuários encontrados: {len(df_display)}")
    else:
        st.warning("Nenhum usuário encontrado neste nível.")

# --- TAB 2: Document Mapping & Bulk Operations ---
with tab2:
    st.header("Mapeamento de Acessos por Documento")
    
    # Auto-init mapped data if cache exists but session state is empty? 
    # Streamlit reloads script, so session state persists unless cleared.
    if 'mapped_data' not in st.session_state:
        st.session_state.mapped_data = None

    # Logic to auto-load if we just refreshed from an operation?
    # Usually complicated, let's stick to manual load or check if we can persist.
    
    if st.button("🚀 Iniciar Mapeamento Completo", key="btn_start_map_usab"):
        workspaces = get_workspaces_and_docs(selected_org_id)
        
        if not workspaces:
            st.warning("Nenhum workspace encontrado.")
        else:
            all_docs = []
            for ws in workspaces:
                ws_name = ws.get('name', 'Sem Nome')
                for doc in ws.get('docs', []):
                    all_docs.append({
                        'doc_id': doc['id'],
                        'doc_name': doc['name'],
                        'workspace': ws_name
                    })
            
            total_docs = len(all_docs)
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            consolidated_data = []
            
            for i, doc in enumerate(all_docs):
                status_text.text(f"Lendo ({i+1}/{total_docs}): {doc['doc_name']}")
                progress_bar.progress((i + 1) / total_docs)
                
                doc_users = get_doc_users(doc['doc_id'])
                
                d_name = doc['doc_name'].strip()
                w_name = doc['workspace'].strip()
                
                if doc_users:
                    for user in doc_users:
                        access_level = user.get('access')
                        if not access_level:
                            access_level = f"{user.get('parentAccess')} (Herdado)" if user.get('parentAccess') else "Indefinido"
                        
                        consolidated_data.append({
                            'Selecionar': False,
                            'Documento': d_name, # First Column
                            'Email': (user.get('email') or "").strip(),
                            'Nome': (user.get('name') or "").strip(),
                            'Nível de Acesso': access_level.strip(),
                            'Workspace': w_name,
                            'Doc ID': doc['doc_id']
                        })
                else:
                    consolidated_data.append({
                        'Selecionar': False,
                        'Documento': d_name,
                        'Email': '-',
                        'Nome': '-',
                        'Nível de Acesso': 'Indefinido/Herdado',
                        'Workspace': w_name,
                        'Doc ID': doc['doc_id']
                    })
            
            status_text.text("Concluído!")
            progress_bar.empty()
            st.session_state.mapped_data = pd.DataFrame(consolidated_data)

    if st.session_state.mapped_data is not None:
        df_consolidated = st.session_state.mapped_data
        
        st.markdown("### 🔍 Filtros")
        hide_inherited = st.checkbox("Ocultar acessos herdados", value=True, key="chk_hide_inh_usab")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            filter_doc = st.text_input("Documento", key="search_mapping_doc_usab")
        with col2:
            filter_email = st.text_input("Email", key="search_mapping_email_usab")
        with col3:
            filter_name = st.text_input("Nome", key="search_mapping_name_usab")
        with col4:
            filter_access = st.text_input("Nível de Acesso", key="search_mapping_access_usab")

        df_filtered = df_consolidated.copy()
        
        if hide_inherited:
            df_filtered = df_filtered[~df_filtered['Nível de Acesso'].str.contains("Herdado|Indefinido", case=False, regex=True)]
            
        if filter_doc:
            df_filtered = df_filtered[df_filtered['Documento'].str.contains(filter_doc, case=False, na=False, regex=False)]
        if filter_email:
            df_filtered = df_filtered[df_filtered['Email'].str.contains(filter_email, case=False, na=False, regex=False)]
        if filter_name:
            df_filtered = df_filtered[df_filtered['Nome'].str.contains(filter_name, case=False, na=False, regex=False)]
        if filter_access:
            df_filtered = df_filtered[df_filtered['Nível de Acesso'].str.contains(filter_access, case=False, na=False, regex=False)]

        st.write(f"Registros encontrados: **{len(df_filtered)}**")

        def highlight_access(val):
            val_str = str(val).lower()
            if 'owner' in val_str:
                return 'background-color: #ffcccc; color: black'
            elif 'editor' in val_str:
                return 'background-color: #cce5ff; color: black'
            elif 'viewer' in val_str:
                return 'background-color: #e6ffcc; color: black'
            elif 'indefinido' in val_str or 'herdado' in val_str:
                return 'background-color: #f0f0f0; color: #666666'
            return ''

        edited_df = st.data_editor(
            df_filtered.style.map(highlight_access, subset=['Nível de Acesso']),
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Doc ID": None,
                "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False)
            },
            disabled=["Documento", "Email", "Nome", "Nível de Acesso", "Workspace"],
            key="data_editor_usab"
        )
        
        selected_rows = edited_df[edited_df['Selecionar'] == True]
        
        if not selected_rows.empty:
            st.divider()
            st.subheader("📦 Operações em Massa")
            st.info(f"**{len(selected_rows)}** usuários selecionados.")
            
            unique_docs = st.session_state.mapped_data[['Documento', 'Doc ID']].drop_duplicates()
            doc_options = {row['Documento']: row['Doc ID'] for _, row in unique_docs.iterrows()}
            doc_names_sorted = sorted(list(doc_options.keys()))
            
            # --- OPERATION 1 & 2: COPY / MOVE ---
            col_op1, col_op2 = st.columns(2)
            
            with col_op1:
                st.markdown("#### 📄 Copiar / Mover")
                target_doc_copy = st.selectbox(
                    "Documento de Destino", 
                    doc_names_sorted, 
                    index=None, 
                    placeholder="Pesquise o documento...",
                    key="dest_copy_sel_usab"
                )
                
                c1, c2 = st.columns(2)
                if c1.button("📄 Copiar", key="btn_copy_usab", disabled=not target_doc_copy):
                    target_id = doc_options[target_doc_copy]
                    
                    try:
                        resp = requests.get(f"{BASE_URL}/docs/{target_id}/access", headers=HEADERS)
                        target_users_data = resp.json().get('users', [])
                        existing_user_map = {u.get('email', '').strip().lower(): u.get('access') for u in target_users_data if u.get('email')}
                    except:
                        existing_user_map = {}

                    count_ok = 0
                    progress_bar = st.progress(0)
                    total = len(selected_rows)
                    
                    for idx, (index, row) in enumerate(selected_rows.iterrows()):
                        email = row['Email'].strip()
                        role_raw = row['Nível de Acesso']
                        
                        role = 'editors'
                        if 'owner' in str(role_raw).lower(): role = 'owners'
                        elif 'viewer' in str(role_raw).lower(): role = 'viewers'
                        
                        existing_access = existing_user_map.get(email.lower())
                        
                        # Promote if inherited/null, Skip if explicit
                        if not (email.lower() in existing_user_map and existing_access is not None):
                            success, msg = update_doc_access(target_id, email, role)
                            if success: count_ok += 1
                        
                        progress_bar.progress((idx + 1) / total)
                    
                    st.toast(f"✅ Cópia finalizada! {count_ok} processados.", icon="✅")
                    refresh_app()

                if c2.button("🚚 Mover (Cortar)", key="btn_move_usab", disabled=not target_doc_copy):
                    target_id = doc_options[target_doc_copy]
                    
                    try:
                        resp = requests.get(f"{BASE_URL}/docs/{target_id}/access", headers=HEADERS)
                        target_users_data = resp.json().get('users', [])
                        existing_user_map = {u.get('email', '').strip().lower(): u.get('access') for u in target_users_data if u.get('email')}
                    except:
                        existing_user_map = {}
                    
                    count_moved = 0
                    progress_bar = st.progress(0)
                    total = len(selected_rows)
                    
                    for idx, (index, row) in enumerate(selected_rows.iterrows()):
                        email = row['Email']
                        doc_origin_id = row['Doc ID']
                        role_raw = row['Nível de Acesso']
                        role = 'editors'
                        if 'owner' in str(role_raw).lower(): role = 'owners'
                        elif 'viewer' in str(role_raw).lower(): role = 'viewers'
                        
                        existing_access = existing_user_map.get(email.lower())
                        if not (email.lower() in existing_user_map and existing_access is not None):
                             update_doc_access(target_id, email, role)
                        
                        update_doc_access(doc_origin_id, email, None) # Remove from origin
                        count_moved += 1
                        progress_bar.progress((idx + 1) / total)
                        
                    st.toast(f"✅ Movimentação concluída: {count_moved}", icon="🚚")
                    refresh_app()

            with col_op2:
                st.markdown("#### 🛠️ Gestão / Remoção")
                
                # Update Level Section
                new_bulk_role = st.selectbox("Novo Nível para Selecionados", ["viewers", "editors", "owners"], key="sel_bulk_role_usab")
                if st.button("✏️ Atualizar Nível", key="btn_update_role_usab"):
                    progress_bar = st.progress(0)
                    total = len(selected_rows)
                    count_upd = 0
                    
                    for idx, (index, row) in enumerate(selected_rows.iterrows()):
                        email = row['Email']
                        doc_id = row['Doc ID']
                        success, _ = update_doc_access(doc_id, email, new_bulk_role)
                        if success: count_upd += 1
                        progress_bar.progress((idx + 1) / total)
                    
                    st.toast(f"✅ Nível atualizado para {count_upd} usuários!", icon="✏️")
                    refresh_app()

                st.divider()
                
                # Remove Section
                if st.button("🗑️ Remover Selecionados", key="btn_bulk_remove_usab", type="primary"):
                    progress_bar = st.progress(0)
                    total = len(selected_rows)
                    count_del = 0
                    
                    for idx, (index, row) in enumerate(selected_rows.iterrows()):
                        email = row['Email']
                        doc_id = row['Doc ID']
                        # Set to None to remove
                        success, _ = update_doc_access(doc_id, email, None)
                        if success: count_del += 1
                        progress_bar.progress((idx + 1) / total)
                    
                    st.toast(f"✅ Removidos {count_del} usuários!", icon="🗑️")
                    refresh_app()

            # Substitution (Full Width below)
            st.divider()
            if st.button("🔄 Ferramenta de Substituição", key="btn_repl_usab"):
                 @st.dialog("Substituir Usuários")
                 def replace_dialog():
                     st.write("Selecione quem entra no lugar dos selecionados.")
                     st.dataframe(selected_rows[['Nome', 'Email', 'Documento']], hide_index=True)
                     new_user_email = st.text_input("Email do Novo Usuário", key="dlg_new_email_usab")
                     new_user_role = st.selectbox("Permissão", ["viewers", "editors", "owners"], key="dlg_new_role_usab")
                     
                     if st.button("Confirmar Substituição", key="dlg_confirm_usab"):
                         for idx, (index, row) in enumerate(selected_rows.iterrows()):
                             old_email = row['Email']
                             doc_id = row['Doc ID']
                             update_doc_access(doc_id, old_email, None) # Remove Old
                             update_doc_access(doc_id, new_user_email, new_user_role) # Add New
                             
                         st.toast("✅ Substituição concluída!", icon="🔄")
                         refresh_app()
                 replace_dialog()

# --- TAB 3: Quick Actions ---
with tab3:
    st.header("⚡ Ações Rápidas")
    doc_options_quick = {}
    if st.session_state.mapped_data is not None:
         unique_docs = st.session_state.mapped_data[['Documento', 'Doc ID']].drop_duplicates()
         for _, row in unique_docs.iterrows():
             doc_options_quick[row['Documento']] = row['Doc ID']
    else:
        if st.button("Carregar Lista de Documentos", key="quick_btn_load_usab"):
             ws_data = get_workspaces_and_docs(selected_org_id)
             for ws in ws_data:
                 for doc in ws.get('docs', []):
                     doc_options_quick[doc['name']] = doc['id']
    
    if doc_options_quick:
        selected_doc_name = st.selectbox("Selecione o Documento", sorted(list(doc_options_quick.keys())), index=None, placeholder="Digite para buscar...", key="quick_sel_doc_usab")
        
        if selected_doc_name:
            selected_target_doc_id = doc_options_quick[selected_doc_name]
            
            st.divider()
            col_add, col_remove = st.columns(2)
            with col_add:
                st.subheader("🟢 Adicionar")
                q_add_email = st.text_input("Email", key="quick_add_email_usab")
                q_add_role = st.selectbox("Permissão", ["viewers", "editors", "owners"], key="quick_add_role_usab")
                if st.button("Executar Adição", key="quick_btn_add_usab"):
                    success, msg = update_doc_access(selected_target_doc_id, q_add_email, q_add_role)
                    if success:
                        st.toast(msg, icon="✅")
                        refresh_app()
                    else:
                        st.error(msg)
            with col_remove:
                st.subheader("🔴 Remover")
                q_rem_email = st.text_input("Email", key="quick_rem_email_usab")
                if st.button("Executar Remoção", key="quick_btn_rem_usab"):
                    success, msg = update_doc_access(selected_target_doc_id, q_rem_email, None)
                    if success:
                        st.toast(msg, icon="🗑️")
                        refresh_app()
                    else:
                        st.error(msg)
