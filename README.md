# Gestor de Acessos Grist PQC-RS

Este aplicativo Streamlit foi desenvolvido para gerenciar permissões de usuários no Grist para o **Prêmio da Qualidade Contábil (PQC-RS)**.

## Funcionalidades

- **Visão Global:** Lista todos os usuários do Team Site e seus níveis de acesso organizacional.
- **Mapeamento de Documentos:** Varredura completa de Workspaces e Documentos para identificar acessos individuais.
- **Operações em Massa:**
  - Copiar usuários entre documentos.
  - Mover (cortar) usuários.
  - Alterar nível de acesso (Viewer, Editor, Owner) em lote.
  - Remover acessos em massa.
  - Substituição rápida de usuários.
- **Auto-Refresh:** Atualização automática da tabela após operações bem-sucedidas.

## Configuração

1. Crie um arquivo `.env` na raiz do projeto.
2. Adicione sua chave de API do Grist:
   ```env
   GRIST_API_KEY=sua_chave_aqui
   ```
3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
4. Execute o app:
   ```bash
   streamlit run app_pqc.py
   ```
