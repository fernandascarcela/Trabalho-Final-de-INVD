import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import time

# --- CONFIGURAÇÃO DA CONEXÃO ---
# Use o nome do serviço do PostgreSQL no docker-compose como 'host'
DB_HOST = "postgres" 
DB_NAME = "seu_banco"
DB_USER = "seu_usuario"
DB_PASS = "sua_senha"
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# Tenta criar a conexão com o banco de dados
try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    st.error(f"Erro ao conectar ao banco de dados: {e}")
    st.stop()

# --- LAYOUT DO DASHBOARD ---
st.set_page_config(page_title="Tendências de Tecnologia", layout="wide")
st.title("📊 Dashboard de Monitoramento de Tendências de Tecnologia")

# Placeholder para o timestamp da última atualização
last_update_placeholder = st.empty()

# Botão para atualizar os dados manualmente
if st.button("Atualizar Dados"):
    st.success("Dados atualizados!")

def carregar_dados():
    """Função para carregar os dados do Data Warehouse."""
    try:
        # Pega as 15 tecnologias mais recentes e relevantes
        query = """
            SELECT tecnologia, fonte_dados, valor, timestamp_coleta
            FROM tendencias_tecnologia
            ORDER BY timestamp_coleta DESC, valor DESC
            LIMIT 15;
        """
        df = pd.read_sql(query, engine)
        last_update_placeholder.text(f"Última atualização: {pd.to_datetime('now').strftime('%H:%M:%S')}")
        return df
    except Exception as e:
        st.error(f"Não foi possível carregar os dados da tabela. Erro: {e}")
        return pd.DataFrame()

# Carrega os dados e exibe no dashboard
df_principal = carregar_dados()

if not df_principal.empty:
    st.header("Últimas Tendências Registradas")
    
    # Separa os dados por fonte para visualização
    df_so = df_principal[df_principal['fonte_dados'] == 'stackoverflow']
    df_gh = df_principal[df_principal['fonte_dados'] == 'github']
    
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Stack Overflow (Perguntas/hora)")
        if not df_so.empty:
            st.bar_chart(df_so.set_index('tecnologia')['valor'])
        else:
            st.warning("Sem dados do Stack Overflow no momento.")

    with col2:
        st.subheader("GitHub (Popularidade)")
        if not df_gh.empty:
            st.bar_chart(df_gh.set_index('tecnologia')['valor'])
        else:
            st.warning("Sem dados do GitHub no momento.")

    st.header("Dados Brutos da Última Carga")
    st.dataframe(df_principal)
else:
    st.warning("Ainda não há dados no Data Warehouse. Aguardando a execução do pipeline...")