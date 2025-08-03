import pandas as pd
from pytrends.request import TrendReq
from datetime import datetime
import os

# Palavras-chave a serem monitoradas
keywords = ["Python", "C++", "Linguagem C", "Java", "C#"]

# Parâmetros da coleta
timeframe = 'now 1-H'  # última hora de dados
output_folder = 'data_lake/raw/google_trends'

def fetch_trends_data(keywords, timeframe):
    print("Iniciando a conexão com o Google Trends...")
    pytrends = TrendReq(hl='pt-BR', tz=180)  # idioma e fuso horário

    try:
        print("Construindo o payload da requisição...")
        pytrends.build_payload(kw_list=keywords, cat=31, timeframe=timeframe)

        print("Buscando dados de interesse por hora...")
        trends_df = pytrends.interest_over_time()

        if trends_df.empty:
            print("Nenhum dado foi retornado pelo Google Trends.")
            return None

        if 'isPartial' in trends_df.columns:
            trends_df = trends_df.drop(columns=['isPartial'])

        # Adiciona coluna com o timestamp da coleta
        trends_df["collected_at"] = pd.Timestamp.now()

        return trends_df

    except Exception as e:
        print(f"Ocorreu um erro durante a coleta: {e}")
        return None

def save_with_timestamp(dataframe, base_folder):
    now = datetime.now()
    filename = f"trends_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    full_path = os.path.join(base_folder, filename)

    os.makedirs(base_folder, exist_ok=True)
    dataframe.to_csv(full_path, encoding='utf-8')
    print(f"Dados salvos em: {full_path}")

if __name__ == "__main__":
    print("=== Iniciando coleta única de dados do Google Trends ===")
    
    trends_data = fetch_trends_data(keywords, timeframe)

    if trends_data is not None:
        print(">> Coleta realizada com sucesso. Salvando os dados...")
        save_with_timestamp(trends_data, output_folder)
    else:
        print("Falha na coleta.")
    print("=== Coleta finalizada ===")