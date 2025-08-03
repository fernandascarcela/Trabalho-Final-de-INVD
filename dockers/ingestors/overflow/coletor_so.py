import requests
import time
from datetime import datetime, timedelta
import json
from kafka import KafkaProducer

# --- CONFIGURAÇÃO ---
LINGUAGENS = ["python", "c++", "c", "java", "c#"]
API_URL = "https://api.stackexchange.com/2.3/questions"
SUA_API_KEY = "rl_Muzn2EgWr8gKwGMRb6iF3D9Be" # Lembre-se de usar sua chave de API
KAFKA_TOPIC = "stackoverflow_topic"
# O endereço do Kafka será o nome do serviço no docker-compose
KAFKA_SERVER = "kafka:9092"

# --- PRODUTOR KAFKA ---
# Configura o produtor para enviar mensagens JSON
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def coletar_contagem_de_perguntas():
    """
    Coleta dados da API do Stack Overflow e os envia para um tópico do Kafka.
    """
    print(f"Iniciando coleta de dados às {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    agora = datetime.now()
    dez_minutos_atras = agora - timedelta(minutes=10) # Coleta a cada 10 minutos
    
    timestamp_final = int(agora.timestamp())
    timestamp_inicial = int(dez_minutos_atras.timestamp())

    resultados_coleta = {
        "timestamp_coleta_utc": agora.isoformat(),
        "contagens": {}
    }

    for linguagem in LINGUAGENS:
        params = {
            "site": "stackoverflow",
            "fromdate": timestamp_inicial,
            "todate": timestamp_final,
            "tagged": linguagem,
            "filter": "total",
            "key": SUA_API_KEY
        }
        try:
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            dados = response.json()
            total_perguntas = dados.get("total", 0)
            resultados_coleta["contagens"][linguagem] = total_perguntas
        except requests.exceptions.RequestException as e:
            print(f"Erro ao consultar API para '{linguagem}': {e}")
            resultados_coleta["contagens"][linguagem] = None
        time.sleep(1)

    # Envia o resultado completo para o Kafka
    producer.send(KAFKA_TOPIC, value=resultados_coleta)
    producer.flush() # Garante que a mensagem foi enviada
    
    print(f"Dados enviados ao tópico Kafka '{KAFKA_TOPIC}': {resultados_coleta}")
    
    return resultados_coleta

if __name__ == "__main__":
    coletar_contagem_de_perguntas()