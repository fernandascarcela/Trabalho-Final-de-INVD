# Usar uma imagem base do Python
FROM python:3.9-slim

# Definir o diretório de trabalho dentro do container
WORKDIR /app

# Copiar o arquivo de dependências e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o script do dashboard
COPY dashboard.py .

# Comando para executar o dashboard quando o container iniciar
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]