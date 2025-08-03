# Imagem base oficial do Python, leve
FROM python:3.10-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia os arquivos do projeto para dentro do container
COPY . /app

# Instala as dependências necessárias
RUN pip install --no-cache-dir requests

CMD ["python", "github_ingestor.py"]