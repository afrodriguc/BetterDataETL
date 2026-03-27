FROM python:3.12-slim

WORKDIR /app

# Instalar el paquete shared primero (mejor cache de Docker)
COPY ../../shared /shared
RUN pip install --no-cache-dir /shared

# Instalar dependencias de la función
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código de la función
COPY . .

ENV PORT=8080
CMD ["functions-framework", "--target=manual_etl_loader", "--port=8080"]
