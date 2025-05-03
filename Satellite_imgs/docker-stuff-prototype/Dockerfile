FROM python:3.13-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN chmod +x entrypoint.sh

# Installa le dipendenze di sistema minime
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Crea la directory di lavoro
WORKDIR /app

# Copia il codice
COPY . .

# Installa le dipendenze Python
RUN pip install --upgrade pip setuptools wheel \
    && pip install "ipython==9.2.0" \
    && pip install --no-cache-dir -r requirements.txt

# Il comando viene fornito da docker-compose.yml
