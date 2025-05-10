FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Copy everything inside setup_orchestrator/ into /app
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Start orchestrator
CMD ["python", "orchestrator.py"]

