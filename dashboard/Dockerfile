FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy app files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir streamlit pandas plotly kafka-python boto3

# Expose Streamlit default port
EXPOSE 8501

# Run Streamlit app
CMD ["streamlit", "run", "dashboard.py", "--server.headless=true", "--server.port=8501", "--server.enableCORS=false"]
