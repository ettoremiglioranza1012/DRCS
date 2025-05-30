FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy app files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir streamlit pandas plotly kafka-python psycopg2-binary

RUN pip install --no-cache-dir boto3 Pillow folium streamlit-folium streamlit-autorefresh plotly redis

# Expose Streamlit default port
EXPOSE 8501

# Run Streamlit app
CMD ["streamlit", "run", "dashboard.py", "--server.headless=true", "--server.port=8501", "--server.enableCORS=false"]
