# Use a lightweight Python base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy everything into /app
COPY . .

# Ensure the SentinelHub config ends up in the expected path
RUN mkdir -p /root/.config/sentinelhub \
 && cp Config/config.toml /root/.config/sentinelhub/config.toml

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the producer script
CMD ["python", "prod_img.py"]

