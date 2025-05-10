# Use a lightweight Python base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the entire imgproducer_m5 directory into the container
COPY . .

# Set SentinelHub config path (if used internally by SHConfig)
ENV SENTINELHUB_CONFIG=imgproducer_m5/Config/config.toml

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the producer script
CMD ["python", "prod_m5_img.py"]

