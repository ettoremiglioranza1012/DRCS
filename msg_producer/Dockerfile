# Use a lightweight Python base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the entire imgproducer directory into the container
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the producer script
CMD ["python", "prod_msg.py"]

