FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port (Hugging Face default is 7860)
EXPOSE 7860

# Ensure runtime directory exists
RUN mkdir -p runtime logs && chmod 777 runtime logs

# Run the collector
CMD ["python", "collector.py"]
