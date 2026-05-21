FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements_v5.txt .
RUN pip install --no-cache-dir -r requirements_v5.txt

# Copy bot files
COPY *.py ./

# Create data/logs dirs (volume mount will overlay /data at runtime)
RUN mkdir -p /data /logs

CMD ["uvicorn", "volsurge_v5_live:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
