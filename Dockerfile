FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src ./src

# Set src as python path so imports work
ENV PYTHONPATH=/app/src

# Default command placeholder, will be overridden by docker-compose
CMD ["python", "--version"]
