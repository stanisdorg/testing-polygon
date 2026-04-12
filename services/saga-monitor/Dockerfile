FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared utilities (local copy)
COPY shared /app/services/shared
RUN touch /app/services/__init__.py /app/services/shared/__init__.py

COPY . .

CMD ["python", "monitor.py"]
