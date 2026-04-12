FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем warehouse mock
COPY warehouse.py /app/services/warehouse.py
COPY services/__init__.py /app/services/__init__.py

COPY . .

CMD ["python", "consumer.py"]
