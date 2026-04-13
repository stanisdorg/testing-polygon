FROM python:3.12-slim

WORKDIR /app

COPY services/student-portal/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/student-portal/ /app/

# Copy UI dashboard files
COPY ui-dashboard /app/ui-dashboard
COPY ui-tasks /app/ui-tasks

EXPOSE 8080

CMD ["uvicorn", "portal:app", "--host", "0.0.0.0", "--port", "8080"]
