FROM python:3.11-slim

WORKDIR /app

COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/app /app/app
COPY frontend /frontend

ENV PORTAL_HOST=0.0.0.0
ENV PORTAL_PORT=8090
ENV PYTHONPATH=/app

EXPOSE 8090

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8090"]
