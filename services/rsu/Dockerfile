FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends mosquitto \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY services/rsu/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/rsu/mosquitto.conf /etc/mosquitto/mosquitto.conf
COPY services/rsu/rsu_core.py services/rsu/rsu_app.py ./
COPY schema/ schema/
EXPOSE 1883 8080
CMD ["uvicorn", "rsu_app:app", "--host", "0.0.0.0", "--port", "8080"]
