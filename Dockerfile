FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY orchestrator orchestrator
COPY mockmh mockmh
COPY bench bench
COPY tests tests
ENV PYTHONUNBUFFERED=1
