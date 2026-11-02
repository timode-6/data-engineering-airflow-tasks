FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rules.toml .
COPY src ./src

RUN useradd --uid 1000 --no-create-home app
USER app

ENTRYPOINT ["python", "-m", "log_alerts"]
CMD ["process"]
