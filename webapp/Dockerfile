FROM python:3.11-slim

WORKDIR /app

# System deps (none needed beyond pip)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates/ templates/

ENV MONGO_URI=mongodb://mongo:27017 \
    MONGO_DB=boxuploader \
    DATA_DIR=/data/devices \
    PAGE_SIZE=20

EXPOSE 5000

# gunicorn for production; 4 sync workers is plenty for an internal dashboard
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "--timeout", "60", "app:app"]
