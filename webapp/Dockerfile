FROM python:3.11-alpine

WORKDIR /app

# System deps
RUN apk add --no-cache curl

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
