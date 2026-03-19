FROM python:3.9-alpine

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apk add --no-cache \
    ffmpeg \
    gcc \
    g++ \
    musl-dev \
    python3-dev

# Create necessary directories
RUN mkdir -p /app/audio_files /app/converted /app/logs

# Copy application code
COPY main.py .


# Expose the port
EXPOSE 40500

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=40500
ENV BASE_DIR=/app/devices

# Run the server
CMD ["python", "main.py"]