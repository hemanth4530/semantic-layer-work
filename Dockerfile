FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY . /app

# Install dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

EXPOSE 8504

# Use entrypoint script
CMD ["/app/entrypoint.sh"]
