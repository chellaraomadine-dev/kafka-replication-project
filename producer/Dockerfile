FROM python:3.11-slim

WORKDIR /app

# Install kafka-python
RUN pip install --no-cache-dir kafka-python==2.0.2

COPY producer.py .

ENTRYPOINT ["python", "producer.py"]
CMD ["--help"]
