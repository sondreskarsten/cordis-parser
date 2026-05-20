FROM python:3.12-slim

RUN pip install --no-cache-dir \
    google-cloud-storage \
    pyarrow

COPY reader.py parser.py cdc.py entrypoint.py /app/

WORKDIR /app

CMD ["python", "entrypoint.py"]
