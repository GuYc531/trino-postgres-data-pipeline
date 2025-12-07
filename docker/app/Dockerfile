FROM python:3.7-slim

WORKDIR /app

COPY src/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/data_pipeline.py data_pipeline.py

CMD ["python", "data_pipeline.py"]
