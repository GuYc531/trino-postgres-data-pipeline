FROM python:3.10-slim


WORKDIR /

COPY src/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/data_pipeline.py data_pipeline.py
COPY src/utils.py utils.py
COPY sql/* sql/

CMD ["python", "data_pipeline.py"]
