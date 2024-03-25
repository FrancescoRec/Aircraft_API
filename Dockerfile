FROM python:3.11-slim

RUN pip install poetry

WORKDIR /app

COPY ./ /app

RUN poetry install

WORKDIR /app/bdi_api

EXPOSE 8000

CMD [ "poetry", "run", "uvicorn", "bdi_api.app:app", "--host", "0.0.0.0"]

