FROM python:3.5-alpine

RUN mkdir -p /opt/stellar-dashboard

COPY . /opt/stellar-dashboard

WORKDIR /opt/stellar-dashboard

RUN pip install pipenv \
    &&  apk add -qU --no-cache -t .build-deps gcc musl-dev git postgresql-dev \
    &&  pipenv install

CMD pipenv run python build_database.py && pipenv run python main.py
