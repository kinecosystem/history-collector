FROM python:3.5-alpine

# Create the workdir
RUN mkdir -p /opt/history-collector

# Set the workdir
WORKDIR /opt/history-collector

# Copy the pipfiles
COPY Pipfile* ./

# Install dependencies
RUN pip install pipenv \
    &&  apk add -qU --no-cache -t .build-deps gcc musl-dev git postgresql-dev \
    &&  pipenv install

# Copy the code
COPY . .

CMD pipenv run python build_database.py && pipenv run python main.py
