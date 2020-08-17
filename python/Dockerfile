FROM python:3.6-alpine

# Create the workdir
RUN mkdir -p /opt/history-collector

# Set the workdir
WORKDIR /opt/history-collector

# Installing pandas
RUN apk add --no-cache python3-dev libstdc++ && \
    apk add --no-cache g++ && \
    ln -s /usr/include/locale.h /usr/include/xlocale.h

# Copy the pipfiles
COPY Pipfile* ./

# Install dependencies
RUN pip install pipenv \
    &&  apk add -qU --no-cache -t .build-deps gcc musl-dev git postgresql-dev \
    &&  pipenv install

# Copy the code
COPY . .

CMD pipenv run python build_database.py && pipenv run python build_s3_storage.py && pipenv run python main.py
