FROM python:3.5-alpine

# Create the workdir
RUN mkdir -p /opt/history-sample

# Set the workdir
WORKDIR /opt/history-sample

# Copy the pipfiles
COPY Pipfile* ./

# Install dependencies
RUN pip install pipenv \
    &&  apk add -qU --no-cache -t .build-deps gcc musl-dev git postgresql-dev \
    &&  pipenv install

# Copy the code
COPY . .

EXPOSE 3000

CMD pipenv run python main.py
