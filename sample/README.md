# History Collector Sample

Working demo of the history collector service.

**This is just a demo, not for use in production**

## Prerequisites
1. Install [docker](https://docs.docker.com/install/)
2. Install [docker-compose](https://docs.docker.com/compose/install/)

## Usage

First, you will need to initialize a database structure for your application. Run the following to do it:
``` bash
$ docker run -it --rm --net=host kinecosystem/history-collector pipenv run python /opt/history-collector/build_database.py
```

Then, run the collector:
```bash
$ docker run -it --rm --net=host --name history-collector kinecosystem/history-collector
```

Or create a `docker-compose.yml` file similar to [the example](https://github.com/kinecosystem/history-collector/raw/master/sample/docker-compose.yaml]) 
and run your application:
```bash
$ sudo docker-compose up -d
````

This [example application](https://github.com/kinecosystem/history-collector/raw/master/sample/docker-compose.yaml]) 
creates a simple HTTP service and lets you access the history database using HTTP requests.

#### GET /payments?source=&limit=
```bash
curl "localhost:3000/payments?source=GDNM52OBYPX7TAOTFRPEED4DSOE6C7HSWFHCB5G45J2KDNZVUK335FVM&limit=3"
```

#### GET /tx?id=
```bash
curl localhost:3000/tx?id=b8dd55089dcb0ed131c1a9d6c2bd0c0e6e7889db1b0d41e59bda9b018196c8ee
```
