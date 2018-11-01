# History Collector Sample

Working demo of the history collector service.

**This is just a demo, not for use in production**

## Prerequisites
1. Install [docker](https://docs.docker.com/install/)
2. Install [docker-compose](https://docs.docker.com/compose/install/)

## Usage:
To run the service, simply clone the [docker-compose.yaml](https://github.com/kinecosystem/history-collector/raw/master/sample/docker-compose.yaml]) file
and run
```bash
$ sudo docker-compose up -d
````

We can now access the database using http requests.

### GET /payments?source=&limit=
```bash
curl "localhost:3000/payments?source=GDNM52OBYPX7TAOTFRPEED4DSOE6C7HSWFHCB5G45J2KDNZVUK335FVM&limit=3"
```
```json
{
  "destination": "GBWGUWBD5U55OTRBTS5SZJFGGIXTQQVAZJNC43GXZGKTGLPWRFT4VEBZ",
  "memo_text": "1-kit-peeb41ec5ef79404aa9ca3",
  "time": "2018-10-20",
  "amount": 21,
  "source": "GDNM52OBYPX7TAOTFRPEED4DSOE6C7HSWFHCB5G45J2KDNZVUK335FVM",
  "hash": "b8dd55089dcb0ed131c1a9d6c2bd0c0e6e7889db1b0d41e59bda9b018196c8ee"
}
{
  "destination": "GDDZMV3ZE3IUBLEA53WQCC7LGBREBJ7JBAGNFLYXDPRJQ4KNLB3XS5IQ",
  "memo_text": "1-kit-p49564228265448e68f633",
  "time": "2018-10-20",
  "amount": 13,
  "source": "GDNM52OBYPX7TAOTFRPEED4DSOE6C7HSWFHCB5G45J2KDNZVUK335FVM",
  "hash": "b96d0299a6b90f7c3ddebff5001bd844054997c2f7fcf40fb72677ef3de6294c"
}
{
  "destination": "GB7FGPEYS6EXWOSCSJ5YSR7LLHGLOKQFJSUHZ2MMDUNLPLBVCHRSQBXG",
  "memo_text": "1-kit-pbf9a00cef83e4c699c3ad",
  "time": "2018-10-20",
  "amount": 10,
  "source": "GDNM52OBYPX7TAOTFRPEED4DSOE6C7HSWFHCB5G45J2KDNZVUK335FVM",
  "hash": "ce8eec75fe0fa65bd260707cdef762b7202583bd6f438148c2c035a5dc41a7c1"
}

```

### GET /tx?id=
```bash
curl localhost:3000/tx?id=b8dd55089dcb0ed131c1a9d6c2bd0c0e6e7889db1b0d41e59bda9b018196c8ee
```
```json
{
  "destination": "GBWGUWBD5U55OTRBTS5SZJFGGIXTQQVAZJNC43GXZGKTGLPWRFT4VEBZ",
  "memo_text": "1-kit-peeb41ec5ef79404aa9ca3",
  "time": "2018-10-20",
  "amount": 21,
  "source": "GDNM52OBYPX7TAOTFRPEED4DSOE6C7HSWFHCB5G45J2KDNZVUK335FVM",
  "hash": "b8dd55089dcb0ed131c1a9d6c2bd0c0e6e7889db1b0d41e59bda9b018196c8ee"
}
```