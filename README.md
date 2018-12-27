# History Collector

Scan the history archives of the kin blockchain, and create a database that contains transaction data.

## How does it work?
The python script constantly downloads files from the blockchain's s3 archive, parses them using 
[xdrparser](https://github.com/kinecosystem/xdrparser), filters payments stores them in a 
[Postgres SQL](https://www.postgresql.org/) database.

## Notes
* Only KIN payments are saved to database.
* Transaction and Operation indices start from 0
* The source account will be the source of the operation, if exists.
* The service stores the last file scanned in the database and continues from there.
* Stored fields: **ledger_sequence, tx_hash, tx_order, tx_status, account, account_sequence, 
operation_order, operation_type, operation_status, source, destination, amount, memo_text,
is_signed_by_app, date**

## Prerequisites
1. Install [docker](https://docs.docker.com/install/)
2. Install [docker-compose](https://docs.docker.com/compose/install/)

## Configuration
Edit the docker-compose file to configure it

|          Variable          |   Default   |   Description          |                                                                                                                                              
|:---------------------------|:----------- |:-----------------------|
| POSTGRES_HOST              | localhost   | Postgres database host |       
| POSTGRES_USER              | postgres    | Postgres superuser name |                                        
| POSTGRES_PASSWORD          | postgres    | Postgres superuser password |   
| POSTGRES_DB                | kin         | Database name |
| DB_USER                    | python      | Database user |
| DB_USER_PASSWORD           | 1234        | Database user password |
| KIN_ISSUER                 | GDF42M3IPERQCBLWFEZKQRK77JQ65SCKTU3CW36HZVCX7XX5A5QXZIVK | The address of Kin issuer |
| NETWORK_PASSPHRASE         | Public Global Kin Ecosystem Network ; June 2018 | The passpharse/network id of the network |                                                                                                                                                                            
| MAX_RETRIES                | 5           | Max number of tries to download a file before quitting, there is a RETRY_DELAY seconds wait time between each try. | 
| RETRY_DELAY                | 180         | The number of seconds to wait until the next download attempt
| BUCKET_NAME                | stellar-core-ecosystem-6145 | S3 bucket name |                                                                                                               
| CORE_DIRECTORY             |             | The path leading to transactions/ledger/results... folders, can be '' |                                                                                                         
| APP_ID                     |             | An app id to filter transactions for. If left empty, all transactions will be saved regardless of app |                                                                                                                                                    |
| FIRST_FILE                 | 0000003f    | The first file to download (ledger sequence) |
| LOG_LEVEL                  | INFO        | Application log level (ERROR/WARNING/INFO/DEBUG) |                                                                                                                                

## Usage:
To run the service, simply clone the [docker-compose.yaml](https://github.com/kinecosystem/history-collector/raw/master/docker-compose.yaml]) 
file, edit the configurations and run
```bash
$ sudo docker-compose up -d
````

Logs can be accessed using
```bash
$ sudo docker-compose logs
```

## Demo:  
You can test this service with the demo app, in the ```sample``` folder.
