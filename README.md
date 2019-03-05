# History Collector

Scan the history archives of the kin blockchain, and create a database that contains payment and account creations.

## How does it work?
The python script constantly downloads files from the blockchain's s3 archive, parses them using [xdrparser](https://github.com/kinecosystem/xdrparser), filters payments and account creations and store these operations in a [postgres SQL](https://www.postgresql.org/) database.

## Notes
* Only payment and creation operations are saved to database.
* Operation indexes start counting from 0
* Saved attributes are: source, destination, memo **text**, tx hash, fee, fee_charged, tx_status, operation_status, and timestamp
* For payments, amount is also saved
* For creation, starting balance is also saved
* Amounts and starting balance are in stroops!
* The source account will be the source of the operation, if it exists.
* The service stores the last file scanned in the database, so you can restart the service without starting all over again

## Prerequisites
1. Install [docker](https://docs.docker.com/install/)
2. Install [docker-compose](https://docs.docker.com/compose/install/)

## Configuration
Edit the docker-compose file to configure it

|          Variable          | Description                                                                                                                                                                                                                     |
|:--------------------------:|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| POSTGRES_PASSWORD          | Master password for the postgres database                                                                                                                                                                                       |
| PYTHON_PASSWORD            | Password for the postgres user 'python' (will be created by the script)                                                                                                                                                                                                                                                                                                                                                                                       |
| KIN_ISSUER                 | Issuer of the kin asset                                                                                                                                                                                                  |
| FIRST_FILE                 | The first file to download If you know the ledger sequence|
| NETWORK_PASSPHRASE         | The passpharse/network id of the network                                                                                                                                                                                        |
| MAX_RETRIES                | Max number of tries to download a file before quitting, there is a 3 minute wait time between each try.                                                                                                                         |
| BUCKET_NAME                | S3 bucket name                                                                                                                                                                                                                  |
| CORE_DIRECTORY             | The path leading to transactions/ledger/results... folders, can be ''                                                                                                                                                      |
| POSTGRES_HOST            | The host of the postgres database                                                                                                                                                     |
| APP_ID             | An app id to filter transactions for. If left empty, all transactions will be saved regardless of app                                                                                                                                                      |
| LOG_LEVEL             | The level of logs to show, "INFO"/"ERROR"/"WARNING"                                                                                                                                                      |

## Usage:
To run the service, simply clone the [docker-compose.yaml](https://github.com/kinecosystem/history-collector/raw/master/docker-compose.yaml]) file, edit the configurations
and run
```bash
$ sudo docker-compose up -d
````

Logs can be accessed using
```bash
$ sudo docker-compose logs
```

## Demo:  
You can test this service with the demo app, in the ```sample``` folder
