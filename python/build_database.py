"""Script to build a database for main.py to write data to."""
#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import sys
import logging
import psycopg2

logging.basicConfig(level='INFO', format='%(asctime)s | %(levelname)s | %(message)s')

argparser = argparse.ArgumentParser(description='Create database structure for history-collector')
argparser.add_argument('--host', default=os.getenv('POSTGRES_HOST', 'localhost'), help='host')
argparser.add_argument('--password', default=os.getenv('POSTGRES_PASSWORD', 'postgres'), help='password')
argparser.add_argument('--db', default=os.getenv('DB_NAME', 'kin'), help='database')
argparser.add_argument('--user', default=os.getenv('DB_USER', 'python'), help='database user')
argparser.add_argument('--userpass', default=os.getenv('DB_USER_PASSWORD', '1234'), help='user password')
argparser.add_argument('--first', default=os.getenv('FIRST_FILE', '0000003f'), help='first file')
argparser.add_argument('--accounts', default='known_accounts.csv', help='known accounts csv filename')
argparser.add_argument('--force', action='store_true', default=False, help='drop old db if exists!')
args = argparser.parse_args()

POSTGRES_HOST = args.host
POSTGRES_PASSWORD = args.password
DB_NAME = args.db
DB_USER = args.user
DB_USER_PASSWORD = args.userpass
FIRST_FILE = args.first
FORCE_NEW_DB = args.force
KNOWN_ACCOUNTS_FILE = args.accounts

if (int(FIRST_FILE,16) + 1) % 64 != 0:
    logging.error('First file selected is invalid')
    sys.exit(1)


def setup_postgres():
    """Set up a connection to the postgres database."""
    conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(POSTGRES_HOST, 'postgres', 'postgres', POSTGRES_PASSWORD)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cur = conn.cursor()

    # drop older database if needed
    cur.execute("select exists(SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('{}'));".format(DB_NAME))
    db_exists = bool(cur.fetchone()[0])
    if db_exists:
        if FORCE_NEW_DB:
            logging.warning('Dropping existing database!')
            cur.execute('SELECT pg_terminate_backend(pg_stat_activity.pid) '
                        'FROM pg_stat_activity WHERE pg_stat_activity.datname = \'{}\' '
                        'AND pid <> pg_backend_pid();'.format(DB_NAME))
            cur.execute('DROP DATABASE {};'.format(DB_NAME))
            cur.execute("DROP USER IF EXISTS {}".format(DB_USER))
        else:
            logging.error('Database already exists, use --force flag to force dropping')
            exit(1)

    # create our database
    cur.execute('CREATE DATABASE {};'.format(DB_NAME))

    # create user
    cur.execute("CREATE USER {} WITH ENCRYPTED PASSWORD '{}';".format(DB_USER, DB_USER_PASSWORD))
    cur.execute("GRANT ALL ON DATABASE {} TO {};".format(DB_NAME, DB_USER))
    cur.close()
    conn.close()

    # reconnect to our db
    conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(POSTGRES_HOST, DB_NAME, 'postgres', POSTGRES_PASSWORD)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cur = conn.cursor()

    return cur


def verify_file_sequence():
    """Verifies that the file sequence is valid"""
    file_sequence = int(FIRST_FILE,16) + 1
    return file_sequence % 64


def main():
    """Main entry point."""

    try:
        cur = setup_postgres()

        # Create payments table
        cur.execute('CREATE TABLE payments('
                    'id SERIAL PRIMARY KEY,'
                    'tx_hash varchar(64) not NULL,'
                    'account varchar(64) NOT NULL,'
                    'account_sequence bigint NOT NULL,'
                    'source varchar(64) not NULL,'
                    'destination varchar(64) not NULL,'
                    'amount FLOAT not NULL,'
                    'memo_text varchar(28),'
                    'op_index INT not NULL,'
                    'ledger_sequence INT NOT NULL,'
                    'date TIMESTAMP without time zone not NULL);')

        cur.execute('CREATE INDEX by_account ON payments USING btree (account, account_sequence);')
        cur.execute('CREATE INDEX by_source ON payments USING btree (source);')
        cur.execute('CREATE INDEX by_destination ON payments USING btree (destination);')
        cur.execute('CREATE INDEX by_hash ON payments USING btree (tx_hash);')
        cur.execute('CREATE UNIQUE INDEX by_op_in_tx ON payments USING btree (op_index, tx_hash);')

        cur.execute('GRANT ALL ON payments TO {}'.format(DB_USER))


        '''
        cur.execute('CREATE TABLE creations('
                    'source varchar(56) not NULL,'
                    'destination varchar(56) not NULL,'
                    'starting_balance FLOAT not NULL,'
                    'memo_text varchar(28),'
                    'fee INT not NULL,'
                    'fee_charged INT not NULL,'
                    'operation_index INT not NULL,'
                    'tx_status text,'
                    'op_status text,'
                    'hash varchar(64) not NULL,'
                    'time TIMESTAMP WITHOUT TIME ZONE NOT NULL);')
                    
        cur.execute('GRANT ALL ON creations TO python')
        '''

        # Create known_accounts table and fill it from csv
        cur.execute('CREATE TABLE known_accounts ('
                    'account varchar(64) PRIMARY KEY,'
                    'description TEXT NOT NULL,'
                    'network VARCHAR(64) NOT NULL);')

        with open(KNOWN_ACCOUNTS_FILE, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO known_accounts VALUES (%s, %s, %s)", row)

        # Create and init lastfile table
        cur.execute('CREATE TABLE lastfile(name varchar(8) not NULL);')

        # This is the name of the file that contains the first ledger to scan
        cur.execute("INSERT INTO lastfile VALUES(%s);", (FIRST_FILE,))

        # setup permissions
        cur.execute('GRANT ALL ON lastfile TO {}'.format(DB_USER))

        cur.execute('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {}'.format(DB_USER))

        logging.info('Database created successfully.')

    except Exception as e:
        logging.error(e)
        raise


if __name__ == '__main__':
    main()
