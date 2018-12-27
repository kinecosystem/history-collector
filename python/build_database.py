#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Build a database structure for history-collector"""

import argparse
import csv
import os
import sys
import logging
import psycopg2
from psycopg2.extras import execute_values

from utils import verify_file_sequence

logging.basicConfig(level='INFO', format='%(asctime)s | %(levelname)s | %(message)s')

argparser = argparse.ArgumentParser(description='Create database structure for history-collector',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
argparser.add_argument('--host', default=os.getenv('POSTGRES_HOST', 'localhost'), help='postgres host')
argparser.add_argument('--superuser', default=os.getenv('POSTGRES_USER', 'postgres'), help='postgres superuser')
argparser.add_argument('--superpass', default=os.getenv('POSTGRES_PASSWORD', 'postgres'), help='postgres superuser password')
argparser.add_argument('--db', default=os.getenv('POSTGRES_DB', 'kin'), help='default database')
argparser.add_argument('--user', default=os.getenv('DB_USER', 'python'), help='database user')
argparser.add_argument('--password', default=os.getenv('DB_USER_PASSWORD', '1234'), help='user password')
argparser.add_argument('--first', default=os.getenv('FIRST_FILE', '0000003f'), help='first file')
argparser.add_argument('--accounts', default='known_accounts.csv', help='known accounts csv filename')
argparser.add_argument('--force', action='store_true', default=False, help='drop old db if exists!')
args = argparser.parse_args()

POSTGRES_HOST = args.host
POSTGRES_USER = args.superuser
POSTGRES_PASSWORD = args.superpass
DB_NAME = args.db
DB_USER = args.user
DB_USER_PASSWORD = args.password
FIRST_FILE = args.first
FORCE_NEW_DB = args.force
KNOWN_ACCOUNTS_FILE = args.accounts


if FIRST_FILE and not verify_file_sequence(FIRST_FILE):
    logging.error('Invalid first file')
    sys.exit(1)


def setup_postgres():
    """Set up a connection to the postgres database."""
    conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(POSTGRES_HOST, 'postgres',
                                                                         POSTGRES_USER, POSTGRES_PASSWORD)
    logging.info('postgres connection string: ' + conn_string)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cur = conn.cursor()

    # drop older database if needed
    cur.execute("SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('{}'));".format(DB_NAME))
    db_exists = bool(cur.fetchone()[0])
    if db_exists:
        if FORCE_NEW_DB:
            logging.warning('Dropping existing database and user!')
            cur.execute('SELECT pg_terminate_backend(pg_stat_activity.pid) '
                        'FROM pg_stat_activity WHERE pg_stat_activity.datname = \'{}\' '
                        'AND pid <> pg_backend_pid();'.format(DB_NAME))
            cur.execute('DROP DATABASE {};'.format(DB_NAME))
            cur.execute('DROP OWNED by {} CASCADE;'.format(DB_USER))
            cur.execute("DROP USER {};".format(DB_USER))
        else:
            logging.error('Database already exists, use --force flag to force dropping')
            exit(1)

    # create our database
    cur.execute('CREATE DATABASE {};'.format(DB_NAME))

    # create database user and give permissions
    cur.execute("CREATE USER {} WITH ENCRYPTED PASSWORD '{}';".format(DB_USER, DB_USER_PASSWORD))
    cur.execute('GRANT ALL ON DATABASE {} TO {};'.format(DB_NAME, DB_USER))

    # close current connection
    cur.close()
    conn.close()

    # reconnect to our new db
    conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(POSTGRES_HOST, DB_NAME, DB_USER, DB_USER_PASSWORD)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cur = conn.cursor()

    return cur


def main():
    """Main entry point."""

    try:
        cur = setup_postgres()

        # Create transactions table
        cur.execute('CREATE TABLE transactions('
                    'id BIGSERIAL PRIMARY KEY,'
                    'ledger_sequence int UNIQUE CHECK (ledger_sequence >= 0),'
                    'tx_hash varchar(64) NOT NULL,'
                    'tx_order smallint NOT NULL,'
                    'tx_status varchar(32) NOT NULL,'
                    'account varchar(64) NOT NULL,'
                    'account_sequence bigint NOT NULL,'
                    'operation_order int NOT NULL,'
                    'operation_type varchar(32) NOT NULL,'
                    'operation_status varchar(32) NOT NULL,'
                    'source varchar(64) NOT NULL,'
                    'destination varchar(64) NOT NULL,'
                    'amount numeric(21, 5) NOT NULL,'
                    'memo_text varchar(28),'
                    'is_signed_by_app boolean NOT NULL,'
                    'timestamp TIMESTAMP without time zone NOT NULL);')

        cur.execute('CREATE INDEX by_account ON transactions USING btree (account, account_sequence);')
        cur.execute('CREATE INDEX by_source ON transactions USING btree (source);')
        cur.execute('CREATE INDEX by_destination ON transactions USING btree (destination);')
        cur.execute('CREATE INDEX by_hash ON transactions USING btree (tx_hash);')
        cur.execute('CREATE INDEX by_op_type ON transactions USING btree (operation_type)')
        cur.execute('CREATE UNIQUE INDEX by_op_in_tx_in_ledger ON transactions '
                    'USING btree (ledger_sequence, tx_hash, operation_order);')

        # Create known_accounts table and fill it from csv
        cur.execute('CREATE TABLE known_accounts ('
                    'account varchar(64) PRIMARY KEY,'
                    'description TEXT NOT NULL,'
                    'network VARCHAR(64) NOT NULL);')

        accounts = []
        with open(KNOWN_ACCOUNTS_FILE, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row.
            for row in reader:
                accounts.append(row)
            execute_values(cur, 'INSERT INTO known_accounts VALUES %s', accounts, '(%s, %s, %s)')

        # Create and init last_state table
        cur.execute('CREATE TABLE last_state(file_sequence varchar(10) NOT NULL);')

        # Init last_state with first ledger to scan
        cur.execute("INSERT INTO last_state (file_sequence) VALUES(%s);", (FIRST_FILE,))

        logging.info('Database created successfully.')

    except Exception as e:
        logging.error(e)
        exit(1)


if __name__ == '__main__':
    main()
