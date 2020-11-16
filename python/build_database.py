"""Script to build a database for main.py to write data to."""

import os
import sys
import logging

import psycopg2

# Get constants from env variables
PYTHON_PASSWORD = os.environ['PYTHON_PASSWORD']
FIRST_FILE = os.environ['FIRST_FILE']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_HOST = os.environ['POSTGRES_HOST']


def setup_postgres(database=''):
    """Set up a connection to the postgres database."""
    conn = psycopg2.connect("postgresql://postgres:{}@{}:5432".format(POSTGRES_PASSWORD, POSTGRES_HOST) + database)
    conn.autocommit = True
    cur = conn.cursor()
    return cur


def verify_file_sequence():
    """Verifies that the file sequence is valid"""
    file_sequence = int(FIRST_FILE,16) + 1
    return file_sequence % 64


def main():
    """Main entry point."""
    logging.basicConfig(level='INFO', format='%(asctime)s | %(levelname)s | %(message)s')
    # Check if the database already exists
    try:
        setup_postgres(database='/kin')
    except psycopg2.OperationalError as e:
        if 'does not exist' in str(e):
            pass
        else:
            raise
    else:
        logging.info('Using existing database instead of creating a new one')
        sys.exit(0)

    if verify_file_sequence() != 0:
        logging.error('First file selected is invalid')
        sys.exit(1)

    # Connect to the postgres database
    try:
        cur = setup_postgres()

        # Create the database
        cur.execute('CREATE DATABASE {};'.format('kin'))

        # Create the user
        cur.execute('CREATE USER python;')
        cur.execute("ALTER USER python WITH PASSWORD '{}'".format(PYTHON_PASSWORD))

        # Create the tables
        cur = setup_postgres('/kin')

        cur.execute('CREATE TABLE payments('
                    'source varchar(56) not NULL,'
                    'destination varchar(56) not NULL,'
                    'amount FLOAT not NULL,'
                    'memo_text varchar(28),'
                    'app_index INT,'
                    'fee INT not NULL,'
                    'fee_charged INT not NULL,'
                    'operation_index INT not NULL,'
                    'tx_status text,'
                    'op_status text,'
                    'hash varchar(64) not NULL,'
                    'time TIMESTAMP not NULL);')

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
                    'time TIMESTAMP not NULL);')

        cur.execute('CREATE TABLE lastfile('
                    'name varchar(8) not NULL);')

        # This is the name of the file that contains the first ledger to scan
        cur.execute("INSERT INTO lastfile VALUES(%s);", (FIRST_FILE,))

        # Grant the user access to the database
        cur.execute('GRANT INSERT on payments TO python')
        cur.execute('GRANT INSERT on creations TO python')
        cur.execute('GRANT SELECT on payments TO python')
        cur.execute('GRANT SELECT on creations TO python')
        cur.execute('GRANT INSERT on lastfile TO python')
        cur.execute('GRANT SELECT on lastfile TO python')
        cur.execute('GRANT UPDATE on lastfile to python')

        logging.info('Database created successfully.')

    except:
        logging.error('Could not fully create database, please delete all data before retrying.')
        raise


if __name__ == '__main__':
    main()
