"""Script to build a database for main.py to write data to."""

import os
import logging

import psycopg2

# Get constants from env variables
PYTHON_PASSWORD = os.environ['PYTHON_PASSWORD']
GRAFANA_PASSWORD = os.environ['GRAFANA_PASSWORD']
ASSET_CODE = os.environ['ASSET_CODE']
FIRST_FILE = os.environ['FIRST_FILE']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']


def setup_postgres(database=''):
    """Set up a connection to the postgres database."""
    conn = psycopg2.connect("postgresql://postgres:{}@db:5432".format(POSTGRES_PASSWORD) + database)
    conn.autocommit = True
    cur = conn.cursor()
    return cur


def main():
    """Main entry point."""
    logging.basicConfig(level='INFO', format='%(asctime)s | %(levelname)s | %(message)s')
    # Check if the database already exists
    try:
        setup_postgres(database='/' + ASSET_CODE.lower())
    except psycopg2.OperationalError as e:
        if 'does not exist' in str(e):
            pass
        else:
            raise
    else:
        logging.info('Using existing database instead of creating a new one')
        quit(0)

    # Connect to the postgres database
    try:
        cur = setup_postgres()

        # Create the database
        cur.execute('CREATE DATABASE {};'.format(ASSET_CODE.lower()))

        # Create the users
        cur.execute('CREATE USER python;')
        cur.execute('CREATE USER grafanareader')
        cur.execute("ALTER USER python WITH PASSWORD '{}'".format(PYTHON_PASSWORD))
        cur.execute("ALTER USER grafanareader WITH PASSWORD '{}'".format(GRAFANA_PASSWORD))

        # Create the tables
        cur = setup_postgres('/' + ASSET_CODE.lower())
        cur.execute('CREATE TABLE payments(source varchar(255)'
                    ',destination varchar(255),amount FLOAT,memo_text varchar(255),hash varchar(255),time TIMESTAMP);')
        cur.execute('CREATE TABLE trustlines(source varchar(255),memo_text varchar(255), hash varchar(255),time TIMESTAMP);')
        cur.execute('CREATE TABLE lastfile(name varchar(255));')

        # This is the name of the file that contains the first ledger to scan
        cur.execute("INSERT INTO lastfile VALUES('{}');".format(FIRST_FILE))

        # Grant the users access to the database
        cur.execute('GRANT INSERT on payments TO python')
        cur.execute('GRANT INSERT on trustlines TO python')
        cur.execute('GRANT INSERT on lastfile TO python')
        cur.execute('GRANT SELECT on lastfile TO python')
        cur.execute('GRANT UPDATE on lastfile to python')
        cur.execute('GRANT SELECT on payments TO grafanareader')
        cur.execute('GRANT SELECT on trustlines TO grafanareader')

        logging.info('Database created successfully.')

    except:
        logging.error('Could not fully create database, please delete all data before retrying.')
        raise


if __name__ == '__main__':
    main()
