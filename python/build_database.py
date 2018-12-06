"""Script to build a database for main.py to write data to."""

import argparse
import os
import sys
import logging
import psycopg2

logging.basicConfig(level='INFO', format='%(asctime)s | %(levelname)s | %(message)s')

parser = argparse.ArgumentParser(description='Create database structure for history-collector')
parser.add_argument('--host', default=os.environ.get('POSTGRES_HOST'), help='host')
parser.add_argument('--password', default=os.environ.get('POSTGRES_PASSWORD'), help='password')
parser.add_argument('--db', default='kin', help='database')
parser.add_argument('--user', default='python', help='database user')
parser.add_argument('--userpass', default=os.environ.get('PYTHON_PASSWORD'), help='user password')
parser.add_argument('--first', default=os.environ.get('FIRST_FILE'), help='first file')
parser.add_argument('--force', action='store_true', default=False, help='drop old db if exists!')
args = parser.parse_args()

POSTGRES_HOST = args.host
POSTGRES_PASSWORD = args.password
DB_NAME = args.db
DB_USER = args.user
DB_USER_PASSWORD = args.userpass
FIRST_FILE = args.first
FORCE_NEW_DB = args.force

# Validate some arguments
if (int(FIRST_FILE,16) + 1) % 64 != 0:
    logging.error('First file selected is invalid')
    sys.exit(1)

def setup_postgres(database):
    """Set up a connection to the postgres database."""
    conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(POSTGRES_HOST, 'postgres', 'postgres', POSTGRES_PASSWORD)
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cur = conn.cursor()

    # drop if needed
    cur.execute("select exists(SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('{}'));".format(DB_NAME))
    db_exists = bool(cur.fetchone()[0])
    if db_exists:
        if FORCE_NEW_DB:
            logging.warning('Dropping existing database!')
            cur.execute('DROP DATABASE {};'.format(DB_NAME))
            cur.execute("DROP USER IF EXISTS {}".format(DB_USER))
        else:
            raise Exception('database already exists, use --force flag')

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
        cur = setup_postgres(DB_NAME)

        # Create tables
        cur.execute('CREATE TABLE payments('
                    'id SERIAL PRIMARY KEY,'
                    'account varchar(64) NOT NULL,'
                    'account_sequence bigint NOT NULL,'
                    'source varchar(64) not NULL,'
                    'destination varchar(64) not NULL,'
                    'amount FLOAT not NULL,'
                    'memo_text varchar(28),'
                    'fee INT not NULL,'
                    'fee_charged INT not NULL,'
                    'op_status text,'
                    'op_index INT not NULL,'
                    'tx_status text,'
                    'tx_hash varchar(64) not NULL,'
                    'ledger_sequence INT NOT NULL,'
                    'time TIMESTAMP without time zone not NULL);')

        cur.execute('CREATE INDEX by_account ON payments USING btree (account, account_sequence);')
        cur.execute('CREATE INDEX by_source ON payments USING btree (source);')
        cur.execute('CREATE INDEX by_destination ON payments USING btree (destination);')
        cur.execute('CREATE INDEX by_hash ON payments USING btree (tx_hash);')

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
                    'time TIMESTAMP not NULL);')
                    
        cur.execute('GRANT ALL ON creations TO python')
        '''

        '''https://github.com/stellar/horizon-importer/blob/master/db/structure.sql
        CREATE TABLE history_transactions (
            transaction_hash character varying(64) NOT NULL,
            ledger_sequence integer NOT NULL,
            application_order integer NOT NULL,
            account character varying(64) NOT NULL,
            account_sequence bigint NOT NULL,
            fee_paid integer NOT NULL,
            operation_count integer NOT NULL,
            created_at timestamp without time zone,
            updated_at timestamp without time zone,
            id bigint,
            tx_envelope text NOT NULL,
            tx_result text NOT NULL,
            tx_meta text NOT NULL,
            tx_fee_meta text NOT NULL,
            signatures character varying(96)[] DEFAULT '{}'::character varying[] NOT NULL,
            memo_type character varying DEFAULT 'none'::character varying NOT NULL,
            memo character varying,
            time_bounds int8range
        );
        '''

        cur.execute('CREATE TABLE lastfile(name varchar(8) not NULL);')

        # This is the name of the file that contains the first ledger to scan
        cur.execute("INSERT INTO lastfile VALUES(%s);", (FIRST_FILE,))

        cur.execute('GRANT ALL ON lastfile TO {}'.format(DB_USER))

        cur.execute('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {}'.format(DB_USER))

        logging.info('Database created successfully.')

    except Exception as e:
        logging.error(e)
        raise


if __name__ == '__main__':
    main()
