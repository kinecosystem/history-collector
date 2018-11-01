"""Sample http api to get transaction history from the database"""

import json

from flask import Flask, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras

app = Flask(__name__)
CORS(app)

# Connect to the database
conn = psycopg2.connect('postgresql://python:1234@db:5432/kin')
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
conn.autocommit = True


@app.route('/payments', methods=['GET'])
def payments():
    """Get transactions sent from a single account"""
    data = request.args
    source = data['source']
    limit = data.get('limit', 20)
    cur.execute("SELECT * from payments where source=%s limit %s", (source, limit))
    results = cur.fetchall()

    response = ''
    for result in results:
        # time is returned as "datatime" object, convert to string for json
        result['time'] = result['time'].strftime("%Y-%m-%d")
        response += json.dumps(result, indent=2)

    return response, 200


@app.route('/tx', methods=['GET'])
def tx():
    """Get a specific transaction by its id"""
    data = request.args
    tx_id = data['id']
    cur.execute("SELECT * from payments where hash=%s", (tx_id,))
    result = cur.fetchone()

    result['time'] = result['time'].strftime("%Y-%m-%d")
    response = json.dumps(result, indent=2)

    return response, 200



if __name__ == '__main__':
    app.run('0.0.0.0', 3000)
