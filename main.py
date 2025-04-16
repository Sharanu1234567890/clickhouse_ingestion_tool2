from flask import Flask, request, jsonify
import clickhouse_connect
import pandas as pd
import os

app = Flask(__name__)

# Connect to ClickHouse Database
def connect_to_clickhouse(host, port, user, jwt_token, database):
    try:
        client = clickhouse_connect.get_client(host=host, port=int(port), username=user, password=jwt_token, database=database)
        return client
    except Exception as e:
        return str(e)

# Fetch tables from ClickHouse
@app.route('/get_tables', methods=['POST'])
def get_tables():
    try:
        host = request.form['host']
        port = request.form['port']
        user = request.form['user']
        jwt_token = request.form['jwt_token']
        database = request.form['database']
        
        client = connect_to_clickhouse(host, port, user, jwt_token, database)
        if isinstance(client, str):
            return jsonify({"error": client}), 500
        tables = client.query("SHOW TABLES").result_rows
        return jsonify({"tables": [t[0] for t in tables]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Fetch columns for a specific ClickHouse table
@app.route('/get_columns', methods=['POST'])
def get_columns():
    try:
        host = request.form['host']
        port = request.form['port']
        user = request.form['user']
        jwt_token = request.form['jwt_token']
        database = request.form['database']
        table = request.form['table']
        
        client = connect_to_clickhouse(host, port, user, jwt_token, database)
        if isinstance(client, str):
            return jsonify({"error": client}), 500
        columns = client.query(f"DESCRIBE TABLE {table}").result_rows
        return jsonify({"columns": [c[0] for c in columns]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Export selected columns from ClickHouse to CSV
@app.route('/export_to_csv', methods=['POST'])
def export_to_csv():
    try:
        host = request.form['host']
        port = request.form['port']
        user = request.form['user']
        jwt_token = request.form['jwt_token']
        database = request.form['database']
        table = request.form['table']
        columns = request.form['columns'].split(',')
        
        client = connect_to_clickhouse(host, port, user, jwt_token, database)
        if isinstance(client, str):
            return jsonify({"error": client}), 500
        query = f"SELECT {', '.join(columns)} FROM {table}"
        result = client.query(query).result_rows
        df = pd.DataFrame(result, columns=columns)
        df.to_csv(f'{table}.csv', index=False)
        return jsonify({"message": "Export successful", "records": len(result)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Import CSV to ClickHouse
@app.route('/import_csv_to_clickhouse', methods=['POST'])
def import_csv_to_clickhouse():
    try:
        host = request.form['host']
        port = request.form['port']
        user = request.form['user']
        jwt_token = request.form['jwt_token']
        database = request.form['database']
        table = request.form['table']
        csv_file = request.files['csv_file']
        
        client = connect_to_clickhouse(host, port, user, jwt_token, database)
        if isinstance(client, str):
            return jsonify({"error": client}), 500

        # Reading the CSV file
        df = pd.read_csv(csv_file)
        columns = ', '.join(df.columns)
        values = ', '.join([f"({', '.join([repr(value) for value in row])})" for row in df.values.tolist()])

        query = f"INSERT INTO {table} ({columns}) VALUES {values}"
        client.command(query)
        return jsonify({"message": "Import successful", "records": len(df)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
