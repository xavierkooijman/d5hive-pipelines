import pymysql
from utils.destinations_registry import register_destination
import clts_pcp as clts


@register_destination("mysql")
def insert_mysql(config, data):
    if not data:
        return

    conn_params = {
        "host": config["host"],
        "port": config["port"],
        "user": config["username"],
        "password": config["password"],
        "database": config["database"],
    }

    if "certificate" in config:
        conn_params["ssl_ca"] = config["certificate"]

    conn = pymysql.connect(**conn_params)

    cursor = conn.cursor()
    table = config["table"]

    columns = list(data[0].keys())
    column_names = ",".join(columns)
    placeholders = ",".join(["%s"] * len(columns))

    query = f"""
        INSERT INTO {table} ({column_names})
        VALUES ({placeholders})
    """

    values = [
        tuple(row.get(col) for col in columns)
        for row in data
    ]

    print(
        f"Inserting {len(values)} rows into MySQL table '{table}' at {config['host']}:{config['port']}")
    cursor.executemany(query, values)

    conn.commit()
    cursor.close()
    conn.close()
