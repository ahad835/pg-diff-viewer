import psycopg2
import pandas as pd
from getpass import getpass
import warnings
import csv
from tabulate import tabulate

warnings.filterwarnings("ignore", category=UserWarning)


def get_connection_details(role, retries=3):
    print(f"\n🔐 Enter details for the {role} database:")
    host = input(f"{role} DB Host: ")
    port = input(f"{role} DB Port: ")
    dbname = input(f"{role} DB Name: ")
    user = input(f"{role} DB User: ")
    schema = input(f"{role} Schema Name: ")

    for attempt in range(retries):
        password = getpass(f"{role} DB Password (attempt {attempt + 1}/{retries}): ")
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password
            )
            conn.close()
            print(f"✅ Connected to {role} DB.")
            return {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": password,
                "schema": schema
            }
        except psycopg2.OperationalError as e:
            if "password authentication failed" in str(e):
                print("❌ Incorrect password.")
            else:
                print(f"❌ Failed to connect: {e}")

        if attempt == retries - 1:
            raise ConnectionError(f"❌ Failed to connect to {role} DB after {retries} attempts.")


def connect_db(config):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"]
    )


def get_tables(conn, schema):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
        """, (schema,))
        return [row[0] for row in cur.fetchall()]


def get_row_count(conn, schema, table):
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            return cur.fetchone()[0]
    except Exception as e:
        print(f"⚠️ Could not count rows in {schema}.{table}: {e}")
        return "ERROR"


def compare_all_tables(source_conn, target_conn, source_schema, target_schema, source_tables, target_tables):
    comparison_result = []

    source_set = set(source_tables)
    target_set = set(target_tables)

    source_only = source_set - target_set
    target_only = target_set - source_set
    common = source_set & target_set

    print(f"\n🧾 Tables in Source DB Only ({len(source_only)}): {sorted(source_only)}")
    print(f"🧾 Tables in Target DB Only ({len(target_only)}): {sorted(target_only)}")
    print(f"🔁 Common Tables to Compare: {len(common)}")

    # Tables only in source
    for table in sorted(source_only):
        source_count = get_row_count(source_conn, source_schema, table)
        comparison_result.append({
            "table": table,
            "source_count": source_count,
            "target_count": "MISSING",
            "counts_match": False
        })

    # Tables only in target
    for table in sorted(target_only):
        target_count = get_row_count(target_conn, target_schema, table)
        comparison_result.append({
            "table": table,
            "source_count": "MISSING",
            "target_count": target_count,
            "counts_match": False
        })

    # Common tables
    for table in sorted(common):
        print(f"\n🔍 Comparing table: {table}")
        source_count = get_row_count(source_conn, source_schema, table)
        target_count = get_row_count(target_conn, target_schema, table)
        counts_match = source_count == target_count
        if counts_match:
            print(f"✅ Row count match: {source_count}")
        else:
            print(f"❗ Row count mismatch: Source({source_count}) vs Target({target_count})")

        comparison_result.append({
            "table": table,
            "source_count": source_count,
            "target_count": target_count,
            "counts_match": counts_match
        })

    # Save to CSV
    csv_filename = "comparison_results.csv"
    with open(csv_filename, "w", newline='') as csvfile:
        fieldnames = ["table", "source_count", "target_count", "counts_match"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(comparison_result)

    print(f"\n📁 Full report written to: {csv_filename}")
    return comparison_result


def main():
    src_conn = tgt_conn = None

    try:
        src_config = get_connection_details("Source")
        tgt_config = get_connection_details("Target")

        src_conn = connect_db(src_config)
        tgt_conn = connect_db(tgt_config)

        print("\n📋 Fetching table lists...")
        source_tables = get_tables(src_conn, src_config["schema"])
        target_tables = get_tables(tgt_conn, tgt_config["schema"])

        results = compare_all_tables(
            src_conn, tgt_conn,
            src_config["schema"], tgt_config["schema"],
            source_tables, target_tables
        )

        print("\n📊 Final Summary:")
        print(tabulate(results, headers="keys", tablefmt="grid"))

    except Exception as e:
        print(f"🔥 Fatal error: {e}")

    finally:
        if src_conn:
            src_conn.close()
        if tgt_conn:
            tgt_conn.close()


if __name__ == "__main__":
    main()
