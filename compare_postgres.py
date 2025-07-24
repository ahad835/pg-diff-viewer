import psycopg2
import csv
from getpass import getpass
import warnings
from tabulate import tabulate
import os

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
            # Only test connection here
            psycopg2.connect(
                host=host, port=port, dbname=dbname, user=user, password=password
            ).close()
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
            print(f"❌ Connection error: {e}")
        if attempt == retries - 1:
            raise ConnectionError(f"❌ Failed to connect to {role} DB after {retries} attempts.")


def connect_db(config):
    # Remove "schema" key before passing to psycopg2
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
            SELECT table_name FROM information_schema.tables
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


def generate_html_report(results, filepath):
    rows_html = ""
    for row in results:
        css_class = row["status"].replace(" ", "_")
        rows_html += f"""
        <tr class="{css_class}">
            <td>{row['table']}</td>
            <td>{row['source_count']}</td>
            <td>{row['target_count']}</td>
            <td>{row['status']}</td>
        </tr>"""

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>PostgreSQL Comparison Report</title>
        <style>
            body {{ font-family: Arial; padding: 20px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ border: 1px solid #ccc; padding: 8px; text-align: center; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .Match {{ background-color: #d4edda; }}
            .Row_Count_Mismatch {{ background-color: #fff3cd; }}
            .Missing_in_Source, .Missing_in_Target {{ background-color: #f8d7da; }}
        </style>
    </head>
    <body>
        <h2>📊 PostgreSQL Table Comparison Report</h2>
        <table>
            <thead>
                <tr>
                    <th>Table</th>
                    <th>Source Count</th>
                    <th>Target Count</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </body>
    </html>
    """
    with open(filepath, "w") as f:
        f.write(html)
    print(f"🌐 HTML report saved to: {filepath}")


def compare_all_tables(src_conn, tgt_conn, src_schema, tgt_schema, src_tables, tgt_tables, exclusions):
    results = []

    src_set = set(src_tables) - set(exclusions)
    tgt_set = set(tgt_tables) - set(exclusions)

    only_in_src = src_set - tgt_set
    only_in_tgt = tgt_set - src_set
    common = src_set & tgt_set

    print(f"\n🧾 Tables in Source Only: {sorted(only_in_src)}")
    print(f"🧾 Tables in Target Only: {sorted(only_in_tgt)}")
    print(f"🔁 Common Tables: {len(common)}")

    for table in sorted(only_in_src):
        count = get_row_count(src_conn, src_schema, table)
        results.append({
            "table": table,
            "source_count": count,
            "target_count": "MISSING",
            "status": "Missing in Target"
        })

    for table in sorted(only_in_tgt):
        count = get_row_count(tgt_conn, tgt_schema, table)
        results.append({
            "table": table,
            "source_count": "MISSING",
            "target_count": count,
            "status": "Missing in Source"
        })

    for table in sorted(common):
        print(f"\n🔍 Comparing table: {table}")
        src_count = get_row_count(src_conn, src_schema, table)
        tgt_count = get_row_count(tgt_conn, tgt_schema, table)
        status = "Match" if src_count == tgt_count else "Row Count Mismatch"
        print(f"🧮 {table}: Source={src_count}, Target={tgt_count} → {status}")
        results.append({
            "table": table,
            "source_count": src_count,
            "target_count": tgt_count,
            "status": status
        })

    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "comparison_results.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["table", "source_count", "target_count", "status"])
        writer.writeheader()
        writer.writerows(results)
    print(f"📁 CSV saved to: {csv_path}")

    html_path = os.path.join(base_dir, "report.html")
    generate_html_report(results, html_path)

    return results


def main():
    src_conn = tgt_conn = None
    try:
        src_config = get_connection_details("Source")
        tgt_config = get_connection_details("Target")

        print("\n🚫 Optional: Enter table names to exclude (comma-separated). If none, just press Enter.")
        exclusions_input = input("Tables to exclude (comma-separated): ")
        exclusions = [x.strip() for x in exclusions_input.split(",") if x.strip()]

        src_conn = connect_db(src_config)
        tgt_conn = connect_db(tgt_config)

        print("\n📋 Fetching table lists...")
        src_tables = get_tables(src_conn, src_config["schema"])
        tgt_tables = get_tables(tgt_conn, tgt_config["schema"])

        results = compare_all_tables(
            src_conn, tgt_conn,
            src_config["schema"], tgt_config["schema"],
            src_tables, tgt_tables,
            exclusions
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
