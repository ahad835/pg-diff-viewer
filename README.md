# pg-diff-viewer

🔍 A powerful CLI tool to compare two PostgreSQL databases — table-by-table.  

Validate schema equality, row counts, and even full row-by-row data — perfect for database sync checks, migrations, and ETL verification.  
Future-ready for UI extensions.

---

## 📦 Features

- Connect to source & target PostgreSQL databases
- Auto-detect all common tables in given schemas
- Compare:
  - ✅ Table presence
  - 📊 Row counts
  - 🔍 Row-by-row data (with `pandas`)
- Clean result summary on terminal
- Secure password prompt
- Future-friendly architecture (UI integration coming)

---

## 🔧 Prerequisites

You'll need Python 3.7+ and the following packages installed:

```bash
pip install -r requirements.txt

To run : python compare_postgres.py
