# W3Schools Database Implementation

This project implements the W3Schools sample database in three different database management systems: MySQL, SQLite, and PostgreSQL. It includes Python scripts to create tables and import data from CSV files.

## Project Structure

```
├── CSV Files/               # Source data files
│   ├── Categories.csv
│   ├── Customers.csv
│   ├── Employees.csv
│   ├── OrderDetails.csv
│   ├── Orders.csv
│   ├── Products.csv
│   ├── Shippers.csv
│   └── Suppliers.csv
│
├── Python Codes/           # MySQL implementation scripts
│   ├── programs_to_insert_data_categories_table.py
│   ├── programs_to_insert_data_customers_table.py
│   ├── programs_to_insert_data_employees_table.py
│   ├── programs_to_insert_data_order_details_table.py
│   ├── programs_to_insert_data_orders_table.py
│   ├── programs_to_insert_data_products_table.py
│   ├── programs_to_insert_data_shippers_table.py
│   └── programs_to_insert_data_suppliers_table.py
│
├── Python Codes For SQLite/  # SQLite implementation scripts
│   └── [Similar structure as MySQL scripts]
│
└── Python Codes For PostgreSQL/  # PostgreSQL implementation scripts
    └── [Similar structure as MySQL scripts]
```

## Prerequisites

### Required Python Packages

-   pandas: For reading CSV files
-   mysql-connector-python: For MySQL database operations
-   sqlite3: Built-in for SQLite operations
-   psycopg2-binary: For PostgreSQL database operations

Install required packages:

```bash
pip install pandas mysql-connector-python psycopg2-binary
```

### Database Setup

1. **MySQL**

    - Install MySQL Server
    - Create a database named 'w3schools'
    - Default connection parameters:
        - Host: localhost
        - User: root
        - Password: password
        - Database: w3schools

2. **SQLite**

    - No installation needed (built into Python)
    - Database file location: Specified in scripts

## W3Schools sample DB — multi-engine import scripts

This repository contains CSV source data and Python helper scripts to load the W3Schools sample dataset into three RDBMS engines: MySQL, SQLite and PostgreSQL.

The goal is a small, portable toolkit you can use to:

-   recreate the sample dataset locally for development or testing
-   compare behaviour across engines (types, constraints, SQL dialects)
-   learn how to wire CSV -> Python -> RDBMS import pipelines

Files created by these scripts are safe to run repeatedly: scripts create tables if they don't exist and handle duplicate rows where applicable.

## Project layout (top-level)

```
CSV Files/
    ├─ Categories.csv
    ├─ Customers.csv
    ├─ Employees.csv
    ├─ OrderDetails.csv
    ├─ Orders.csv
    ├─ Products.csv
    ├─ Shippers.csv
    └─ Suppliers.csv

Python Codes/                       # MySQL scripts (original)
Python Codes For SQLite/            # Equivalent scripts using sqlite3
Python Codes For PostgreSQL/       # Equivalent scripts using psycopg2
README.md                           # This file
```

All CSV files are expected to be in the `CSV Files/` folder adjacent to the scripts. If you move CSVs, update the relative paths in the scripts (they currently use `../CSV Files/...`).

## Quick setup

1. Install Python (3.8+ recommended).

2. Install required packages:

```bash
pip install -r requirements.txt
# or individually
pip install pandas mysql-connector-python psycopg2-binary
```

If you prefer to work in a virtualenv:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Note: `sqlite3` is part of the Python standard library; `psycopg2-binary` and `mysql-connector-python` are external packages.

## Databases & connection defaults used in scripts

-   MySQL: database name `w3schools`, host `localhost`, user `root`, password `password` (change these in the MySQL scripts to match your environment).
-   SQLite: scripts point to the SQLite file path `../db.sqlite3_bugsbyte` or a full path in some files — verify `programs_to_insert_data_*_sqlite.py` for the exact location.
-   PostgreSQL: database `w3schools`, host `localhost`, port `5432`, user `postgres`, password `password` — change to match your setup.

Always update credentials before running scripts on real systems.

## Recommended import order (preserves FK integrity)

1. Categories, Suppliers
2. Products (references Categories & Suppliers)
3. Employees, Customers
4. Shippers
5. Orders (references Customers, Employees, Shippers)
6. OrderDetails (references Orders, Products)

Run scripts for one engine at a time. Example for SQLite:

```bash
cd "Python Codes For SQLite"
python programs_to_insert_data_categories_table_sqlite.py
python programs_to_insert_data_suppliers_table_sqlite.py
python programs_to_insert_data_products_table_sqlite.py
python programs_to_insert_data_employees_table_sqlite.py
python programs_to_insert_data_customers_table_sqlite.py
python programs_to_insert_data_shippers_table_sqlite.py
python programs_to_insert_data_orders_table_sqlite.py
python programs_to_insert_data_order_details_table_sqlite.py
```

Replace folder and filenames for MySQL or PostgreSQL versions.

## Script behaviour and safety

-   Each script reads the corresponding CSV into a `pandas.DataFrame` and converts empty/NaN values to `NULL` equivalents.
-   Scripts include a `CREATE TABLE IF NOT EXISTS ...` block so they can be run repeatedly without failing on table creation.
-   Inserts use parameterized queries to avoid SQL injection and to let the DB handle type conversions.
-   Where primary key conflicts are expected, scripts either skip duplicate inserts or handle `IntegrityError` and continue. That makes re-running idempotent for most cases.

## Example verification queries

Use your DB client or the command line to run quick checks after import. Example SQLite checks:

```sql
-- count rows
SELECT COUNT(*) FROM categories;
SELECT COUNT(*) FROM products;

-- sample joined query
SELECT o.OrderID, c.CustomerName, p.ProductName, od.Quantity
FROM order_details od
JOIN orders o ON od.OrderID = o.OrderID
JOIN customers c ON o.CustomerID = c.CustomerID
JOIN products p ON od.ProductID = p.ProductID
LIMIT 20;
```

For PostgreSQL / MySQL, run the same SQL using `psql`/`mysql` clients.

## Common problems and fixes

-   "no such table" (SQLite) — you ran the insert script before the create-table step. Run the script that creates the table first or run the full pipeline in the recommended order.
-   `IntegrityError: UNIQUE constraint failed` — CSV contains duplicate PK values. Options:
    -   Remove duplicates from CSV
    -   Let the script skip duplicates (some scripts already use try/except)
-   Column name mismatches (KeyError in pandas) — the CSV header differs (e.g. `SupplierId` vs `SupplierID`). Update script column names or normalize CSV headers.
-   Date parsing / datatype mismatch — cast values in the script (int(), float(), or parse dates). Some scripts coerce types explicitly before insert.

## Development tips

-   If you change schema definitions in scripts, re-create the database or drop the affected tables before re-running imports to avoid type mismatches.
-   Keep the CSV files as source-of-truth. If you need to change a column header, update both CSV and scripts.
-   Add logging around imports if you want detailed progress output; currently scripts print a single success message per table.

## File list (quick reference)

-   `CSV Files/*.csv` — source data
-   `Python Codes/*.py` — original MySQL scripts
-   `Python Codes For SQLite/*.py` — sqlite3 scripts (look for `..._sqlite.py` suffix)
-   `Python Codes For PostgreSQL/*.py` — psycopg2 scripts (look for `..._postgresql.py` suffix)

## Reproducible run (example)

1. Create a Python virtual environment and install requirements (see above).
2. Ensure DB server is running (MySQL/Postgres) or verify SQLite path.
3. Run scripts in recommended order for your target engine.
4. Run verification queries.

## Contributing

1. Fork the repo.
2. Add or improve scripts/tests in a feature branch.
3. Open a PR with a clear description of changes.
