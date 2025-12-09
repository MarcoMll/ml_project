import sqlite3
import shutil

# --- INPUT / OUTPUT PATHS ---
SOURCE_DB = "viewer_interactions.db"         # original file
TARGET_DB = "viewer_interactions_small.db"   # new smaller DB

# 1. Read the original DB schema --------------------------------------

# Connect to source
src_conn = sqlite3.connect(SOURCE_DB)
src_cur = src_conn.cursor()

# Create new DB (overwrite if exists)
# Or you may delete manually before running
try:
    shutil.os.remove(TARGET_DB)
except FileNotFoundError:
    pass

tgt_conn = sqlite3.connect(TARGET_DB)
tgt_cur = tgt_conn.cursor()

# 2. Get all tables in the database ------------------------------------

src_cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND name NOT LIKE 'sqlite_%';
                """)

tables = [row[0] for row in src_cur.fetchall()]
print("Tables found:", tables)

# 3. Copy schema + up to 100 rows for each table ------------------------

for table in tables:
    print(f"\nProcessing table: {table}")

    # Get CREATE TABLE statement
    src_cur.execute(f"""
        SELECT sql 
        FROM sqlite_master 
        WHERE type='table' 
          AND name='{table}';
    """)
    create_sql = src_cur.fetchone()[0]

    # Create identical table in target DB
    tgt_cur.execute(create_sql)

    # Fetch up to 100 rows
    src_cur.execute(f"SELECT * FROM {table} LIMIT 100;")
    rows = src_cur.fetchall()

    if rows:
        # Prepare insertion placeholder
        placeholders = ", ".join(["?"] * len(rows[0]))
        insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"
        tgt_cur.executemany(insert_sql, rows)

    print(f"  → Copied {len(rows)} rows")

# Save and close --------------------------------------------------------

tgt_conn.commit()
src_conn.close()
tgt_conn.close()

print("\n✔ DONE! New smaller DB created as:", TARGET_DB)