import sqlite3

conn = sqlite3.connect("db/housing.db")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS test (
    id INTEGER PRIMARY KEY,
    name TEXT
)
""")

cur.execute("INSERT INTO test (name) VALUES ('Keith')")

conn.commit()

for row in cur.execute("SELECT * FROM test"):
    print(row)

conn.close()