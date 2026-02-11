import sqlite3
from pathlib import Path

db = Path("db/shaded.db")
con = sqlite3.connect(db)
rows = con.execute("PRAGMA table_info(clan_members);").fetchall()
con.close()

for r in rows:
    # (cid, name, type, notnull, dflt_value, pk)
    print(r)
    