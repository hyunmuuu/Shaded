import sqlite3
from pathlib import Path

DB_PATH = Path("db/shaded.db")

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    print("== clan_id groups ==")
    rows = cur.execute("""
      SELECT clan_id, platform, COUNT(*)
      FROM clan_members
      GROUP BY clan_id, platform
      ORDER BY clan_id, platform
    """).fetchall()
    for r in rows:
        print(r)

    print("\n== members by clan_id ==")
    rows = cur.execute("""
      SELECT cm.clan_id, p.player_name, cm.account_id
      FROM clan_members cm
      JOIN players p ON p.platform=cm.platform AND p.account_id=cm.account_id
      ORDER BY cm.clan_id, p.player_name
    """).fetchall()
    for r in rows:
        print(r)

    con.close()

if __name__ == "__main__":
    main()
