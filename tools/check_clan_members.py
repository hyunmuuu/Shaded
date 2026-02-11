import sqlite3
from pathlib import Path

con = sqlite3.connect(Path("db/shaded.db"))
cnt = con.execute(
    "SELECT COUNT(*) FROM clan_members WHERE clan_id=? AND platform=?",
    ("shaded_steam", "steam"),
).fetchone()[0]

print("members_cnt =", cnt)

rows = con.execute(
    """
    SELECT cm.account_id, p.player_name
    FROM clan_members cm
    LEFT JOIN players p
      ON p.platform=cm.platform AND p.account_id=cm.account_id
    WHERE cm.clan_id=? AND cm.platform=?
    ORDER BY p.player_name
    LIMIT 20
    """,
    ("shaded_steam", "steam"),
).fetchall()

for r in rows:
    print(r)

con.close()
