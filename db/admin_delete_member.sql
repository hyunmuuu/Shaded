-- sqlite
PRAGMA foreign_keys = ON;

DELETE FROM clan_members
WHERE clan_id = 'shaded_steam'
  AND platform = 'steam'
  AND account_id = 'PUT_ACCOUNT_ID_HERE';

