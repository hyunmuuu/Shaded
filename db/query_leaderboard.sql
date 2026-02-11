-- sqlite
SELECT
  p.player_name,
  k.kills_total,
  k.fetched_at
FROM clan_members m
JOIN players p
  ON p.platform = m.platform AND p.account_id = m.account_id
JOIN player_kills_current k
  ON k.platform = m.platform AND k.account_id = m.account_id
WHERE m.clan_id = 'shaded_steam'
  AND m.is_active = 1
  AND k.source = 'lifetime'
ORDER BY k.kills_total DESC, p.player_name ASC
LIMIT 50;
