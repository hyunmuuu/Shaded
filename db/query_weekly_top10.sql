-- sqlite
-- params:
--   :clan_id
--   :week_start_utc
--   :week_end_utc
--   :limit

SELECT
  p.player_name AS pubg_id,
  COALESCE(SUM(pm.kills), 0) AS kills
FROM clan_members cm
JOIN players p
  ON p.platform = cm.platform AND p.account_id = cm.account_id
JOIN player_matches pm
  ON pm.platform = cm.platform AND pm.account_id = cm.account_id
JOIN matches m
  ON m.match_id = pm.match_id AND m.platform = pm.platform
WHERE cm.clan_id = :clan_id
  AND m.created_at_utc >= :week_start_utc
  AND m.created_at_utc <  :week_end_utc
  AND m.game_mode IN ('solo','duo','squad','solo-fpp','duo-fpp','squad-fpp')
GROUP BY p.player_name
ORDER BY kills DESC, pubg_id ASC
LIMIT :limit;
