-- sqlite
-- params:
--   :clan_id
--   :week_start_utc  (예: 2026-02-04T00:00:00Z)
--   :week_end_utc    (예: 2026-02-11T00:00:00Z)
--   :limit

SELECT
  p.player_name,
  SUM(pm.kills) AS kills
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
  AND m.game_mode IN ('solo','duo','squad','solo-fpp','duo-fpp','squad-fpp')  -- (2) A안: 표준 6모드만
GROUP BY p.player_name
ORDER BY kills DESC, p.player_name ASC
LIMIT :limit;
