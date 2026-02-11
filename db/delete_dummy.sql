SELECT cm.account_id, p.player_name
FROM clan_members cm
LEFT JOIN players p
  ON p.platform=cm.platform AND p.account_id=cm.account_id
WHERE cm.clan_id='shaded_steam' AND cm.platform='steam'
ORDER BY p.player_name;
