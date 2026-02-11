-- sqlite
PRAGMA foreign_keys = ON;

INSERT OR IGNORE INTO players (platform, account_id, player_name)
VALUES
('steam', 'accountid_1', 'PlayerOne'),
('steam', 'accountid_2', 'PlayerTwo');

INSERT OR IGNORE INTO clan_members (clan_id, platform, account_id, clan_role)
VALUES
('shaded_steam', 'steam', 'accountid_1', 'member'),
('shaded_steam', 'steam', 'accountid_2', 'member');

INSERT OR REPLACE INTO player_kills_current (platform, account_id, source, kills_total, breakdown, fetched_at)
VALUES
('steam', 'accountid_1', 'lifetime', 321, '{"solo":120,"duo":50,"squad":151}', datetime('now')),
('steam', 'accountid_2', 'lifetime', 210, '{"solo":60,"duo":40,"squad":110}', datetime('now'));
