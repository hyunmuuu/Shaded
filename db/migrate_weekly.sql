-- sqlite
PRAGMA foreign_keys = ON;

-- 매치 메타(시간/모드/플랫폼)
CREATE TABLE IF NOT EXISTS matches (
  match_id        TEXT PRIMARY KEY,
  platform        TEXT NOT NULL,                 -- 'steam'
  created_at_utc  TEXT NOT NULL,                 -- 'YYYY-MM-DDTHH:MM:SSZ' (UTC)
  game_mode       TEXT,                          -- solo/duo/squad/solo-fpp/duo-fpp/squad-fpp 등
  is_ranked       INTEGER NOT NULL DEFAULT 0,     -- 1=ranked, 0=normal (알 수 없으면 0)
  inserted_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
);

-- 매치별 플레이어 킬(핵심)
CREATE TABLE IF NOT EXISTS player_matches (
  match_id        TEXT NOT NULL,
  platform        TEXT NOT NULL,
  account_id      TEXT NOT NULL,
  kills           INTEGER NOT NULL DEFAULT 0,
  inserted_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (match_id, platform, account_id),
  FOREIGN KEY (match_id) REFERENCES matches(match_id) ON DELETE CASCADE,
  FOREIGN KEY (platform, account_id) REFERENCES players(platform, account_id) ON DELETE CASCADE
);

-- 성능용 인덱스
CREATE INDEX IF NOT EXISTS idx_matches_time
ON matches (platform, created_at_utc);

CREATE INDEX IF NOT EXISTS idx_player_matches_player
ON player_matches (platform, account_id);
