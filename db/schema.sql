-- sqlite
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS clans (
  clan_id     TEXT PRIMARY KEY,
  platform    TEXT NOT NULL,          -- steam 고정
  clan_name   TEXT NOT NULL,
  clan_tag    TEXT,
  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS players (
  platform        TEXT NOT NULL,
  account_id      TEXT NOT NULL,      -- PUBG accountId
  player_name     TEXT NOT NULL,      -- 표시 닉네임(변경될 수 있으니 최신값으로 갱신)
  discord_user_id TEXT,               -- 선택
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (platform, account_id)
);

CREATE TABLE IF NOT EXISTS clan_members (
  clan_id    TEXT NOT NULL,
  platform   TEXT NOT NULL,
  account_id TEXT NOT NULL,
  clan_role  TEXT,                    -- leader / manager / member (선택)
  is_active  INTEGER NOT NULL DEFAULT 1,
  joined_at  TEXT DEFAULT (datetime('now')),
  left_at    TEXT,
  PRIMARY KEY (clan_id, platform, account_id),
  FOREIGN KEY (clan_id) REFERENCES clans(clan_id) ON DELETE CASCADE,
  FOREIGN KEY (platform, account_id) REFERENCES players(platform, account_id) ON DELETE CASCADE
);

-- 리더보드 조회용 “최신 스냅샷”
CREATE TABLE IF NOT EXISTS player_kills_current (
  platform    TEXT NOT NULL,
  account_id  TEXT NOT NULL,
  source      TEXT NOT NULL DEFAULT 'lifetime',  -- 우선 lifetime만
  kills_total INTEGER NOT NULL DEFAULT 0,
  breakdown   TEXT NOT NULL DEFAULT '{}',        -- 모드별 kills JSON 문자열 저장
  fetched_at  TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (platform, account_id),
  FOREIGN KEY (platform, account_id) REFERENCES players(platform, account_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_members_active
ON clan_members (clan_id, is_active);

CREATE INDEX IF NOT EXISTS idx_kills_rank
ON player_kills_current (kills_total DESC);
