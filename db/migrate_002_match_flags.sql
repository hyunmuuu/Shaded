-- 1) 컬럼 추가 (기존 데이터 안 날아감)
ALTER TABLE matches ADD COLUMN is_custom_match INTEGER NOT NULL DEFAULT 0;
ALTER TABLE matches ADD COLUMN is_casual      INTEGER NOT NULL DEFAULT 0;

-- 2) 기존 데이터 백필(대충 추정)
--    - 캐주얼: game_mode 문자열에 'casual'이 포함되면 1
UPDATE matches
SET is_casual = CASE
  WHEN game_mode IS NOT NULL AND lower(game_mode) LIKE '%casual%' THEN 1
  ELSE 0
END;

-- 3) 인덱스(주간 집계 속도용)
CREATE INDEX IF NOT EXISTS idx_matches_time_flags
ON matches(created_at_utc, is_ranked, is_casual, is_custom_match);
