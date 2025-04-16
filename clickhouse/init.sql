CREATE TABLE IF NOT EXISTS users_summary (
    country String,
    avg_age Float32
) ENGINE = MergeTree
ORDER BY country;
