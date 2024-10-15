SELECT
    queryid,
    query,
    calls,
    total_exec_time,
    mean_exec_time AS avg_response_time_ms,
    min_exec_time,
    max_exec_time,
    stddev_exec_time,
    rows
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 20;

