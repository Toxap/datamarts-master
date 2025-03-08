CREATE EXTERNAL TABLE IF NOT EXISTS nba_engine.dim_ctn_ban_call_cycle
(
    subscriber_sk bigint,
    subs_key string,
    ban_key string,
    first_ctn string,
    first_ban bigint
)
PARTITIONED BY (time_key_src STRING)
STORED AS ORC;