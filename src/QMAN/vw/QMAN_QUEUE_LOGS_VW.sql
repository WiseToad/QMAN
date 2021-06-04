CREATE VIEW QMAN_QUEUE_LOGS_VW (
    ID,
    RUN_ID,
    QUEUE_ID,
    QUEUE_CODE,
    START_DATE,
    END_DATE,
    STATE
) AS
select
    id,
    run_id,
    queue_id,
    queue_code,
    start_date, 
    end_date,
    qman_pkg.get_log_state(id) as state
from
    qman_run_logs
where
    task_id is null;
