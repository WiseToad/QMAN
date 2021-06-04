CREATE VIEW QMAN_TASK_LOGS_VW (
    ID,
    RUN_ID,
    QUEUE_ID,
    GROUP_ID,
    TASK_ID,
    QUEUE_CODE,
    GROUP_CODE,
    TASK_CODE,
    RUN_PROC,
    BYPASS,
    START_DATE,
    END_DATE,
    STATE,
    SESSIONID,
    SID
) AS
select
    id,
    run_id,
    queue_id,
    group_id,
    task_id,
    queue_code,
    group_code,
    task_code, 
    run_proc,
    bypass,
    start_date,
    end_date,
    qman_pkg.get_log_state(id) as state,
    sessionid,
    sid
from
    qman_run_logs
where
    task_id is not null;
