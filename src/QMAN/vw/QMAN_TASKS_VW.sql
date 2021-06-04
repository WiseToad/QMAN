CREATE VIEW QMAN_TASKS_VW (
    ID,
    QUEUE_ID,
    GROUP_ID,
    ORDER_NUM,
    CODE,
    RUN_PROC,
    ENABLED,
    BYPASS,
    ASYNC,
    STATE,
    RUN_ID,
    SESSIONID,
    SID
) AS
select
    id,
    queue_id,
    group_id,
    order_num,
    code,
    run_proc,
    enabled,
    bypass,
    async,
    qman_pkg.get_task_state(id) as state,
    run_id,
    sessionid,
    sid
from
    qman_tasks;
