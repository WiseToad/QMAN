CREATE VIEW QMAN_QUEUES_VW (
    ID,
    CODE,
    NAME,
    ENABLED,
    ASYNC,
    STATE,
    RUN_ID,
    RUN_LIMIT,
    RUN_HOURS,
    RUN_NEXT,
    CHECK_PROC
) AS
select
    id,
    code,
    name,
    enabled,
    async,
    qman_pkg.get_queue_state(id) as state,
    run_id,
    run_limit,
    run_hours,
    run_next,
    check_proc
from
    qman_queues;
