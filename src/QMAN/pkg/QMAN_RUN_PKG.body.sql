CREATE OR REPLACE PACKAGE BODY QMAN_RUN_PKG AS

NO_LONGER_EXISTS exception;
pragma EXCEPTION_INIT (NO_LONGER_EXISTS, -8103);

function calc_queue_next(p_run_hours varchar2) return date is
    v_run_hours qman_queues.run_hours%type;
    v_run_hour qman_queues.run_hours%type;
    v_hour_index number;
    v_run_next date;

    type t_hour_flags is varray(24) of boolean;
    v_hour_flags t_hour_flags;

    p number;
begin
    v_hour_flags := t_hour_flags();
    v_hour_flags.extend(24);

    v_run_hours := p_run_hours;
    while v_run_hours is not null loop
        p := instr(v_run_hours, ',');
        if p > 0 then
            v_run_hour := substr(v_run_hours, 1, p - 1);
            v_run_hours := substr(v_run_hours, p + 1);
        else
            v_run_hour := v_run_hours;
            v_run_hours := null;
        end if;
        begin
            v_hour_index := to_number(trim(v_run_hour)) + 1;
        exception when VALUE_ERROR then
            raise_application_error(-20500, 'Неверный формат строки часов запуска очереди');            
        end;
        if v_hour_index < 1 or v_hour_index > 24 then
            raise_application_error(-20500, 'Часы запуска очереди должны быть заданы в диапазоне от 0 до 23');
        end if;
        if v_hour_index is not null then
            v_hour_flags(v_hour_index) := true;
        end if;                        
    end loop;                            

    v_run_next := trunc(sysdate, 'hh24'); 
    for i in 1..24 loop
        v_run_next := v_run_next + 1/24;
        v_hour_index := to_number(to_char(v_run_next, 'hh24')) + 1;
        if v_hour_flags(v_hour_index) then
            return v_run_next;
        end if;            
    end loop;
    return null;    
end;

procedure run_queue(p_queue_id number) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_code qman_queues.code%type;
    v_queue_state qman_queues.state%type;
    v_queue_enabled number(1);
    v_check_proc qman_queues.check_proc%type;
    v_run_hours qman_queues.run_hours%type;
    v_run_next date;
    v_run_id number;
    v_run_check boolean;
    v_tasks_exist number;
    
    function check_proc return boolean is -- Запустить внешнюю процедуру проверки условий запуска очереди 
        pragma AUTONOMOUS_TRANSACTION;
        r number(1);
    begin
        if v_check_proc is not null then
            execute immediate 'begin :r := case '||
                v_check_proc||' when true then 1 when false then 0 end; end;'  
            using out r;
            commit;
        end if;
        return r <> 0;
    end;
begin
    select q.code, q.state, q.enabled, q.check_proc, q.run_hours, q.run_next
    into v_queue_code, v_queue_state, v_queue_enabled, v_check_proc, v_run_hours, v_run_next
    from qman_queues q where q.id = p_queue_id
    for update; -- блокировка очереди

    if v_queue_state = 'OK' then
        if sysdate >= v_run_next then
            v_run_next := calc_queue_next(v_run_hours); 
            update qman_queues q set q.run_next = v_run_next where q.id = p_queue_id;
            v_run_check := true;
        end if;
        if nvl(check_proc, v_run_check) and v_queue_enabled <> 0 then
            select count(*) into v_tasks_exist from qman_tasks t, qman_groups g
            where t.queue_id = p_queue_id and g.id = t.group_id 
                and (nvl(t.enabled, 0) = 0 or nvl(g.enabled, 0) = 0)
                and rownum = 1;
            if v_tasks_exist <> 0 then
                notify_pkg.notify('QMAN', 'В очереди '||v_queue_code||' присутствуют отключенные задания либо группы');
            end if;
            update qman_queues q set q.state = 'RUNNING', q.run_id = qman_run_seq.nextval
            where q.id = p_queue_id returning run_id into v_run_id;
            insert into qman_run_logs (id, run_id, queue_id, queue_code, start_date, state)
            values (qman_run_logs_seq.nextval, v_run_id, p_queue_id, v_queue_code, sysdate, 'RUNNING');
        end if;
    end if;
    commit; -- снимаем блокировку очереди
    qman_run_pkg.kick_queue(p_queue_id);
exception when others then
    rollback;
    log_pkg.msg('ERROR', 'Очередь '||v_queue_code||':', 'QMAN');
    log_pkg.err('ERROR', 'QMAN');
end;

procedure kick_queue(p_queue_id number) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_code qman_queues.code%type;
    v_queue_enabled number(1);
    v_queue_async number(1);
    v_queue_state qman_queues.state%type;
    v_queue_limit number;
    v_run_id number;
    v_run_count number;
    v_no_tasks boolean;
    v_notify number(1);
    v_notify_new number(1);
begin
    select q.code, q.enabled, q.async, q.state, q.run_id, q.run_limit, q.notify
    into v_queue_code, v_queue_enabled, v_queue_async, v_queue_state, v_run_id, v_queue_limit, v_notify
    from qman_queues q where q.id = p_queue_id
    for update; -- блокировка очереди

    if v_queue_state = 'RUNNING' and v_run_id is not null then
        v_run_count := 0;
        v_no_tasks := true;
        for r in (
            select q.task_id, q.task_state, q.group_enabled,
                case when q.task_enabled <> 0 and not exists (
                        select null from qman_depends d, qman_tasks p
                        where d.queue_id = p_queue_id and d.child_id = q.task_id
                            and p.queue_id = p_queue_id and p.id = d.parent_id
                            and (p.state <> 'OK' or nvl(p.run_id, 0) < v_run_id))
                    then 1 end task_enabled,                            
                case when (v_queue_async <> 0 and q.group_async <> 0) 
                    or q.task_async <> 0 then 1 end task_async,
                least(coalesce(v_queue_limit, q.group_limit, 5),
                    coalesce(q.group_limit, v_queue_limit, 5)) run_limit
            from (
                select g.id group_id, g.enabled group_enabled,
                    g.async group_async, g.run_limit group_limit,
                    first_value(g.id) over (order by g.order_num) first_group_id,
                    t.id task_id, t.enabled task_enabled, t.async task_async, 
                    case when t.state = 'RUNNING' and s.sid is null then 'BROKEN'
                        else t.state end task_state, 
                    t.order_num 
                from qman_groups g, qman_tasks t, v$session s
                where g.queue_id = p_queue_id 
                    and t.queue_id = p_queue_id and t.group_id = g.id
                    and (t.state <> 'OK' or nvl(t.run_id, 0) < v_run_id)
                    and s.sid(+) = t.sid and s.audsid(+) = t.sessionid 
            ) q
            where q.group_id = q.first_group_id
            order by case when q.task_state = 'OK' then 1 else 0 end,
                q.order_num
        ) loop
            v_no_tasks := false;
            if v_run_count >= r.run_limit then
                exit;
            end if;
            if r.task_state = 'RUNNING' then
                v_run_count := v_run_count + 1;
            elsif r.task_state = 'OK' and r.task_enabled <> 0
                and r.group_enabled <> 0 and v_queue_enabled <> 0
            then
                run_task(r.task_id);
                v_run_count := v_run_count + 1;
            end if;
            if nvl(r.task_async, 0) = 0 then
                exit;
            end if;
        end loop;
        if v_no_tasks then
            update qman_queues set state = 'OK' where id = p_queue_id;
            update qman_run_logs set end_date = sysdate, state = 'OK'
            where run_id = v_run_id and task_id is null;     
        end if;
    end if;

    v_queue_state := qman_pkg.get_queue_state(p_queue_id);
    v_notify_new := case when v_queue_state = 'INACTIVE' and v_run_count = 0 then 1
            when v_queue_state = 'PREFAIL' then 2 when v_queue_state = 'FAILURE' then 3
        end;
    if nvl(v_notify_new, 0) <> nvl(v_notify, 0) then
        update qman_queues set notify = v_notify_new where id = p_queue_id;
    end if;        
    commit; -- снимаем блокировку очереди
    
    if v_notify_new <> nvl(v_notify, 0) then
        notify_pkg.notify('QMAN', 'Очередь '||v_queue_code||' находится в статусе '||v_queue_state);
    end if;
exception when others then
    rollback;
    raise;
end;

procedure run_task(p_task_id number) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_group_id number;
    v_queue_code qman_queues.code%type;
    v_group_code qman_groups.code%type;
    v_task_code qman_tasks.code%type;
    v_task_state qman_tasks.state%type;
    v_bypass number(1);
    v_run_id number;
    v_run_proc qman_tasks.run_proc%type;
    v_run_limit number;
    v_job_sid number;
    v_job_count number;
    v_job_timeout number;
    v_run_log_id number;
    v_retry_count number;

    procedure create_job is             -- Создать джоб в автономной транзакции
        pragma AUTONOMOUS_TRANSACTION;
        v_job binary_integer;
    begin
        /* legacy jobs approach
        dbms_job.submit(v_job, 'qman_run_pkg.run_task('||p_task_id||');');
        commit;
        /**/
        -- scheduler jobs approach
        dbms_scheduler.create_job (
            job_name => 'QMAN_TASK_'||p_task_id,
            job_type => 'PLSQL_BLOCK',
            job_action => 'qman_run_pkg.run_task('||p_task_id||');');
        dbms_scheduler.set_attribute (
            name => 'QMAN_TASK_'||p_task_id,
            attribute => 'MAX_FAILURES',
            value => 1);
        dbms_scheduler.enable (
            name => 'QMAN_TASK_'||p_task_id);
        commit;
        /**/
    exception when others then
        rollback;
        raise;
    end;        

    procedure set_task_state(           -- Зафиксировать состояние задания
        p_state varchar2)               -- Код состояния
    is
        v_session_id number;
        v_sid number;
    begin
        update qman_tasks set state = p_state where id = p_task_id;
        if v_run_log_id is null then
            v_session_id := sys_context('USERENV', 'SESSIONID');
            v_sid := sys_context('USERENV', 'SID');
            update qman_tasks set run_id = v_run_id,
                sessionid = v_session_id, sid = v_sid
            where id = p_task_id;
            insert into qman_run_logs (id, run_id, queue_id, group_id, task_id,
                queue_code, group_code, task_code, run_proc, bypass, start_date,
                state, sessionid, sid)
            values (qman_run_logs_seq.nextval, v_run_id, v_queue_id, v_group_id, p_task_id,
                v_queue_code, v_group_code, v_task_code, v_run_proc, v_bypass, sysdate,
                p_state, v_session_id, v_sid)
            returning id into v_run_log_id;                 
        end if;
        if p_state in ('OK', 'FAILURE') then
            update qman_run_logs set end_date = sysdate, state = p_state
            where id = v_run_log_id;
            v_run_log_id := null;
        end if;                         
    end;
begin
    select q.id, g.id, q.code, g.code, t.code, t.state, t.bypass, q.run_id, t.run_proc,
        least(coalesce(q.run_limit, g.run_limit, 5), coalesce(g.run_limit, q.run_limit, 5))    
    into v_queue_id, v_group_id, v_queue_code, v_group_code, v_task_code, 
        v_task_state, v_bypass, v_run_id, v_run_proc, v_run_limit
    from qman_tasks t, qman_queues q, qman_groups g
    where t.id = p_task_id and q.id = t.queue_id and g.id = t.group_id
    for update of t.state; -- блокировка задания

    if v_task_state <> 'OK' then
        commit; -- снимаем блокировку задания
        return;
    end if;        
    begin
        begin
            v_job_timeout := 30;
            loop
                /* legacy jobs approach
                select rj.sid into v_job_sid from dba_jobs j, dba_jobs_running rj 
                where rj.job(+) = j.job and j.what = 'qman_run_pkg.run_task('||p_task_id||');';
                /**/
                -- scheduler jobs approach
                select rj.session_id into v_job_sid from user_scheduler_running_jobs rj
                where rj.job_name = 'QMAN_TASK_'||p_task_id;
                /**/
                exit when v_job_sid is not null;
                if v_job_timeout <= 0 then
                    raise_application_error(-20550, 'Превышен таймаут ожидания джоба задания');
                end if;
                v_job_timeout := v_job_timeout - 0.5;
                dbms_lock.sleep(0.5);
            end loop;                
        exception when NO_DATA_FOUND then
            /* legacy jobs approach
            select count(*) into v_job_count
            from dba_jobs j, dba_jobs_running rj, qman_tasks t
            where rj.job(+) = j.job and j.what = 'qman_run_pkg.run_task('||t.id||');' 
                and t.queue_id = v_queue_id and nvl(rj.sid, -1) <> sys_context('USERENV', 'SID');
            /**/
            -- scheduler jobs approach
            select count(*) into v_job_count
            from user_scheduler_running_jobs rj, qman_tasks t
            where rj.job_name = 'QMAN_TASK_'||t.id and t.queue_id = v_queue_id
                and nvl(rj.session_id, -1) <> sys_context('USERENV', 'SID');
            /**/
            if v_job_count >= v_run_limit then
                commit; -- снимаем блокировку задания
                return;
            end if;        
            create_job; -- в автономной транзакции, чтобы не снимать блокировку задания
            v_job_timeout := 30;
            loop
                begin
                    /* legacy jobs approach
                    select rj.sid into v_job_sid from dba_jobs j, dba_jobs_running rj 
                    where rj.job(+) = j.job and j.what = 'qman_run_pkg.run_task('||p_task_id||');' 
                        and rj.sid is not null;
                    /**/
                    -- scheduler jobs approach
                    select rj.session_id into v_job_sid from user_scheduler_running_jobs rj
                    where rj.job_name = 'QMAN_TASK_'||p_task_id and rj.session_id is not null;
                    /**/
                    exit;
                exception when NO_DATA_FOUND then
                    null;
                end;                        
                if v_job_timeout <= 0 then
                    raise_application_error(-20550, 'Превышен таймаут запуска джоба задания');
                end if;
                v_job_timeout := v_job_timeout - 0.5;
                dbms_lock.sleep(0.5);
            end loop;
            commit; -- снимаем блокировку задания, которую сейчас должен ждать джоб
        end;
    exception when others then
        set_task_state('FAILURE');
        commit;
        raise;
    end;
    if v_job_sid <> sys_context('USERENV', 'SID') then
        commit; -- снимаем блокировку задания
        return;
    end if;            
    begin
        if nvl(v_bypass, 0) = 0 then
            set_task_state('RUNNING');
            commit; -- снимаем блокировку задания
            execute immediate 'begin '||v_run_proc||'; end;';
            commit;
        elsif v_bypass = 2 then -- одиночный пропуск задания
            update qman_tasks set bypass = null where id = p_task_id;
        end if;
        set_task_state('OK');
        commit;
    exception when others then
        set_task_state('FAILURE');
        commit;
        raise;
    end;
    kick_queue(v_queue_id);
exception when others then
    rollback;
    log_pkg.msg('ERROR', 'Задание '||v_queue_code||'.'||v_task_code||':', 'QMAN');
    log_pkg.err('ERROR', 'QMAN');
end;

end;
/
