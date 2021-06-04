CREATE OR REPLACE PACKAGE BODY QMAN_PKG AS

UNIQUE_VIOLATED exception;
pragma EXCEPTION_INIT (UNIQUE_VIOLATED, -1);

NO_SUCH_JOB exception;
pragma EXCEPTION_INIT (NO_SUCH_JOB, -27475);

function block_queue (                  -- Заблокировать очередь
    p_queue_code varchar2,              -- Код очереди
    p_must_idle boolean)                -- Признак необходимости нахождения в неактивном состоянии
    return number;                      -- ID очереди

function normalize_proc_name (          -- Нормализовать имя процедуры
    p_proc_name varchar2,               -- Имя процедуры (полное, включая владельца схемы)
    p_ret_type varchar2 := null)        -- Тип возвращаеого значения (если функция)
    return varchar2;                    -- Нормализованное имя

function get_group_id (                 -- Получить ID группы заданий
    p_queue_id number,                  -- ID очереди
    p_group_code varchar2)              -- Код группы заданий
    return number;                      -- ID группы заданий

function get_task_id (                  -- Получить ID задания
    p_queue_id number,                  -- ID очереди
    p_task_code varchar2)               -- Код задания
    return number;                      -- ID задания

-- Управление метаданными очередей

procedure create_queue(p_queue_code varchar2, p_queue_name varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    
    procedure create_job is             -- Создать джоб в автономной транзакции
        pragma AUTONOMOUS_TRANSACTION;
    begin
        dbms_scheduler.create_job (
            job_name => 'QMAN_QUEUE_'||v_queue_id,
            job_type => 'PLSQL_BLOCK',
            job_action => 'qman_run_pkg.run_queue('||v_queue_id||');',
            start_date => trunc(sysdate, 'hh24'),
            repeat_interval => 'FREQ=MINUTELY;INTERVAL=5',
            auto_drop => false,
            enabled => true);
        commit;
    exception when others then
        rollback;
        raise;
    end;        
begin
    begin
        insert into qman_queues (id, code, name)
        values (qman_queues_seq.nextval, upper(trim(p_queue_code)), rtrim(p_queue_name))
        returning id into v_queue_id;
    exception when UNIQUE_VIOLATED then
        raise_application_error(-20500, 'Очередь с указанным кодом уже существует');
    end;
    commit;
    create_job;
exception when others then
    rollback;
    raise;    
end;

function block_queue(p_queue_code varchar2, p_must_idle boolean) return number is
    v_queue_id number;
    v_enabled number(1);
begin
    begin
        select q.id, q.enabled into v_queue_id, v_enabled
        from qman_queues q where q.code = upper(trim(p_queue_code))
        for update;
    exception when NO_DATA_FOUND then 
        raise_application_error(-20500, 'Неверный код очереди');
    end;
    if p_must_idle and (v_enabled <> 0 or get_queue_state(v_queue_id) <> 'OK') then
        raise_application_error(-20500, 'Очередь должна быть отключенной и находиться в состоянии ожидания');
    end if;        
    return v_queue_id;
end;

procedure drop_queue(p_queue_code varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_groups_exist number;
    
    procedure drop_job is               -- Удалить джоб в автономной транзакции
        pragma AUTONOMOUS_TRANSACTION;
    begin
        dbms_scheduler.drop_job (
            job_name => 'QMAN_QUEUE_'||v_queue_id);
        commit;
    exception when NO_SUCH_JOB then
        null;
    when others then
        rollback;
        raise;        
    end;        
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);

    select count(*) into v_groups_exist from qman_groups g
    where g.queue_id = v_queue_id and rownum = 1;
    if v_groups_exist <> 0 then
        raise_application_error(-20500, 'Перед удалением самой очереди '||
            'должны быть удалены все принадлежащие ей группы и задания');
    end if;

    drop_job;
    delete from qman_queues q where q.id = v_queue_id;
    commit;
exception when others then
    rollback;
    raise;    
end;
    
procedure set_queue_code(p_queue_code varchar2, p_new_code varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    begin
        update qman_queues q set q.code = upper(trim(p_new_code)) where q.id = v_queue_id;
    exception when UNIQUE_VIOLATED then
        raise_application_error(-20500, 'Очередь с указанным кодом уже существует');
    end;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_queue_name(p_queue_code varchar2, p_queue_name varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_id number;
begin
    v_id := block_queue(p_queue_code, p_must_idle => false);
    update qman_queues q set q.name = rtrim(p_queue_name) where q.id = v_id;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_queue_enabled(p_queue_code varchar2, p_enabled boolean) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_enabled number(1);
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_enabled := case p_enabled when true then 1 when false then 0 end;
    update qman_queues q set q.enabled = v_enabled where q.id = v_queue_id;
    commit;
    qman_run_pkg.kick_queue(v_queue_id);
exception when others then
    rollback;
    raise;    
end;
    
procedure set_queue_async(p_queue_code varchar2, p_async boolean) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_async number(1);
    v_depend_exist number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);

    select count(*) into v_depend_exist from qman_depends d
    where d.queue_id = v_queue_id and rownum = 1;
    if not nvl(p_async, false) and v_depend_exist <> 0 then
        raise_application_error(-20500, 'Перед переводом очереди в синхронный режим '||
            'должны быть удалены все зависимости между ее заданиями');
    end if;        

    v_async := case p_async when true then 1 when false then 0 end;
    update qman_queues q set q.async = v_async where q.id = v_queue_id;
    commit;
    qman_run_pkg.kick_queue(v_queue_id);
exception when others then
    rollback;
    raise;    
end;
    
procedure set_queue_limit(p_queue_code varchar2, p_run_limit number) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    if p_run_limit < 0 then
        raise_application_error(-20500, 'Неверное значение порога');
    end if;        
    update qman_queues q set q.run_limit = p_run_limit where q.id = v_queue_id;
    commit;
    qman_run_pkg.kick_queue(v_queue_id);
exception when others then
    rollback;
    raise;    
end;

procedure set_queue_hours(p_queue_code varchar2, p_run_hours varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_run_next date;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_run_next := qman_run_pkg.calc_queue_next(p_run_hours);
    update qman_queues q set q.run_hours = p_run_hours, q.run_next = v_run_next where q.id = v_queue_id;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_queue_next(p_queue_code varchar2, p_run_next date) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_run_next date;
    v_run_hours qman_queues.run_hours%type;

    procedure kick_job is
        pragma AUTONOMOUS_TRANSACTION;
    begin        
        dbms_scheduler.run_job(
            job_name => 'QMAN_QUEUE_'||v_queue_id,
            use_current_session => false);
        commit;
    exception when others then
        rollback;
        raise;    
    end;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_run_next := p_run_next;        
    if v_run_next is null then
        select q.run_hours into v_run_hours from qman_queues q where q.id = v_queue_id;
        v_run_next := qman_run_pkg.calc_queue_next(v_run_hours);
    end if;
    update qman_queues q set q.run_next = v_run_next where q.id = v_queue_id;
    commit;
    if sysdate >= v_run_next then
        kick_job;
    end if;
exception when others then
    rollback;
    raise;    
end;

procedure set_queue_check_proc(p_queue_code varchar2, p_check_proc varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_check_proc qman_queues.check_proc%type;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_check_proc := normalize_proc_name(p_check_proc, 'PL/SQL BOOLEAN');
    update qman_queues q set q.check_proc = v_check_proc where q.id = v_queue_id;
    commit;
exception when others then
    rollback;
    raise;    
end;

function normalize_proc_name(p_proc_name varchar2, p_ret_type varchar2 := null) return varchar2 is
    v_proc_name varchar2(4000);
    v_owner varchar2(4000);
    v_pkg varchar2(4000);
    v_proc varchar2(4000);
    v_args varchar2(4000);
    v_exists number;
    v_ret_type varchar2(30);
    p number;
begin
    v_proc_name := trim(p_proc_name);
    if v_proc_name is not null then
        v_proc := v_proc_name;
        p := instr(v_proc, '(');
        if p <= 0 then
            v_args := null;
        else
            v_args := trim(substr(p_proc_name, p));
            v_proc := trim(substr(p_proc_name, 1, p - 1));
        end if;             
        v_proc := upper(v_proc);
        p := instr(v_proc, '.');
        if p <= 0 then
            v_owner := null;
        else            
            v_owner := trim(substr(v_proc, 1, p - 1));
            v_proc := trim(substr(v_proc, p + 1));         
            p := instr(v_proc, '.');
            if p <= 0 then
                v_pkg := null;
            else            
                v_pkg := trim(substr(v_proc, 1, p - 1));
                v_proc := trim(substr(v_proc, p + 1));         
            end if;
        end if;            

        if v_owner is null then
            raise_application_error(-20500, 'Не указан владелец схемы');
        end if;            
        if v_proc is null then
            raise_application_error(-20500, 'Не задано имя процедуры');
        end if;

        if regexp_replace(v_owner, '[A-Z0-9_$#]') is not null
            or regexp_replace(v_pkg, '[A-Z0-9_$#]') is not null
            or regexp_replace(v_proc, '[A-Z0-9_$#]') is not null
        then            
            raise_application_error(-20500, 'Недопустимые символы в имени');
        end if;

        select count(*) into v_exists
        from dba_procedures p, dba_arguments a
        where p.owner = v_owner and p.object_name = nvl(v_pkg, v_proc)
            and ((v_pkg is null and p.procedure_name is null) or (v_pkg is not null and p.procedure_name = v_proc))
            and a.object_id(+) = p.object_id and a.subprogram_id(+) = p.subprogram_id and a.position(+) = 0
            and ((trim(p_ret_type) is null and a.data_type is null) or (p_ret_type = a.data_type));
        if v_exists = 0 then
            raise_application_error(-20500, 'Точка вызова с указанной сигнатурой не найдена');
        end if;
        /*
        Это может означать, что:
        - найдена функция, в то время как указана (должна быть) процедура
        - найдена процедура, в то время как указана (должна быть) функция
        - тип возвращаемого значения найденной функции не совпадает с указанным
        Аргументы не проверяются
        /**/

        /*-- Предыдущий алгоритм проверки сигнатуры. Отключено 2017-11-20 из-за бага
          -- (не работало при наличии перегруженных процедуры и функции)
        select count(*) into v_exists from dba_users u where u.username = v_owner;
        if v_exists = 0 then
            raise_application_error(-20500, 'Неизвестная схема данных: '||v_owner);
        end if;

        select count(*) into v_exists from dba_procedures p
        where p.owner = v_owner and p.object_name = nvl(v_pkg, v_proc)
            and ((v_pkg is null and p.procedure_name is null)
                or (v_pkg is not null and p.procedure_name = v_proc));
        if v_exists = 0 then
            raise_application_error(-20500, 'Неизвестная процедура');
        end if;

        begin
            select a.data_type into v_ret_type from dba_arguments a
            where a.owner = v_owner and a.object_name = v_proc
                and ((v_pkg is null and a.package_name is null)
                    or (v_pkg is not null and a.package_name = v_pkg))
                and a.position = 0;
        exception when NO_DATA_FOUND then
            v_ret_type := null;        
        end;                
        if trim(p_ret_type) is null and v_ret_type is not null then
            raise_application_error(-20500, 'Должна быть указана процедура, а не функция');
        end if;
        if trim(p_ret_type) is not null and v_ret_type is null then
            raise_application_error(-20500, 'Должна быть указана функция, а не процедура');
        end if;
        if upper(trim(p_ret_type)) <> v_ret_type then
            raise_application_error(-20500, 'Неверный тип возвращаемого значения: '''||
                v_ret_type||''' вместо '''||trim(p_ret_type)||'''');
        end if;
        /**/
        
        v_proc_name := case when v_pkg is not null 
            then v_owner||'.'||v_pkg||'.'||v_proc||v_args
            else v_owner||'.'||v_proc||v_args end;
    end if;
    return v_proc_name;        
end;

-- Управление метаданными групп заданий

procedure create_group(p_queue_code varchar2, p_group_code varchar2, p_group_name varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_order_num number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    select nvl(max(g.order_num), 0) + 1 into v_order_num 
    from qman_groups g where g.queue_id = v_queue_id;
    begin
        insert into qman_groups (id, queue_id, order_num, code, name)
        values (qman_groups_seq.nextval, v_queue_id, v_order_num, 
            upper(trim(p_group_code)), rtrim(p_group_name));
    exception when UNIQUE_VIOLATED then
        raise_application_error(-20500, 'Группа заданий с указанным кодом уже существует');
    end;
    commit;
exception when others then
    rollback;
    raise;    
end;
    
function get_group_id(p_queue_id number, p_group_code varchar2) return number is
    v_group_id number;
begin
    begin
        select g.id into v_group_id from qman_groups g 
        where g.code = upper(trim(p_group_code)) and g.queue_id = p_queue_id;
    exception when NO_DATA_FOUND then 
        raise_application_error(-20500, 'Неверный код группы заданий');
    end;
    return v_group_id;
end;

procedure drop_group(p_queue_code varchar2, p_group_code varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_group_id number;
    v_tasks_exist number;
    v_order_num number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_group_id := get_group_id(v_queue_id, p_group_code);

    select count(*) into v_tasks_exist from qman_tasks t
    where t.group_id = v_group_id and rownum = 1;
    if v_tasks_exist <> 0 then
        raise_application_error(-20500, 'Перед удалением группы '||
            'должны быть удалены все принадлежащие ей задания');
    end if;

    delete from qman_groups g where g.id = v_group_id
    returning order_num into v_order_num;        
    update qman_groups g set g.order_num = g.order_num - 1 
    where g.queue_id = v_queue_id and g.order_num > v_order_num;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_group_code(p_queue_code varchar2, p_group_code varchar2, p_new_code varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_group_id number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_group_id := get_group_id(v_queue_id, p_group_code);
    begin
        update qman_groups g set g.code = upper(trim(p_new_code)) where g.id = v_group_id;
    exception when UNIQUE_VIOLATED then
        raise_application_error(-20500, 'Группа заданий с указанным кодом уже существует');
    end;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_group_name(p_queue_code varchar2, p_group_code varchar2, p_group_name varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_group_id number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_group_id := get_group_id(v_queue_id, p_group_code);
    update qman_groups g set g.name = rtrim(p_group_name) where g.id = v_group_id;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_group_order(p_queue_code varchar2, p_group_code varchar2, p_group_after varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_group_id number;
    v_after_id number;
    v_tasks_exist number;
    v_old_num number;
    v_new_num number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_group_id := get_group_id(v_queue_id, p_group_code);

    select count(*) into v_tasks_exist from qman_tasks t 
    where t.group_id = v_group_id and rownum = 1;
    if v_tasks_exist <> 0 then
        raise_application_error(-20500, 'Недопустимо изменять порядок выполнения непустой группы');
    end if;        
    if p_group_after is not null then
        v_after_id := get_group_id(v_queue_id, p_group_after);
        select g.order_num + 1 into v_new_num
        from qman_groups g where g.id = v_after_id;
    else
        select nvl(min(g.order_num), 1) into v_new_num
        from qman_groups g where g.queue_id = v_queue_id;
    end if;        
    update qman_groups g set g.order_num = g.order_num + 1 
    where g.queue_id = v_queue_id and g.order_num >= v_new_num;
    select g.order_num into v_old_num from qman_groups g
    where g.id = v_group_id;
    update qman_groups g set g.order_num = v_new_num
    where g.id = v_group_id; 
    update qman_groups g set g.order_num = g.order_num - 1 
    where g.queue_id = v_queue_id and g.order_num > v_old_num;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_group_enabled(p_queue_code varchar2, p_group_code varchar2, p_enabled boolean) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_group_id number;
    v_enabled number(1);
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_group_id := get_group_id(v_queue_id, p_group_code);
    v_enabled := case p_enabled when true then 1 when false then 0 end;
    update qman_groups g set g.enabled = v_enabled where g.id = v_group_id;
    commit;
    qman_run_pkg.kick_queue(v_queue_id);
exception when others then
    rollback;
    raise;    
end;

procedure set_group_async(p_queue_code varchar2, p_group_code varchar2, p_async boolean) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_group_id number;
    v_async number(1);
    v_depend_exist number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_group_id := get_group_id(v_queue_id, p_group_code);

    select count(*) into v_depend_exist from qman_depends d
    where d.group_id = v_group_id and rownum = 1;
    if not nvl(p_async, false) and v_depend_exist <> 0 then
        raise_application_error(-20500, 'Перед переводом группы в синхронный режим '||
            'должны быть удалены все зависимости между ее заданиями');
    end if;        

    v_async := case p_async when true then 1 when false then 0 end;
    update qman_groups g set g.async = v_async where g.id = v_group_id;
    commit;
    qman_run_pkg.kick_queue(v_queue_id);
exception when others then
    rollback;
    raise;    
end;

procedure set_group_limit(p_queue_code varchar2, p_group_code varchar2, p_run_limit number) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_group_id number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_group_id := get_group_id(v_queue_id, p_group_code);
    if p_run_limit < 0 then
        raise_application_error(-20500, 'Неверное значение порога');
    end if;        
    update qman_groups g set g.run_limit = p_run_limit where g.id = v_group_id;
    commit;
    qman_run_pkg.kick_queue(v_queue_id);
exception when others then
    rollback;
    raise;    
end;

-- Управление метаданными заданий

procedure create_task(p_queue_code varchar2, p_group_code varchar2,
    p_task_code varchar2, p_run_proc varchar2) is

    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_group_id number;
    v_order_num number;
    v_run_proc qman_tasks.run_proc%type;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_group_id := get_group_id(v_queue_id, p_group_code);
    select nvl(max(t.order_num), 0) + 1 into v_order_num 
    from qman_tasks t where t.queue_id = v_queue_id;
    v_run_proc := normalize_proc_name(p_run_proc);
    begin
        insert into qman_tasks (id, queue_id, group_id, order_num, code, run_proc)
        values (qman_tasks_seq.nextval, v_queue_id, v_group_id, v_order_num, 
            upper(trim(p_task_code)), v_run_proc);
    exception when UNIQUE_VIOLATED then
        raise_application_error(-20500, 'Задание с указанным кодом уже существует');
    end;
    commit;
exception when others then
    rollback;
    raise;    
end;

function get_task_id(p_queue_id number, p_task_code varchar2) return number is
    v_task_id number;
begin
    begin
        select t.id into v_task_id from qman_tasks t 
        where t.code = upper(trim(p_task_code)) and t.queue_id = p_queue_id;
    exception when NO_DATA_FOUND then 
        raise_application_error(-20500, 'Неверный код задания');
    end;
    return v_task_id;
end;

procedure drop_task(p_queue_code varchar2, p_task_code varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_depend_exist number;
    v_order_num number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_task_id := get_task_id(v_queue_id, p_task_code);

    select count(*) into v_depend_exist from qman_depends d
    where d.parent_id = v_task_id and rownum = 1;
    if v_depend_exist <> 0 then
        raise_application_error(-20500, 'Нельзя удалить задание, от которого зависят другие');    
    end if;

    delete from qman_depends d where d.child_id = v_task_id;
    delete from qman_tasks t where t.id = v_task_id
    returning order_num into v_order_num;        
    update qman_tasks t set t.order_num = t.order_num - 1 
    where t.queue_id = v_queue_id and t.order_num > v_order_num;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_task_code(p_queue_code varchar2, 
    p_task_code varchar2, p_new_code varchar2) is

    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_task_id := get_task_id(v_queue_id, p_task_code);
    begin
        update qman_tasks t set t.code = upper(trim(p_new_code)) where t.id = v_task_id;
    exception when UNIQUE_VIOLATED then
        raise_application_error(-20500, 'Задание с указанным кодом уже существует');
    end;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_task_proc(p_queue_code varchar2,
    p_task_code varchar2, p_run_proc varchar2) is

    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_run_proc qman_tasks.run_proc%type;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_task_id := get_task_id(v_queue_id, p_task_code);
    v_run_proc := normalize_proc_name(p_run_proc);
    update qman_tasks t set t.run_proc = v_run_proc where t.id = v_task_id;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_task_order(p_queue_code varchar2,
    p_task_code varchar2, p_task_after varchar2) is

    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_group_id number;
    v_after_id number;
    v_after_group number;
    v_old_num number;
    v_new_num number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_task_id := get_task_id(v_queue_id, p_task_code);
    select t.group_id into v_group_id from qman_tasks t where t.id = v_task_id; 

    if p_task_after is not null then
        v_after_id := get_task_id(v_queue_id, p_task_after);
        select t.group_id, t.order_num + 1 into v_after_group, v_new_num
        from qman_tasks t where t.id = v_after_id;
        if v_after_group <> v_group_id then
            raise_application_error(-20500, 'Переупорядочивание '||
                'заданий допустимо только в пределах своей группы');
        end if;                
    else
        select nvl(min(t.order_num), 1) into v_new_num
        from qman_tasks t where t.group_id = v_group_id;
    end if;        
    update qman_tasks t set t.order_num = t.order_num + 1 
    where t.queue_id = v_queue_id and t.order_num >= v_new_num;
    select t.order_num into v_old_num from qman_tasks t 
    where t.id = v_task_id;
    update qman_tasks t set t.order_num = v_new_num
    where t.id = v_task_id; 
    update qman_tasks t set t.order_num = t.order_num - 1 
    where t.queue_id = v_queue_id and t.order_num > v_old_num;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_task_enabled(p_queue_code varchar2,
    p_task_code varchar2, p_enabled boolean) is

    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_enabled number(1);
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_task_id := get_task_id(v_queue_id, p_task_code);
    v_enabled := case p_enabled when true then 1 when false then 0 end;
    update qman_tasks t set t.enabled = v_enabled where t.id = v_task_id;
    commit;
    qman_run_pkg.kick_queue(v_queue_id);
exception when others then
    rollback;
    raise;    
end;

procedure set_task_bypass(p_queue_code varchar2, p_task_code varchar2, p_bypass boolean) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_bypass number(1);
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_task_id := get_task_id(v_queue_id, p_task_code);

    select t.bypass into v_bypass from qman_tasks t where t.id = v_task_id;
    if not nvl(p_bypass, false) and v_bypass = 2 then
        raise_application_error(-20500, 'Для задания установлен признак одиночного пропуска');
    end if;            
    v_bypass := case p_bypass when true then 1 when false then 0 end;
    update qman_tasks t set t.bypass = v_bypass where t.id = v_task_id;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_task_skip(p_queue_code varchar2, p_task_code varchar2, p_skip boolean) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_bypass number(1);
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_task_id := get_task_id(v_queue_id, p_task_code);

    select t.bypass into v_bypass from qman_tasks t where t.id = v_task_id;
    if v_bypass = 1 then
        raise_application_error(-20500, 'Для задания установлен признак постоянного пропуска');
    end if;
    if not nvl(p_skip, false) and nvl(v_bypass, 0) = 0 then
        raise_application_error(-20500, 'Признак одиночного пропуска не установлен '||
            '(возможно сброшен автоматически при выполнении очереди)');
    end if;            
    v_bypass := case p_skip when true then 2 when false then 0 end;
    update qman_tasks t set t.bypass = v_bypass where t.id = v_task_id;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure set_task_async(p_queue_code varchar2,
    p_task_code varchar2, p_async boolean) is

    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_async number(1);
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_task_id := get_task_id(v_queue_id, p_task_code);
    v_async := case p_async when true then 1 when false then 0 end;
    update qman_tasks t set t.async = v_async where t.id = v_task_id;
    commit;
    qman_run_pkg.kick_queue(v_queue_id);
exception when others then
    rollback;
    raise;    
end;

procedure add_task_parent(p_queue_code varchar2,
    p_task_code varchar2, p_parent_code varchar2) is

    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_parent_id number;
    v_group_id number;
    v_group_async number(1);
    v_queue_async number(1);

    procedure check_depend_loop(        -- Выполнить проверку на отсутствие циклов в зависимостях
        p_ancestor_id number) is        -- ID родительского задания
    begin
        if p_ancestor_id = v_task_id then
            raise_application_error(-20500, 'Обнаружена циклическая зависимость заданий');
        end if;            
        for r in (
            select d.parent_id from qman_depends d where d.child_id = p_ancestor_id
        ) loop
            check_depend_loop(r.parent_id);
        end loop;
    end;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_task_id := get_task_id(v_queue_id, p_task_code);
    v_parent_id := get_task_id(v_queue_id, p_parent_code);

    select g.id, g.async, q.async into v_group_id, v_group_async, v_queue_async
    from qman_tasks t, qman_groups g, qman_queues q
    where t.id = v_task_id and g.id = t.group_id and q.id = t.queue_id;
    if nvl(v_queue_async, 0) = 0 or nvl(v_group_async, 0) = 0 then
        raise_application_error(-20500, 'Недопустимо задавать зависимости между заданиями в синхронной группе и/или очереди');
    end if;
    check_depend_loop(v_parent_id);

    begin
        insert into qman_depends(id, queue_id, group_id, parent_id, child_id)
        values (qman_depends_seq.nextval, v_queue_id, v_group_id, v_parent_id, v_task_id);
    exception when UNIQUE_VIOLATED then
        null;
    end;
    commit;
exception when others then
    rollback;
    raise;    
end;

procedure remove_task_parent(p_queue_code varchar2,
    p_task_code varchar2, p_parent_code varchar2) is

    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_parent_id number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => true);
    v_task_id := get_task_id(v_queue_id, p_task_code);
    v_parent_id := get_task_id(v_queue_id, p_parent_code);
    delete from qman_depends where parent_id = v_parent_id and child_id = v_task_id;
    commit;
exception when others then
    rollback;
    raise;    
end;

-- Управление выполнением

procedure recover_task(p_queue_code varchar2, p_task_code varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_task_id := get_task_id(v_queue_id, p_task_code);
    if get_task_state(v_task_id) not in ('FAILURE', 'BROKEN') then
        raise_application_error(-20500, 'Задание не находится в сбойном состоянии');
    end if;
    update qman_tasks t set t.state = 'OK', t.run_id = null where t.id = v_task_id;
    commit;
    qman_run_pkg.kick_queue(v_queue_id);
exception when others then
    rollback;
    raise;    
end;

procedure kill_task(p_queue_code varchar2, p_task_code varchar2) is
    pragma AUTONOMOUS_TRANSACTION;
    v_queue_id number;
    v_task_id number;
    v_sid number;
    v_serial# number;
begin
    v_queue_id := block_queue(p_queue_code, p_must_idle => false);
    v_task_id := get_task_id(v_queue_id, p_task_code);
    begin
        select s.sid, s.serial# into v_sid, v_serial#  
        from qman_tasks t, v$session s
        where t.id = v_task_id and t.state = 'RUNNING'
            and s.sid = t.sid and s.audsid = t.sessionid;
    exception when NO_DATA_FOUND then
        raise_application_error(-20500, 'Задание не находится в состоянии выполнения');
    end;                     
    execute immediate 'alter system kill session '''||v_sid||','||v_serial#||''' immediate';
    commit;
exception when others then
    rollback;
    raise;    
end;

-- Мониторинг состояния

/*
Task states:
OK
RUNNING
    BROKEN (нет живой связанной сессии)
FAILURE
/**/

function get_task_state(p_task_id number) return varchar2 is
    v_state qman_tasks.state%type;
begin
    begin
        select case when t.state = 'RUNNING' and s.sid is null then 'BROKEN' else t.state end 
        into v_state from qman_tasks t, v$session s
        where t.id = p_task_id and s.sid(+) = t.sid and s.audsid(+) = t.sessionid;
    exception when NO_DATA_FOUND then
        v_state := null;
    end;
    return v_state;        
end;

/*
Queue states:
OK
RUNNING
    PREFAIL (есть сломанные задания, есть параллельно выполняющиеся задания)
    FAILURE (есть сломанные задания, нет параллельно выполняющихся заданий)
    INACTIVE (нет сломанных заданий, нет выполняющихся заданий, есть невыполненные задания)
/**/

function get_queue_state(p_queue_id number) return varchar2 is
    v_state qman_queues.state%type;
begin
    begin
        select
            case when a.state = 'RUNNING' then
                case when a.running <> 0 then
                    case when a.failure <> 0 or a.broken <> 0 then 'PREFAIL' else 'RUNNING' end
                else
                    case when a.failure <> 0 or a.broken <> 0 then 'FAILURE' else 'INACTIVE' end
                end
            else state end
        into v_state
        from (                    
            select q.state,
                sum(case when t.state = 'RUNNING' and s.sid is not null then 1 end) running,
                sum(case when t.state = 'RUNNING' and s.sid is null then 1 end) broken,
                sum(case when t.state = 'FAILURE' then 1 end) failure
            from qman_queues q, qman_tasks t, v$session s
            where q.id = p_queue_id and t.queue_id(+) = q.id
                and s.sid(+) = t.sid and s.audsid(+) = t.sessionid
            group by q.state
        ) a;
    exception when NO_DATA_FOUND then
        v_state := null;
    end;
    return v_state;        
end;

/*
*/

function get_log_state(p_log_id number) return varchar2 is
    v_state qman_queues.state%type;
begin
    begin
        select
            case when l.state = 'RUNNING' then
                case when l.task_id is not null then
                    case when not exists (select null from v$session s
                        where s.sid = l.sid and s.audsid = l.sessionid)
                    then 'BROKEN' else l.state end
                else
                    (select
                        case when a.running <> 0 then
                            case when a.failure <> 0 or a.broken <> 0 then 'PREFAIL' else 'RUNNING' end
                        else
                            case when a.failure <> 0 or a.broken <> 0 then 'FAILURE' else 'INACTIVE' end
                        end
                    from (                    
                        select
                            sum(case when lt.state = 'RUNNING' and s.sid is not null then 1 end) running,
                            sum(case when lt.state = 'RUNNING' and s.sid is null then 1 end) broken,
                            sum(case when lt.state = 'FAILURE' then 1 end) failure
                        from qman_run_logs lt, v$session s 
                        where lt.id in (
                                select max(lt.id) from qman_run_logs lq, qman_run_logs lt 
                                where lq.id = p_log_id and lt.run_id = lq.run_id and lt.task_id is not null
                                group by lt.task_id)
                            and s.sid(+) = lt.sid and s.audsid(+) = lt.sessionid
                    ) a)
                end 
            else l.state end
        into v_state
        from qman_run_logs l
        where l.id = p_log_id;
    exception when NO_DATA_FOUND then
        v_state := null;
    end;
    return v_state;        
end;

end;
/
