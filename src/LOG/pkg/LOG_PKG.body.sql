CREATE OR REPLACE PACKAGE BODY LOG_PKG AS

g_app_locked boolean;
g_level_locked boolean;

g_app_code varchar2(50);
g_msg_level varchar2(15);
g_allow_msg number(1);

g_sessionid varchar2(15);
g_sid number;

procedure msg(p_msg_level varchar2, p_msg_text varchar2,
    p_app_code varchar2, p_call_level number := 0)
is
    pragma AUTONOMOUS_TRANSACTION;

    v_msg_level varchar2(15);
    v_app_code varchar2(50);
    
    v_stack varchar2(32000);
    v_caller varchar2(100);
    v_rownum varchar2(50);
    
    v_msg_date date;
    v_msg_text varchar2(255);
    v_msg_tail varchar2(32000);
    v_lf_pos number;
    v_sp_pos number;
    v_length number;
begin
    v_msg_date := sysdate;
    v_app_code := nvl(upper(substr(trim(p_app_code), 1, 50)), app_pkg.get_app_code); 
    v_msg_level := upper(substr(trim(p_msg_level), 1, 15));
    -- Фильтрация сообщения
    if v_app_code is null then
        if g_app_locked then
            return;
        end if;        
        g_app_locked := true;
        begin
            raise_application_error(-20500, 'Не определен код приложения'); 
        exception when others then
            err(p_app_code => 'LOG');
            raise;
        end;                
    elsif g_app_code is null or v_app_code <> g_app_code then
        begin
            select f.app_code into g_app_code from log_filters f
            where f.app_code = v_app_code and f.filter_type = 'INCLUDE' and rownum = 1;
        exception when NO_DATA_FOUND then
            insert into log_filters(app_code, msg_level, filter_type)
            values (v_app_code, '%', 'INCLUDE');
            commit;
            g_app_code := v_app_code;
        end;
        g_msg_level := null;
    end if;
    if v_msg_level is null then
        if g_level_locked then
            return;
        end if;        
        g_level_locked := true;
        begin
            raise_application_error(-20500, 'Не определен уровень значимости сообщения'); 
        exception when others then
            err(p_app_code => p_app_code);
            raise;
        end;                
    elsif g_msg_level is null or v_msg_level <> g_msg_level  then 
        begin
            select 1 into g_allow_msg from dual
            where exists (
                select null from log_filters f
                where f.app_code = v_app_code and f.filter_type = 'INCLUDE'
                    and v_msg_level like f.msg_level)
            and not exists (
                select null from log_filters f
                where f.app_code = v_app_code and f.filter_type = 'EXCLUDE'
                    and v_msg_level like f.msg_level);
        exception when NO_DATA_FOUND then
            g_allow_msg := 0;
        end;        
    end if;
    if g_allow_msg = 0 then
        return;
    end if;        
    g_app_locked := false;
    g_level_locked := false;
    -- Определение точки вызова
    v_stack := dbms_utility.format_call_stack;
    v_lf_pos := instr(v_stack, chr(10), 1, 4);
    v_stack := substr(v_stack, v_lf_pos + 1);
    if p_call_level > 0 then -- задан корректный p_call_level
        v_lf_pos := instr(v_stack, chr(10), 1, p_call_level);
        if v_lf_pos > 0 and v_lf_pos < length(v_stack) then
            -- заданный p_call_level не превышает глубину стека
            v_stack := substr(v_stack, v_lf_pos + 1);
        end if;
    end if;
    v_stack := substr(v_stack, 1, instr(v_stack, chr(10)) - 1);
    v_stack := ltrim(substr(v_stack, instr(v_stack, ' ')));
    v_sp_pos := instr(v_stack, ' ');
    v_rownum := substr(v_stack, 1, v_sp_pos - 1);
    v_caller := ltrim(substr(v_stack, v_sp_pos));
    if v_caller != 'anonymous block' then
        v_caller := substr(v_caller, instr(v_caller, ' ', -1) + 1);
    end if;
    v_caller := v_caller||' ('||v_rownum||')';
    -- Определение неизменных параметров сессии
    if g_sessionid is null then
        g_sessionid := sys_context('USERENV', 'SESSIONID');
    end if;
    if g_sid is null then
        g_sid := sys_context('USERENV', 'SID');
    end if;
    -- Разбиение на части и вывод сообщения
    v_msg_tail := rtrim(p_msg_text);
    loop
        -- Разбиение на подстроки символами chr(10) или если сообщение слишком длинное
        v_lf_pos := instr(v_msg_tail, chr(10));
        v_length := length(v_msg_tail);
        if v_lf_pos > 0 and v_lf_pos <= 251 then
            v_msg_text := rtrim(substr(v_msg_tail, 1, v_lf_pos - 1));
            v_msg_tail := substr(v_msg_tail, v_lf_pos + 1);
        elsif v_length > 250 then
            v_sp_pos := instr(v_msg_tail, ' ', 250 - v_length - 1);
            if v_sp_pos = 0 then
                v_sp_pos := 250;
            end if;
            v_msg_text := rtrim(substr(v_msg_tail, 1, v_sp_pos));
            v_msg_tail := ltrim(substr(v_msg_tail, v_sp_pos + 1));
        else
            v_msg_text := v_msg_tail;
            v_msg_tail := null;
        end if;
        -- Вывод подстроки в протокол
        insert into logs(msg_seq, msg_date, msg_level,
            msg_text, app_code, caller, sessionid, sid)
        values (logs_seq.nextval, v_msg_date, v_msg_level,
            v_msg_text, v_app_code, v_caller, g_sessionid, g_sid);
        commit;
        -- Выход, если больше нечего выводить, иначе продолжение разбиения
        exit when v_msg_tail is null;
    end loop;
end;

procedure err(p_msg_level varchar2, p_app_code varchar2, p_call_level number) is
begin
    msg(p_msg_level, sqlerrm||chr(10)||dbms_utility.format_error_backtrace,
        p_app_code, nvl(p_call_level, 0) + 1);
end;

procedure set_filter(p_app_code varchar2, p_include_levels varchar2, 
    p_exclude_levels varchar2 := null)
is
    pragma AUTONOMOUS_TRANSACTION;

    v_app_code varchar2(50);
begin
    begin
        v_app_code := upper(trim(p_app_code));
    exception when VALUE_ERROR then
        raise_application_error(-20500, 'Слишком длинный код приложения');
    end;
    if v_app_code is null then
        raise_application_error(-20500, 'Не задан код приложения');
    end if;        

    delete from log_filters where app_code = v_app_code;

    insert into log_filters(app_code, msg_level, filter_type)
    select v_app_code, msg_level, 'INCLUDE' from ( 
        select upper(trim(regexp_replace(p_include_levels||'|', '(.*?\|){'||(level-1)||'}(.*)\|.*$', '\2'))) msg_level
        from dual start with p_include_levels is not null connect by level-1 <= length(regexp_replace(p_include_levels, '[^|]'))
    ) where msg_level is not null;         

    insert into log_filters(app_code, msg_level, filter_type)
    select v_app_code, msg_level, 'EXCLUDE' from ( 
        select upper(trim(regexp_replace(p_exclude_levels||'|', '(.*?\|){'||(level-1)||'}(.*)\|.*$', '\2'))) msg_level
        from dual start with p_exclude_levels is not null connect by level-1 <= length(regexp_replace(p_exclude_levels, '[^|]'))
    ) where msg_level is not null;

    commit;         
end;    

end;
/
