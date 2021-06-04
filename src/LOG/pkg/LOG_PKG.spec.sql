CREATE OR REPLACE PACKAGE LOG_PKG AS

-- Средства протоколирования 
-- Версия 02, апрель 2016, WiseToad

procedure msg (                         -- Вывести сообщение в протокол
    p_msg_level varchar2,               -- Уровень значимости сообщения (ERROR, WARNING, INFO, DEBUG)
    p_msg_text varchar2,                -- Текст сообщения
    p_app_code varchar2 := null,        -- Принудительный код приложения
    p_call_level number := 0);          -- Уровень вложенности при вызове процедуры из враппера

procedure err (                         -- Вывести текущее исключение в протокол
    p_msg_level varchar2 := 'ERROR',    -- Уровень значимости сообщения (ERROR, WARNING)
    p_app_code varchar2 := null,        -- Принудительный код приложения
    p_call_level number := 0);          -- Уровень вложенности при вызове процедуры из враппера

procedure set_filter (                  -- Задать фильтр сообщений
    p_app_code varchar2,                -- Код приложения
    p_include_levels varchar2,          -- Маски включаемых уровней важности, через '|'
    p_exclude_levels varchar2 := null); -- Маски исллючаемых уровней важности, через '|'

end;
/
