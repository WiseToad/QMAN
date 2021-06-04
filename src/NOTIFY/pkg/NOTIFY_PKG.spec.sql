CREATE OR REPLACE PACKAGE NOTIFY_PKG AS

procedure notify (                      -- Разослать сообщения подписчикам
    p_channel_code varchar2,            -- Код канала оповещения
    p_text varchar2);                   -- Текст сообщения

end;
/
