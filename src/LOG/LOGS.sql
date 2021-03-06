CREATE TABLE LOGS (
    MSG_SEQ   NUMBER       NOT NULL,
    MSG_DATE  DATE         NOT NULL,
    MSG_LEVEL VARCHAR2(15) NOT NULL,
    MSG_TEXT  VARCHAR2(250),
    APP_CODE  VARCHAR2(50) NOT NULL,
    CALLER    VARCHAR2(100),
    SESSIONID VARCHAR2(15),
    SID       NUMBER
);

COMMENT ON TABLE LOGS IS 'Протоколы';
COMMENT ON COLUMN LOGS.APP_CODE IS 'Код приложения';
COMMENT ON COLUMN LOGS.CALLER IS 'Точка вызова';
COMMENT ON COLUMN LOGS.MSG_DATE IS 'Дата регистрации сообщения';
COMMENT ON COLUMN LOGS.MSG_LEVEL IS 'Уровень важности сообщения';
COMMENT ON COLUMN LOGS.MSG_SEQ IS 'Порядковый номер сообщения';
COMMENT ON COLUMN LOGS.MSG_TEXT IS 'Текст сообщения';
COMMENT ON COLUMN LOGS.SESSIONID IS 'Уникальный ID сессии';
COMMENT ON COLUMN LOGS.SID IS 'SID сессии';

CREATE INDEX LOGS_IX01 ON LOGS (MSG_SEQ);
CREATE INDEX LOGS_IX02 ON LOGS (MSG_DATE);
CREATE BITMAP INDEX LOGS_BM01 ON LOGS (APP_CODE);
CREATE BITMAP INDEX LOGS_BM02 ON LOGS (MSG_LEVEL);

