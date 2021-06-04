﻿CREATE TABLE QMAN_GROUPS (
    ID        NUMBER        NOT NULL,
    QUEUE_ID  NUMBER        NOT NULL,
    ORDER_NUM NUMBER        NOT NULL,
    CODE      VARCHAR2(50)  NOT NULL,
    NAME      VARCHAR2(100) NOT NULL,
    ENABLED   NUMBER(1, 0),
    ASYNC     NUMBER(1, 0),
    RUN_LIMIT NUMBER,

    CHECK (order_num = trunc(order_num)),
    CHECK (enabled IN (0, 1)),
    CHECK (async IN (0, 1)),

    CONSTRAINT QMAN_GROUPS_PK PRIMARY KEY (ID),
    CONSTRAINT QMAN_GROUPS_FK01 FOREIGN KEY (QUEUE_ID) REFERENCES QMAN_QUEUES (ID),
    CONSTRAINT QMAN_GROUPS_UK01 UNIQUE (QUEUE_ID, CODE),
    CONSTRAINT QMAN_GROUPS_UK02 UNIQUE (QUEUE_ID, ORDER_NUM)
);

COMMENT ON TABLE QMAN_GROUPS IS 'Группы заданий';