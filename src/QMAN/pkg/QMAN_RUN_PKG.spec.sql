CREATE OR REPLACE PACKAGE QMAN_RUN_PKG AS

-- Механизм очередей QMAN
-- Поддержка выполнения процессов 
-- Версия 02, апрель 2016, WiseToad

-- ВНИМАНИЕ! Процедуры данного пакета не рассчитаны на прямой запуск.
-- Интерфейс предназначен только для внутренних целей механизма очередей QMAN.
-- Для управления очередями и заданиями существует пакет QMAN_PKG.

function calc_queue_next (
    p_run_hours varchar2) 
    return date;

procedure run_queue (
    p_queue_id number);

procedure kick_queue (
    p_queue_id number);

procedure run_task (
    p_task_id number);

end;
/
