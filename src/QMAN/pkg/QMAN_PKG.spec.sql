CREATE OR REPLACE PACKAGE QMAN_PKG AS

-- Механизм очередей QMAN
-- Средства управления метаданными и процессами очередей, групп и заданий 
-- Версия 02, апрель 2016, WiseToad

-- Управление метаданными очередей

procedure create_queue (                -- Создать очередь
    p_queue_code varchar2,              -- Код очереди
    p_queue_name varchar2);             -- Имя очереди

procedure drop_queue (                  -- Удалить очередь
    p_queue_code varchar2);             -- Код очереди
    
procedure set_queue_code (              -- Установить код очереди
    p_queue_code varchar2,              -- Код очереди
    p_new_code varchar2);               -- Новый код очереди

procedure set_queue_name (              -- Установить имя очереди
    p_queue_code varchar2,              -- Код очереди
    p_queue_name varchar2);             -- Имя очереди

procedure set_queue_enabled (           -- Установить признак включения очереди
    p_queue_code varchar2,              -- Код очереди
    p_enabled boolean);                 -- Признак включения очереди
    
procedure set_queue_async (             -- Установить признак асинхронности очереди
    p_queue_code varchar2,              -- Код очереди
    p_async boolean);                   -- Признак асинхронности очереди
    
procedure set_queue_limit (             -- Установить порог одновременно работающих заданий очереди
    p_queue_code varchar2,              -- Код очереди
    p_run_limit number);                -- Порог одновременно работающих заданий очереди

procedure set_queue_hours (             -- Установить часы запуска очереди
    p_queue_code varchar2,              -- Код очереди
    p_run_hours varchar2);              -- Часы запуска очереди в виде строки через запятую

procedure set_queue_next (              -- Установить дату/время следующего запуска очереди
    p_queue_code varchar2,              -- Код очереди
    p_run_next date);                   -- Дата/время следующего запуска очереди

procedure set_queue_check_proc (        -- Установить процедуру проверки условий запуска очереди
    p_queue_code varchar2,              -- Код очереди
    p_check_proc varchar2);             -- Процедура проверки условий запуска очереди

-- Управление метаданными групп заданий

procedure create_group (                -- Создать группу заданий
    p_queue_code varchar2,              -- Код очереди
    p_group_code varchar2,              -- Код группы заданий
    p_group_name varchar2);             -- Имя группы заданий
    
procedure drop_group (                  -- Удалить группу заданий
    p_queue_code varchar2,              -- Код очереди
    p_group_code varchar2);             -- Код группы заданий

procedure set_group_code (              -- Установить код группы заданий
    p_queue_code varchar2,              -- Код очереди
    p_group_code varchar2,              -- Код группы заданий
    p_new_code varchar2);               -- Новый код группы заданий

procedure set_group_name (              -- Установить имя группы заданий
    p_queue_code varchar2,              -- Код очереди
    p_group_code varchar2,              -- Код группы заданий
    p_group_name varchar2);             -- Имя группы заданий

procedure set_group_order (             -- Установить порядок выполнения группы заданий
    p_queue_code varchar2,              -- Код очереди
    p_group_code varchar2,              -- Код группы заданий
    p_group_after varchar2);            -- Код группы, после которой следует поставить данную группу

procedure set_group_enabled (           -- Установить признак включения группы заданий
    p_queue_code varchar2,              -- Код очереди
    p_group_code varchar2,              -- Код группы заданий
    p_enabled boolean);                 -- Признак включения группы заданий

procedure set_group_async (             -- Установить признак асинхронности группы заданий
    p_queue_code varchar2,              -- Код очереди
    p_group_code varchar2,              -- Код группы заданий
    p_async boolean);                   -- Признак асинхронности группы заданий

procedure set_group_limit (             -- Установить порог одновременно работающих заданий группы
    p_queue_code varchar2,              -- Код очереди
    p_group_code varchar2,              -- Код группы заданий
    p_run_limit number);                -- Порог одновременно работающих заданий группы

-- Управление метаданными заданий

procedure create_task (                 -- Создать задание
    p_queue_code varchar2,              -- Код очереди
    p_group_code varchar2,              -- Код группы заданий
    p_task_code varchar2,               -- Код задания
    p_run_proc varchar2);               -- Исполняемая процедура задания

procedure drop_task (                   -- Удалить задание
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2);              -- Код задания

procedure set_task_code (               -- Установить код задания
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2,               -- Код задания
    p_new_code varchar2);               -- Новый код задания

procedure set_task_proc (               -- Установить исполняемую процедуру задания
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2,               -- Код задания
    p_run_proc varchar2);               -- Исполняемая процедура задания

procedure set_task_order (              -- Установить порядок выполнения задания
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2,               -- Код задания
    p_task_after varchar2);             -- Код задания, после которого следует поставить данное задание

procedure set_task_enabled (            -- Установить признак включения задания
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2,               -- Код задания
    p_enabled boolean);                 -- Признак включения задания

procedure set_task_bypass (             -- Установить признак постоянного пропуска задания 
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2,               -- Код задания
    p_bypass boolean);                  -- Признак постоянного пропуска задания

procedure set_task_skip (               -- Установить признак одиночного пропуска задания 
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2,               -- Код задания
    p_skip boolean);                    -- Признак одиночного пропуска задания

procedure set_task_async (              -- Установить признак асинхронности задания
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2,               -- Код задания
    p_async boolean);                   -- Признак асинхронности задания

procedure add_task_parent(              -- Добавить зависимость задания
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2,               -- Код задания
    p_parent_code varchar2);            -- Код родительского задания

procedure remove_task_parent(           -- Удалить зависимость задания
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2,               -- Код задания
    p_parent_code varchar2);            -- Код родительского задания

-- Управление выполнением

procedure recover_task (                -- Перезапустить задание после остановки (по ошибке)
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2);              -- Код задания

procedure kill_task (                   -- Прервать выполнение задания
    p_queue_code varchar2,              -- Код очереди
    p_task_code varchar2);              -- Код задания

-- Мониторинг состояния

function get_task_state (               -- Получить точное состояние задания
    p_task_id number)                   -- ID задания
    return varchar2;                    -- Состояние задания

function get_queue_state (              -- Получить точное состояние очереди
    p_queue_id number)                  -- ID очереди
    return varchar2;                    -- Состояние очереди

function get_log_state (                -- Получить точное состояние задания/очереди в журнале выполнения
    p_log_id number)                    -- ID журнальной записи
    return varchar2;                    -- Состояние задания/очереди в журнале выполнения

end;
/
