-- 1) Создать хранимую процедуру, которая, не уничтожая базу данных,
--     уничтожает все те таблицы текущей базы данных,
--     имена которых начинаются с фразы 'TableName'

-- CREATE DATABASE temp;
-- DROP DATABASE temp;
-- DROP TABLE IF EXISTS "AnotherTable_1";
-- DROP TABLE IF EXISTS "AnotherTable_2";

CREATE TABLE IF NOT EXISTS "TableName_1" (
    column_1 VARCHAR,
    column_2 INT
);

CREATE TABLE IF NOT EXISTS "TableName_2" (
    column_1 VARCHAR,
    column_2 INT
);

CREATE TABLE IF NOT EXISTS "AnotherTable_1" (
    column_1 VARCHAR,
    column_2 INT
);

CREATE TABLE IF NOT EXISTS "AnotherTable_2" (
    column_1 VARCHAR,
    column_2 INT
);

-- основная процедура
DROP PROCEDURE IF EXISTS drop_tables(drop_table_prefix VARCHAR);
CREATE OR REPLACE PROCEDURE drop_tables(
    IN drop_table_prefix VARCHAR DEFAULT 'TableName'
)
AS $$
DECLARE
    dt RECORD;
BEGIN
    FOR dt IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_catalog = current_database()
          AND table_name LIKE (drop_table_prefix || '%')
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(dt.table_name) || ' CASCADE';
        END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL drop_tables();
-- SELECT * FROM "TableName_1";
-- SELECT * FROM "TableName_2";



-- 2) Создать хранимую процедуру с выходным параметром,
--     которая выводит список имен и параметров
--     всех скалярных SQL функций пользователя в текущей базе данных.
--     Имена функций без параметров не выводить.
--     Имена и список параметров должны выводиться в одну строку.
--     Выходной параметр возвращает количество найденных функций.

-- создание временных таблиц с входными параметрами для проверки работоспособности
CREATE OR REPLACE FUNCTION scalar_func_1()
RETURNS TABLE
(
    cur_date date
)
AS $$
BEGIN
    RETURN QUERY
        SELECT current_date;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM scalar_func_1();

CREATE OR REPLACE FUNCTION scalar_func_2(lag INTEGER)
RETURNS TABLE
(
    cur_date date
)
AS $$
BEGIN
    RETURN QUERY
        SELECT current_date - lag;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM scalar_func_2(1);

CREATE OR REPLACE FUNCTION scalar_func_3(lag INTEGER, lag2 INTEGER)
    RETURNS TABLE
            (
                cur_date date
            )
AS $$
BEGIN
    RETURN QUERY
        SELECT current_date - lag - lag2;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM scalar_func_3(1, 5);


-- основная процедура в ex 02
CREATE OR REPLACE PROCEDURE proc_get_scalar_functions(
    IN cur refcursor
) AS $$
BEGIN
    OPEN cur FOR
        WITH
        t1 AS (
            SELECT proname, proargnames, proargtypes
            FROM pg_proc
            WHERE proargnames IS NOT NULL
            AND proname NOT LIKE '%pg%'),
        t2 AS (
            SELECT routine_name
            FROM information_schema.routines
            WHERE specific_schema = 'public'
            AND routine_type = 'FUNCTION')
        SELECT proname, proargnames FROM t1
        JOIN t2 ON t1.proname = t2.routine_name;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL proc_get_scalar_functions('cur');
    FETCH ALL IN cur;
END;


-- 3) Создать хранимую процедуру с выходным параметром,
--     которая уничтожает все SQL DML триггеры в текущей базе данных.
--     Выходной параметр возвращает количество уничтоженных триггеров.

CREATE OR REPLACE PROCEDURE proc_drop_dml_triggers(
    OUT trg_count INTEGER)
AS $$
DECLARE
    trigger_name TEXT;
    object_table TEXT;
    triggers_count INTEGER := 0;
BEGIN
    FOR trigger_name, object_table IN
        SELECT info.trigger_name, info.event_object_table AS object_table
        FROM information_schema.triggers AS info
        WHERE trigger_schema = 'public'
        AND event_manipulation IN ('INSERT', 'UPDATE', 'DELETE')
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I;', trigger_name, 'public', object_table);
        triggers_count := triggers_count + 1;
    END LOOP;
    SELECT triggers_count INTO trg_count;
END;
$$ LANGUAGE plpgsql;

-- при тестировании создаются 2 процедуры из part2.sql
CALL proc_drop_dml_triggers(0);


-- 4) Создать хранимую процедуру с входным параметром,
--     которая выводит имена и описания типа объектов
--     (только хранимых процедур и скалярных функций),
--     в тексте которых на языке SQL встречается строка, задаваемая параметром процедуры

CREATE OR REPLACE PROCEDURE proc_print_routines_descriptions(IN fnc_name VARCHAR, IN cur refcursor)
AS $$
BEGIN
    OPEN cur FOR
        SELECT routine_name AS name, routine_type AS type
        FROM information_schema.routines
        WHERE specific_schema = 'public' AND routine_definition LIKE concat('%', fnc_name, '%');
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL proc_print_routines_descriptions('p2p', 'cur');
    FETCH ALL IN cur;
END;