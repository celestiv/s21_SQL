CREATE TABLE IF NOT EXISTS Peers
(
    Nickname varchar PRIMARY KEY NOT NULL,
    Birthday DATE NOT NULL,
    CONSTRAINT check_birthday CHECK (Birthday <= CURRENT_DATE)
);

CREATE TABLE IF NOT EXISTS Tasks
(
    Title varchar PRIMARY KEY,
    Parent_Task varchar,
    Max_Xp BIGINT not null
);

CREATE TABLE IF NOT EXISTS Checks
(
    ID BIGINT PRIMARY KEY,
    Peer varchar,
    Task varchar,
    Date DATE NOT NULL DEFAULT current_date,
    FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
    FOREIGN KEY (Task) REFERENCES Tasks(Title)
);

CREATE TABLE IF NOT EXISTS P2P
(
    ID BIGINT PRIMARY KEY,
    Check_id BIGINT,
    Checking_Peer varchar,
    State varchar NOT NULL,
    Time TIME,
    FOREIGN KEY (Checking_Peer) REFERENCES Peers(Nickname),
    FOREIGN KEY (Check_id) REFERENCES Checks(ID)
);

CREATE TABLE IF NOT EXISTS Verter
(
    ID BIGINT PRIMARY KEY,
    Check_id BIGINT,
    State varchar NOT NULL,
    Time TIME NOT NULL,
    FOREIGN KEY (Check_id) REFERENCES Checks(ID)
);

CREATE TABLE IF NOT EXISTS Transferred_Points
(
    ID BIGINT PRIMARY KEY,
    Checking_Peer varchar,
    Checked_Peer varchar,
    Points_Amount BIGINT NOT NULL,
    FOREIGN KEY (Checking_Peer) REFERENCES Peers(Nickname),
    FOREIGN KEY (Checked_Peer) REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS Friends
(
    ID BIGINT PRIMARY KEY,
    Peer1 varchar,
    Peer2 varchar,
    FOREIGN KEY (Peer1) REFERENCES Peers(Nickname),
    FOREIGN KEY (Peer2) REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS Recommendations
(
    ID BIGINT PRIMARY KEY,
    Peer varchar,
    Recommended_Peer varchar,
    FOREIGN KEY (Peer) REFERENCES Peers(Nickname),
    FOREIGN KEY (Recommended_Peer) REFERENCES Peers(Nickname)
);

CREATE TABLE IF NOT EXISTS XP
(
    ID BIGINT PRIMARY KEY,
    Check_id BIGINT,
    XPAmount INT,
    FOREIGN KEY (Check_id) REFERENCES Checks(ID)
);

CREATE TABLE IF NOT EXISTS Time_Tracking
(
    ID BIGINT PRIMARY KEY,
    Peer varchar,
    Date DATE NOT NULL DEFAULT current_date,
    Time TIME NOT NULL,
    State varchar NOT NULL,
    FOREIGN KEY (Peer) REFERENCES Peers(Nickname)
);



CREATE TYPE CheckStatus AS ENUM ('Start', 'Success', 'Failure');


----------      IMPORT      ----------
------------------------------------------------------------------------------------------------------------------------
-- -- импорт данных при помощи простой команды COPY
-- COPY peers FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Peers.csv' DELIMITER ',' CSV HEADER;
-- COPY tasks FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Tasks.csv' DELIMITER ',' CSV HEADER;
-- COPY checks FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Checks.csv' DELIMITER ',' CSV HEADER;
-- COPY verter FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Verter.csv' DELIMITER ',' CSV HEADER;
-- COPY p2p FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/P2P.csv' DELIMITER ',' CSV HEADER;
-- COPY transferred_Points FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Transferred_Points.csv' DELIMITER ',' CSV HEADER;
-- COPY friends FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Friends.csv' DELIMITER ',' CSV HEADER;
-- COPY recommendations FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Recommendations.csv' DELIMITER ',' CSV HEADER;
-- COPY xp FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/XP.csv' DELIMITER ',' CSV HEADER;
-- COPY time_tracking FROM '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Time_Tracking.csv' DELIMITER ',' CSV HEADER;
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
-- -- одна процедура для всех таблиц с параметрами, чтобы не исправлять везде пути вручную
CREATE OR REPLACE PROCEDURE
    proc_import_data(
    IN table_name TEXT,
    IN file_path TEXT,
    IN delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY %I FROM %L DELIMITER %L CSV HEADER',
                    table_name,
                    file_path,
                    delimiter
        );
END;
$$ LANGUAGE plpgsql;

-- вызов единой процедуры
CALL proc_import_data('peers', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Peers.csv', ',');
CALL proc_import_data('tasks', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Tasks.csv', ',');
CALL proc_import_data('checks', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Checks.csv', ',');
CALL proc_import_data('verter', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Verter.csv', ',');
CALL proc_import_data('p2p', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/P2P.csv', ',');
CALL proc_import_data('transferred_points', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Transferred_Points.csv', ',');
CALL proc_import_data('friends', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Friends.csv', ',');
CALL proc_import_data('recommendations', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Recommendations.csv', ',');
CALL proc_import_data('xp', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/XP.csv', ',');
CALL proc_import_data('time_tracking', '/Users/celestiv/SQL2_Info21_v1.0-1/src/csv/Time_Tracking.csv', ',');
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
-- import from peers
CREATE OR REPLACE PROCEDURE
proc_import_data_from_csv_peers(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY peers FROM ''csv/Peers.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- import from tasks
CREATE OR REPLACE PROCEDURE
proc_import_data_from_csv_tasks(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY tasks FROM ''csv/Tasks.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- import from checks
CREATE OR REPLACE PROCEDURE
    proc_import_data_from_csv_checks(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY checks FROM ''csv/Checks.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- import from Friends
CREATE OR REPLACE PROCEDURE
    proc_import_data_from_csv_friends(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY friends FROM ''csv/Friends.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- import from p2p
CREATE OR REPLACE PROCEDURE
    proc_import_data_from_csv_p2p(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY p2p FROM ''csv/P2P.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- import from recommendations
CREATE OR REPLACE PROCEDURE
    proc_import_data_from_csv_recommendations(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY recommendations FROM ''csv/Recommendations.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- import from Time_Tracking
CREATE OR REPLACE PROCEDURE
    proc_import_data_from_csv_time_tracking(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY time_tracking FROM ''csv/Time_Tracking.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- import from transferred points
CREATE OR REPLACE PROCEDURE
    proc_import_data_from_csv_transferred_points(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY transferred_points FROM ''csv/Transferred_Points.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- import from verter
CREATE OR REPLACE PROCEDURE
    proc_import_data_from_csv_verter(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY verter FROM ''csv/Verter.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- import from XP
CREATE OR REPLACE PROCEDURE
    proc_import_data_from_csv_xp(
delimiter TEXT
)
AS $$
BEGIN
    EXECUTE format('COPY xp FROM ''csv/XP.csv'' DELIMITER %L CSV HEADER',
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- вызов процедур с одним параметром Delimeter для каждой таблицы отдельно
------------------------------------------------------------------------------------------------------------------------
-- CALL proc_import_data_from_csv_checks(',');
-- CALL proc_import_data_from_csv_tasks(',');
-- CALL proc_import_data_from_csv_peers(',');
-- CALL proc_import_data_from_csv_verter(',');
-- CALL proc_import_data_from_csv_p2p(',');
-- CALL proc_import_data_from_csv_friends(',');
-- CALL proc_import_data_from_csv_recommendations(',');
-- CALL proc_import_data_from_csv_time_tracking(',');
-- CALL proc_import_data_from_csv_xp(',');
-- CALL proc_import_data_from_csv_transferred_points(',');
------------------------------------------------------------------------------------------------------------------------

----------       EXPORT      ----------
-- -- экспорт данных в папку "out" при помощи простой команды COPY
------------------------------------------------------------------------------------------------------------------------
-- COPY (SELECT * FROM peers) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Peers.csv' DELIMITER ',' CSV HEADER;
-- COPY (SELECT * FROM tasks) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Tasks.csv' DELIMITER ',' CSV HEADER;
-- COPY (SELECT * FROM p2p) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/P2P.csv' DELIMITER ',' CSV HEADER;
-- COPY (SELECT * FROM verter) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Verter.csv' DELIMITER ',' CSV HEADER;
-- COPY (SELECT * FROM checks) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Checks.csv' DELIMITER ',' CSV HEADER;
-- COPY (SELECT * FROM transferred_Points) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Transferred_Points.csv' DELIMITER ',' CSV HEADER;
-- COPY (SELECT * FROM friends) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Friends.csv' DELIMITER ',' CSV HEADER;
-- COPY (SELECT * FROM recommendations) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Recommendations.csv' DELIMITER ',' CSV HEADER;
-- COPY (SELECT * FROM xp) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/XP.csv' DELIMITER ',' CSV HEADER;
-- COPY (SELECT * FROM time_Tracking) TO '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Time_Tracking.csv' DELIMITER ',' CSV HEADER;
------------------------------------------------------------------------------------------------------------------------

-- одна процедура для всех таблиц с параметрами, чтобы не исправлять везде пути вручную
CREATE OR REPLACE PROCEDURE
    proc_export_data(
    target_table TEXT ,
    output_file TEXT,
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
    LANGUAGE plpgsql;

CALL proc_export_data('peers', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Peers.csv', ',');
CALL proc_export_data('tasks', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Tasks.csv', ',');
CALL proc_export_data('checks', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Checks.csv', ',');
CALL proc_export_data('verter', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Verter.csv', ',');
CALL proc_export_data('p2p', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/P2P.csv', ',');
CALL proc_export_data('transferred_points', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Transferred_Points.csv', ',');
CALL proc_export_data('friends', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Friends.csv', ',');
CALL proc_export_data('recommendations', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Recommendations.csv', ',');
CALL proc_export_data('xp', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/XP.csv', ',');
CALL proc_export_data('time_tracking', '/Users/celestiv/SQL2_Info21_v1.0-1/src/out/Time_Tracking.csv', ',');
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
-- отдельные процедуры для импорта таблиц
-- export into peers
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_peers(
    target_table TEXT DEFAULT 'peers',
    output_file TEXT DEFAULT 'out/Peers.csv',
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- CALL proc_export_data_to_csv_peers();
-- export into tasks
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_tasks(
    target_table TEXT DEFAULT 'tasks',
    output_file TEXT DEFAULT 'out/Tasks.csv',
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- export into checks
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_checks(
    IN target_table TEXT DEFAULT 'checks',
    IN output_file TEXT DEFAULT 'out/Checks.csv',
    IN delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- export into friends
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_freinds(
    target_table TEXT DEFAULT 'freinds',
    output_file TEXT DEFAULT 'out/Friends.csv',
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- export into P2P
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_p2p(
    target_table TEXT DEFAULT 'p2p',
    output_file TEXT DEFAULT 'out/P2P.csv',
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- export into recommendations
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_recommendations(
    target_table TEXT DEFAULT 'recommendations',
    output_file TEXT DEFAULT 'out/Recommendations.csv',
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- export into time tracking
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_time_tracking(
    target_table TEXT DEFAULT 'time_tracking',
    output_file TEXT DEFAULT 'out/Time_Tracking.csv',
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- export into transfered points
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_transferred_points(
    target_table TEXT DEFAULT 'transferred_points',
    output_file TEXT DEFAULT 'out/Transferred_Points.csv',
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- export into Verter
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_verter(
    target_table TEXT DEFAULT 'verter',
    output_file TEXT DEFAULT 'out/Verter.csv',
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

-- export into XP
CREATE OR REPLACE PROCEDURE
    proc_export_data_to_csv_xp(
    target_table TEXT DEFAULT 'xp',
    output_file TEXT DEFAULT 'out/XP.csv',
    delimiter TEXT DEFAULT ','
)
AS $$
BEGIN
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        target_table,
        output_file,
        delimiter
    );
END;
$$
LANGUAGE plpgsql;

