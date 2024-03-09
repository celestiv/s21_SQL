-- ex 1
DROP FUNCTION IF EXISTS fnc_get_peer_points();
CREATE OR REPLACE FUNCTION fnc_get_peer_points()
RETURNS TABLE (
    peer1 VARCHAR,
    peer2 VARCHAR,
    Points_Amount NUMERIC
) AS $$
BEGIN
    RETURN QUERY
        SELECT DISTINCT t1.checking_peer AS peer1,
                        t2.checking_peer AS peer2,
                        (
                            SELECT COALESCE(
                                (
                                    SELECT SUM(t3.points_amount)
                                    FROM transferred_points AS t3
                                    WHERE (t3.checking_peer = t1.checking_peer AND t3.checked_peer = t2.checking_peer)
                                ), 0)
                                -
                                COALESCE(
                                (
                                    SELECT SUM(t4.points_amount)
                                    FROM transferred_points as t4
                                    WHERE (t4.checking_peer = t2.checking_peer AND t4.checked_peer = t1.checking_peer)
                                ), 0)
                        ) AS points_amount
        FROM
            transferred_points AS t1
                CROSS JOIN
            transferred_points AS t2
        WHERE
                t1.checking_peer <> t2.checking_peer
        ORDER BY peer1;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_peer_points();

-- ex 2
DROP FUNCTION IF EXISTS fnc_get_peer_task_xp();
CREATE OR REPLACE FUNCTION fnc_get_peer_task_xp()
RETURNS TABLE (
    peer VARCHAR,
    task VARCHAR,
    xp INTEGER
) AS $$
BEGIN
    RETURN QUERY
        SELECT checks.peer, checks.task, xp.xpamount AS xp
        FROM xp
        LEFT JOIN checks ON checks.id = xp.check_id
        LEFT JOIN verter ON verter.check_id = checks.id
        LEFT JOIN p2p ON xp.check_id = p2p.check_id
        WHERE p2p.state = 'Success' AND (verter.state = 'Success' OR verter.state IS NULL);
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_peer_task_xp();

-- ex 3
DROP FUNCTION IF EXISTS fnc_get_peer_campus_leave(IN date_check DATE);
CREATE OR REPLACE FUNCTION fnc_get_peer_campus_leave(IN date_check DATE) RETURNS TABLE (peer VARCHAR) AS $$
BEGIN
    RETURN QUERY
        SELECT time_tracking.peer
        FROM time_tracking
        WHERE date = date_check
        GROUP BY time_tracking.peer
        HAVING COUNT(state) = 2;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_peer_campus_leave('2023-06-13');

-- ex 4
DROP FUNCTION IF EXISTS fnc_get_peer_points_change();
CREATE OR REPLACE FUNCTION fnc_get_peer_points_change() RETURNS TABLE (peer VARCHAR, points_change NUMERIC) AS $$
BEGIN
    RETURN QUERY
        SELECT DISTINCT checking_peer AS peer, (
            SELECT (
                       SELECT SUM(points_amount)
                       FROM transferred_points AS t2
                       WHERE t2.checking_peer = t1.checking_peer
                   ) - (
                       SELECT SUM(points_amount)
                       FROM transferred_points AS t3
                       WHERE t3.checked_peer = t1.checking_peer
                   ) AS Points_change
        ) AS Points_change
        FROM transferred_points AS t1
        ORDER BY points_change DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_peer_points_change();

-- ex 5
DROP FUNCTION IF EXISTS fnc_get_points_change();
CREATE OR REPLACE FUNCTION fnc_get_points_change()
    RETURNS TABLE (
        Peer VARCHAR(20),
        PointsChange NUMERIC
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT peer1 AS peer, SUM(points_amount) as points_change
        FROM fnc_get_peer_points()
        GROUP BY peer1
        ORDER BY points_change DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_points_change();

-- ex 6
DROP FUNCTION IF EXISTS fnc_get_frequent_checked_task();
CREATE OR REPLACE FUNCTION fnc_get_frequent_checked_task()
    RETURNS TABLE
    (
        Day date,
        Task VARCHAR
    )
AS $$
BEGIN
    RETURN QUERY
        SELECT t1.date, t1.task
        FROM checks AS t1
        INNER JOIN (
            SELECT checks.id, checks.task, checks.date
            FROM checks
            WHERE date = checks.date
        ) AS t2
        ON t1.id = t2.id
        GROUP BY t1.task, t1.date
        HAVING COUNT(t1.task) = (
            SELECT MAX(count) FROM (
               SELECT t3.task, COUNT(t3.task) AS count
               FROM checks AS t3
               WHERE date = t1.date
               GROUP BY t3.task
           ) AS pv
        )
        ORDER BY t1.date;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_frequent_checked_task();


-- ex 7
DROP FUNCTION IF EXISTS fnc_get_finishers(block_name TEXT);
CREATE OR REPLACE FUNCTION fnc_get_finishers(IN block_name TEXT)
RETURNS TABLE (
    Peer VARCHAR(20),
    Day DATE
)
AS $$
BEGIN
    RETURN QUERY (
        SELECT t1.nickname, t2.date
        FROM peers AS t1
        INNER JOIN
        (
        SELECT checks.peer, checks.date
        FROM xp
            LEFT JOIN checks ON checks.id = xp.check_id
            LEFT JOIN verter ON verter.check_id = checks.id
            LEFT JOIN p2p ON xp.check_id = p2p.check_id
        WHERE p2p.state = 'Success'
        AND (verter.state = 'Success' OR verter.state IS NULL)
        AND task = (
            SELECT title
            FROM tasks
            WHERE title LIKE CONCAT(block_name, '%')
            LIMIT 1
        )
        ORDER BY checks.peer, checks.date
        ) AS t2
        ON t1.nickname = t2.peer
        ORDER BY t2.date DESC
);
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_finishers('CPP');


-- ex 8
DROP FUNCTION IF EXISTS fnc_get_recommendations();
CREATE OR REPLACE FUNCTION fnc_get_recommendations()
RETURNS table(
    peer VARCHAR,
    recommended_peer VARCHAR
)
AS $$
BEGIN
    RETURN QUERY (
        SELECT f.peer1, r.recommended_peer FROM friends f
        JOIN recommendations r ON f.peer1 = r.peer
        GROUP BY f.peer1, r.recommended_peer
        ORDER BY count(*)
    );
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_recommendations();


-- ex 9
DROP FUNCTION IF EXISTS fnc_get_started_blocks(block1 TEXT, block2 TEXT);
CREATE OR REPLACE FUNCTION fnc_get_started_blocks(block1 TEXT, block2 TEXT)
RETURNS TABLE (
    StartedBlock1 NUMERIC,
    StartedBlock2 NUMERIC,
    StartedBothBlocks NUMERIC,
    DidntStartAnyBlock NUMERIC
)
AS $$
DECLARE
    all_peers NUMERIC;
BEGIN
    SELECT COUNT(*) INTO all_peers
    FROM Peers;

RETURN QUERY
    WITH
    t1 AS (
        SELECT DISTINCT(peer)
        FROM checks
        WHERE task LIKE block1 || '%'),
    t2 AS (
        SELECT DISTINCT (peer)
        FROM checks
        WHERE task LIKE block2 || '%'
    ),
    t3 AS (
        SELECT t1.peer
        FROM t1
        JOIN t2 ON t1.peer = t2.peer
    ),
    t4 AS (
        SELECT nickname
        FROM peers
        LEFT JOIN t1 ON t1.peer = peers.nickname
        LEFT JOIN t2 ON t2.peer = peers.nickname
        WHERE t1.peer IS NULL
            AND t2.peer IS NULL
    )
    SELECT ROUND((SELECT COUNT(*) FROM t1) * 100.0 / all_peers),
           ROUND((SELECT COUNT(*) FROM t2) * 100.0 / all_peers),
           ROUND((SELECT COUNT(*) FROM t3) * 100.0 / all_peers),
           ROUND((SELECT COUNT(*) FROM t4) * 100.0 / all_peers);
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_started_blocks('C', 'D');

-- ex 10
DROP FUNCTION IF EXISTS fnc_birthday_checks();
CREATE OR REPLACE FUNCTION fnc_birthday_checks()
RETURNS TABLE (
    SuccessfulChecks NUMERIC,
    UnsuccessfulChecks NUMERIC
)
AS $$
DECLARE
    success_c INTEGER;
    failure_c INTEGER;
BEGIN
    SELECT COUNT(p.nickname) INTO success_c
        FROM peers p
        JOIN checks c on p.nickname = c.peer
        JOIN p2p on c.id = p2p.check_id
    WHERE EXTRACT('MONTH' FROM p.birthday::date) = EXTRACT('MONTH' FROM c.date::date)
    AND EXTRACT('DAY' FROM p.birthday::date) = EXTRACT('DAY' FROM c.date::date)
    AND p2p.state = 'Success';

    SELECT COUNT(p.nickname) INTO failure_c
    FROM peers p
        JOIN checks c on p.nickname = c.peer
        JOIN p2p on c.id = p2p.check_id
    WHERE EXTRACT('MONTH' FROM p.birthday::date) = EXTRACT('MONTH' FROM c.date::date)
      AND EXTRACT('DAY' FROM p.birthday::date) = EXTRACT('DAY' FROM c.date::date)
    AND p2p.state = 'Failure';

    RETURN QUERY
        SELECT ROUND(success_c * 100.0 / (success_c + failure_c)),
            ROUND(failure_c * 100.0 / (success_c + failure_c));
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_birthday_checks();

-- ex 11
DROP FUNCTION IF EXISTS fnc_123_checks(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR);
CREATE OR REPLACE FUNCTION fnc_123_checks(IN task1 VARCHAR, IN task2 VARCHAR, IN task3 VARCHAR)
    RETURNS TABLE (
        peers VARCHAR
    )
AS $$
BEGIN
    RETURN QUERY (
        SELECT DISTINCT(peer)
        FROM p2p
        JOIN checks c on c.id = p2p.check_id
        WHERE state = 'Success'
        AND (task LIKE task1 OR task LIKE task2)
        AND task NOT LIKE task3
    );
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_123_checks('C2_SimpleBashUtils', 'C5_s21_decimal', 'C6_s21_matrix');

-- ex 12
DROP FUNCTION IF EXISTS fnc_get_prev_task_count();
CREATE OR REPLACE FUNCTION fnc_get_prev_task_count()
RETURNS TABLE (
    Task VARCHAR,
    PrevCount INTEGER
    )
AS $$
BEGIN
    RETURN QUERY
        WITH RECURSIVE task_recursive(MainTitle, parent, step) AS (
            SELECT title, parent_task, 0
            FROM tasks
            UNION ALL
            SELECT MainTitle, t2.parent_task, 1
            FROM task_recursive tr
                JOIN tasks t2
                ON tr.parent = t2.title
        )
        SELECT MainTitle::VARCHAR AS Task, SUM(step)::INTEGER AS PrevCount
        FROM task_recursive
        GROUP BY MainTitle
        ORDER BY 2;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_get_prev_task_count();

-- ex 13
CREATE OR REPLACE FUNCTION get_lucky_days(IN days INTEGER) RETURNS TABLE (date_row DATE) AS $$
BEGIN
    RETURN QUERY
    WITH data_table AS (
    SELECT checks.id, checks.date, p2p.time, 
        (p2p.state = 'Success' AND 
        (verter.state = 'Success' OR verter.state IS NULL) AND
        (xp.xpamount::float / tasks.max_xp::float) > 0.8) AS success_check
    FROM checks
    LEFT JOIN p2p ON checks.id = p2p.check_id
    LEFT JOIN verter ON checks.id = verter.check_id
    LEFT JOIN xp on checks.id = xp.check_id
    LEFT JOIN tasks on checks.task = tasks.title
    WHERE (p2p.state = 'Success' OR p2p.state = 'Failure') 
        AND (verter.state = 'Success' OR verter.state IS NULL OR verter.state = 'Failure')
    ORDER BY date, time
    )
    SELECT date
    FROM data_table
    WHERE success_check = 'true'
    GROUP BY date
    HAVING COUNT(*) > days;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_lucky_days(1);

-- ex 14
DROP FUNCTION IF EXISTS fnc_get_max_xp_peer();
CREATE OR REPLACE FUNCTION fnc_get_max_xp_peer()
RETURNS TABLE(
    Peer TEXT,
    XP INTEGER
             )
AS $$
BEGIN
    RETURN QUERY (
        SELECT checks.peer::text , SUM(x.xpamount)::INTEGER
        FROM checks
            JOIN xp x on checks.id = x.check_id
        GROUP BY checks.peer
        ORDER BY SUM(x.xpamount) DESC
        LIMIT 1
    );
END;
$$LANGUAGE plpgsql;

SELECT * FROM fnc_get_max_xp_peer();

-- ex 15
CREATE OR REPLACE FUNCTION get_peer_early_comming(IN time_var TIME, IN N INTEGER) RETURNS TABLE (peer VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT time_tracking.peer
    FROM time_tracking
    WHERE time < time_var
    GROUP BY time_tracking.peer
    HAVING COUNT(*) > N;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_peer_early_comming('13:00:00', 2);
-- ex 16
CREATE OR REPLACE FUNCTION get_peer_campus_leave_ext(IN days INTEGER, IN count BIGINT) RETURNS TABLE (peers VARCHAR) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (time_tracking.peer) peer
    FROM time_tracking
    WHERE date BETWEEN current_date - days AND current_date 
    AND state = '2'
    GROUP BY peer, date
    HAVING COUNT(*) - 1 > count
    ORDER BY peer, count DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_peer_campus_leave_ext(30, 1);

-- ex 17
CREATE OR REPLACE FUNCTION get_early_entries() RETURNS TABLE (Month TEXT, early_entries DOUBLE PRECISION) AS $$
BEGIN
RETURN QUERY
    SELECT month1 AS Month, (
        (count2::float / count1::float) * 100
    ) AS Early_Entries FROM(
        SELECT pv1.month AS month1, COUNT(*) AS count1, pv3.month, pv3.count2 FROM (
            SELECT DISTINCT ON (peer, date) peer, date, time, (
                SELECT TO_CHAR(
                    TO_DATE ((
                        SELECT EXTRACT (MONTH FROM date) FROM time_tracking AS m1
                        WHERE m1.id = t1.id
                    )::text, 'MM'), 'Month'
                ) AS month_Name
            ) AS month, (
                SELECT EXTRACT (MONTH FROM date) FROM time_tracking AS m1
                WHERE m1.id = t1.id
            ) AS monthDigit, id FROM time_tracking AS t1
            WHERE (
                SELECT EXTRACT(MONTH FROM date) 
                FROM time_tracking AS t2
                WHERE t1.id = t2.id
                ) = (
                    SELECT EXTRACT (MONTH FROM birthday) 
                    FROM peers AS t3
                    WHERE t3.nickname = t1.peer
                ) AND t1.state = '1'
            ORDER BY peer, date, time
        ) AS pv1
        LEFT JOIN (
            SELECT pv2.month, COUNT(*) AS count2 FROM (
                SELECT DISTINCT ON (peer, date) peer, date, time, (
                    SELECT TO_CHAR(
                        TO_DATE ((
                            SELECT EXTRACT (MONTH FROM date) FROM time_tracking AS m1
                            WHERE m1.id = t1.id
                        )::text, 'MM'), 'Month'
                    ) AS month_Name
                ) AS month, (
                    SELECT EXTRACT (MONTH FROM date) FROM time_tracking AS m1
                    WHERE m1.id = t1.id
                ) AS monthDigit, id FROM time_tracking AS t1
                WHERE (
                    SELECT EXTRACT(MONTH FROM date) 
                    FROM time_tracking AS t2
                    WHERE t1.id = t2.id
                    ) = (
                        SELECT EXTRACT (MONTH FROM birthday) 
                        FROM peers AS t3
                        WHERE t3.nickname = t1.peer
                    ) AND t1.state = '1'
                ORDER BY peer, date, time
            ) AS pv2
            WHERE  (
                SELECT EXTRACT (HOUR FROM time) FROM time_tracking AS t4
                WHERE t4.id = pv2.id
            ) < 12
            GROUP BY pv2.monthDigit, pv2.month
        ) AS pv3 ON pv1.month = pv3.month
        GROUP BY pv1.monthDigit, pv1.month, pv3.month, pv3.count2
    ) AS evr;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_early_entries();