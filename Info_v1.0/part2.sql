CREATE OR REPLACE PROCEDURE p2p_add (
    IN peer_checks TEXT, 
    IN peer_checking TEXT, 
    IN task_name TEXT,
    IN state_name TEXT,
    IN time_check time
    ) AS $$
BEGIN
    IF state_name = 'Start' THEN
        INSERT INTO checks (id, peer, task, date)
        VALUES 
        (
            (SELECT MAX(id+1) FROM checks),
            peer_checks,
            task_name,
            CURRENT_DATE
        );
    END IF;

    INSERT INTO p2p (id, check_id, checking_peer, state, time)
    VALUES 
    (
        (SELECT MAX(id+1) FROM p2p),
        (
            SELECT
            CASE WHEN state_name = 'Success' OR state_name = 'Failure'  THEN
                COALESCE((SELECT check_id FROM (
SELECT check_id, count(check_id) AS c FROM p2p
group by check_id
having count(check_id) = 1
ORDER BY check_id
) AS pv
LEFT JOIN (
SELECT checks.id 
FROM p2p
JOIN checks ON checks.id = p2p.check_id
WHERE checks.peer = peer_checks AND
p2p.checking_peer = peer_checking AND
p2p.state = 'Start'
ORDER BY checks.id DESC) AS pv2 ON pv.check_id = pv2.id
ORDER BY check_id
LIMIT 1), -1)
            ELSE
                (SELECT MAX(id) FROM checks)
            END AS  max_id
            FROM checks
            LIMIT 1
        ),
        peer_checking,
        state_name,
        time_check
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE verter_add (
    IN peer_checks TEXT,
    IN task_name TEXT,
    IN state_name TEXT,
    IN time_check time
) AS $$
BEGIN
    INSERT INTO verter (id, check_id, state, time)
    VALUES
    (
        (SELECT MAX(id+1) FROM verter), 
        (SELECT CASE WHEN state_name = 'Start' THEN
        COALESCE((            
            SELECT pv.id FROM (
            SELECT checks.id FROM p2p
            LEFT JOIN checks ON checks.id = p2p.check_id
            where p2p.state = 'Success'
                AND
                checks.peer = peer_checks
                AND
                checks.task = task_name
            ORDER BY checks.date, p2p.time) AS pv
            LEFT JOIN (
            SELECT checks.id FROM verter
            LEFT JOIN checks ON checks.id = verter.check_id
            LEFT JOIN p2p ON checks.id = p2p.check_id
            where p2p.state = 'Success'
                AND
                checks.peer = peer_checks
                AND
                checks.task = task_name
            ORDER BY checks.date, p2p.time) AS pv2 ON pv.id = pv2.id
            WHERE pv2.id IS NULL
            LIMIT 1
        ), -1)
        ELSE
            COALESCE((
            SELECT pv.check_id FROM ( 
            SELECT check_id, count(check_id) AS c FROM verter
            group by check_id
            HAVING count(check_id) = 1
            ORDER BY check_id) AS pv
            LEFT JOIN (
            SELECT checks.id FROM p2p
            LEFT JOIN checks on checks.id = p2p.check_id
            where p2p.state = 'Success'
                AND
                checks.peer = peer_checks
                AND
                checks.task = task_name
            ORDER BY checks.date, p2p.time) AS pv2 ON pv.check_id = pv2.id
            LIMIT 1), -1)
            END AS  max_id
            FROM checks
            LIMIT 1
        ),
        state_name,
        time_check
    );
END;
$$ LANGUAGE plpgsql;


-- 3
CREATE OR REPLACE FUNCTION fnc_update_points_amount ()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
    DECLARE
        checked_peer_nickname TEXT;
        points_amount_history INTEGER;
BEGIN
    IF NEW.state = 'start' THEN
        SELECT c.peer INTO checked_peer_nickname
        FROM checks c
        JOIN p2p p ON c.id = p.check_id
        WHERE c.id = NEW.check_id;

        SELECT points_amount INTO points_amount_history
        FROM transferred_points
        WHERE checking_peer = NEW.checking_peer
        AND checked_peer = checked_peer_nickname;

        IF points_amount_history = 0 THEN
            INSERT INTO transferred_points VALUES
            (
                (SELECT MAX(id) + 1 FROM transferred_points),
                (NEW.checking_peer),
                (SELECT c.peer AS Checked_peer FROM p2p
                JOIN checks c on c.id = p2p.check_id
                WHERE c.id = NEW.check_id
                ),
                (SELECT points_amount + 1 FROM transferred_points
                WHERE checking_peer = NEW.checking_peer
                AND checked_peer = checked_peer_nickname
                )
            );
        ELSE
            UPDATE transferred_points tp
            SET points_amount = tp.points_amount + 1
            WHERE checked_peer = checked_peer_nickname AND
            NEW.checking_peer = checking_peer;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;


CREATE OR REPLACE TRIGGER trg_update_points_amount
    AFTER INSERT
    ON P2P
    FOR EACH ROW
EXECUTE FUNCTION fnc_update_points_amount();


-- 4
CREATE OR REPLACE FUNCTION fnc_check_maximum_exp()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS(SELECT * FROM checks WHERE ID = NEW.Check_id) THEN
        RAISE EXCEPTION 'No such check ID';
    END IF;

    IF NEW.xpamount > (SELECT t.max_xp
        FROM tasks t
        JOIN checks c on t.title = c.task
        WHERE c.id = NEW.check_id) THEN
        RAISE EXCEPTION 'Too many exp points for this task';
    END IF;

    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_check_maximum_exp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_check_maximum_exp();
