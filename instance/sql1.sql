-- SQLite
    SELECT d.department, j.job,
    SUM(CASE WHEN CAST(strftime('%m', e.datetime) AS INTEGER) BETWEEN 1 AND 3 THEN 1 else 0 end) AS Q1,
    SUM(CASE WHEN CAST(strftime('%m', e.datetime) AS INTEGER) BETWEEN 4 AND 6 THEN 1 else 0 end) AS Q2,
    SUM(CASE WHEN CAST(strftime('%m', e.datetime) AS INTEGER) BETWEEN 7 AND 9 THEN 1 else 0 end) AS Q3,
    SUM(CASE WHEN CAST(strftime('%m', e.datetime) AS INTEGER) BETWEEN 10 AND 12 THEN 1 else 0 end) AS Q4
    FROM department d
    JOIN employee e ON d.id = e.department_id
    JOIN job j ON j.id = e.job_id
    WHERE CAST(strftime('%Y', e.datetime) AS INTEGER) = 2021
    GROUP BY d.id, j.id
    ORDER BY d.department, j.job