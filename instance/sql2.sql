-- SQLite

    SELECT d.id, d.department, count(e.id)
    FROM department d
    JOIN employee e ON d.id = e.department_id
    WHERE CAST(strftime('%Y', e.datetime) AS INTEGER) = 2021
    GROUP BY d.id, d.department
    HAVING COUNT(e.id) > (SELECT AVG(employee_count) FROM (
                    SELECT department_id, COUNT(id) AS employee_count
                    FROM employee
                    WHERE CAST(strftime('%Y', datetime) AS INTEGER) = 2021
                    GROUP BY department_id
                ) AS department_averages)
    ORDER BY 3 desc