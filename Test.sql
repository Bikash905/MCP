group by -> where -> order by -> select -> join -> top5/limit -> having -> from

from  join  where group by having select order by top5/limit

tab1 tab2
1	 NULL
1	 1
0 	 1
1	 0
0  	 1
0	 0
NULL
 
Left join  -> 9+6+1=16
Right Join ->16
Inner join ->15
Full join  ->9+6+2=17

Find the date on 3rd saturday from today..


SELECT 
    DATEADD(
        WEEK, 
        2, 
        NEXT_DAY(CURRENT_DATE, 'SATURDAY')
    ) AS third_saturday;


Return Second most recent activity for each person (in case a person has done less than 2 activities then return the first activity):
username activity Activity_date
vinay    travel   2025-05-01
vinay    swim     2025-05-02
vinay    dance    2025-05-03
yash     travel   2025-05-03


SELECT username, activity, activity_date
FROM (
    SELECT 
        username,
        activity,
        activity_date,
        ROW_NUMBER() OVER (PARTITION BY username ORDER BY activity_date DESC) AS rn
    FROM activities
) sub
WHERE rn = 2 OR (
    rn = 1 AND username IN (
        SELECT username
        FROM (
            SELECT username, COUNT(*) AS cnt
            FROM activities
            GROUP BY username
        ) t
        WHERE cnt < 2
    )
);




select 

i/p:
date_value	state
2019-01-01	success
2019-01-02	success
2019-01-03	success
2019-01-04	fail
2019-01-05	fail
2019-01-06 	success
 
o/p:
start_date end_date	    state
2019-01-01 2019-01-03	success	
2019-01-04 2019-01-05	fail
2019-01-06 2019-01-06	success



WITH marked AS (
    SELECT *,
        ROW_NUMBER() OVER (ORDER BY date_value) 
        - ROW_NUMBER() OVER (PARTITION BY state ORDER BY date_value) AS grp
    FROM your_table
),
grouped AS (
    SELECT 
        MIN(date_value) AS start_date,
        MAX(date_value) AS end_date,
        state
    FROM marked
    GROUP BY state, grp
)
SELECT * FROM grouped
ORDER BY start_date;

-------
SELECT 
    first_leg.CID,
    first_leg.Origin AS Origin,
    last_leg.Destination AS Destination
FROM 
    Flights AS first_leg
JOIN 
    Flights AS last_leg
ON 
    first_leg.CID = last_leg.CID
WHERE 
    first_leg.Origin NOT IN (SELECT Destination FROM Flights WHERE Flights.CID = first_leg.CID)
    AND last_leg.Destination NOT IN (SELECT Origin FROM Flights WHERE Flights.CID = last_leg.CID);
	
	-----
	WITH FirstLeg AS (
SELECT CID, Origin
FROM Flight
WHERE Origin NOT IN (SELECT Destination FROM Flight)
),
LastLeg AS (
SELECT CID, Destination
FROM Flight
WHERE Destination NOT IN (SELECT Origin FROM Flight)
)
SELECT 
f.CID,
f.Origin,
l.Destination
FROM
FirstLeg f
JOIN 
LastLeg l ON f.CID = l.CID order by CID;
	
	
	
	-------
	
	
	SELECT 
    c.ID,
    c.Name,
    SUM(o.Amount) AS TotalSpent
FROM 
    Customers c
JOIN 
    Orders o ON c.ID = o.CustomerID
GROUP BY 
    c.ID, c.Name;
----

SELECT Name
FROM (
    SELECT GOLD AS Name FROM OlympicResults
    UNION ALL
    SELECT SILVER FROM OlympicResults
    UNION ALL
    SELECT BRONZE FROM OlympicResults
) AS AllMedals
GROUP BY Name
HAVING COUNT(*) = 1;
----
WITH RankedOrders AS (
    SELECT 
        order_no,
        cust_code,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY cust_code ORDER BY order_date) AS rn
    FROM Orders
),
FirstSecondOrders AS (
    SELECT 
        o1.cust_code,
        DATEDIFF(DAY, o1.order_date, o2.order_date) AS days_between
    FROM RankedOrders o1
    JOIN RankedOrders o2 
        ON o1.cust_code = o2.cust_code 
        AND o1.rn = 1 
        AND o2.rn = 2
)
SELECT * FROM FirstSecondOrders;

-----

WITH PositionCounts AS (
    SELECT *
    FROM Job_Positions jp
    LEFT JOIN (select position_id,count(*) FilledCount from job_employees group by 1) je using(Position_Id)
),
Expanded AS (
    SELECT 
        pc.Title,
        pc.Groups,
        pc.Levels,
        pc.Payscale,
        je.Name AS Employee_Name
    FROM PositionCounts pc
    LEFT JOIN job_employees je ON pc.Position_Id = je.Position_Id

    UNION ALL

    SELECT 
        pc.Title,
        pc.Groups,
        pc.Levels,
        pc.Payscale,
        'Vacant' AS Employee_Name
    FROM PositionCounts pc
    JOIN (
        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    ) AS nums on nums.n <= (pc.TotalPost - pc.FilledCount)
)
SELECT * FROM Expanded
ORDER BY GROUPS;

----
WITH PositionCounts AS (
    SELECT *
    FROM Job_Positions jp
    LEFT JOIN (select position_id,count(*) FilledCount from job_employees group by 1) je using(Position_Id)
),
Expanded AS (
    SELECT 
        pc.Title,
        pc.Groups,
        pc.Levels,
        pc.Payscale,
        je.Name AS Employee_Name
    FROM PositionCounts pc
    LEFT JOIN job_employees je ON pc.Position_Id = je.Position_Id

    UNION ALL

    SELECT 
        pc.Title,
        pc.Groups,
        pc.Levels,
        pc.Payscale,
        'Vacant' AS Employee_Name
    FROM PositionCounts pc
    inner JOIN (
        SELECT seq4() as n
  FROM TABLE(GENERATOR(ROWCOUNT => 100)) 
  ORDER BY 1
    ) AS nums on nums.n <= (pc.TotalPost - pc.FilledCount) and nums.n!=0
)
SELECT * FROM Expanded
ORDER BY GROUPS;
-----

SELECT seq4(), uniform(1, 10, RANDOM(12)) 
  FROM TABLE(GENERATOR(ROWCOUNT => 10)) v 
  ORDER BY 1;
